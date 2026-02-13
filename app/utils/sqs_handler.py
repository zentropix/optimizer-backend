import json
import time
import boto3
import os
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from app.utils.db_handler import DBHandler
from app.utils.voluum_data_handler import VoluumDataHandler

load_dotenv()

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)


def setup_logger(name: str, filename: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:
        handler = RotatingFileHandler(
            filename=os.path.join(LOG_DIR, filename),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
        )

        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )

        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Also log to console so you can see output when running interactively
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


logger = setup_logger("sqs_handler", "sqs_handler.log")


class SQSHandler:
    def __init__(self):
        self.sqs = boto3.client(
            "sqs",
            region_name=os.getenv("AWS_REGION"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        self.queue_url = os.getenv("SQS_QUEUE_URL")
        self.db = DBHandler()
        self.voluum = VoluumDataHandler()

        # In-memory caches for name resolution
        self.offer_map = {}
        self.campaign_map = {}

    def _load_lookup_caches(self):
        """Load offer & campaign name caches from DB."""
        self.offer_map = self.db.get_all_offer_names()
        self.campaign_map = self.db.get_all_campaign_names()
        logger.info(f"Loaded {len(self.offer_map)} offers and {len(self.campaign_map)} campaigns into cache.")

    def _refresh_offers_and_campaigns(self):
        """Fetch latest offers & campaigns from Voluum API and upsert into DB."""
        logger.info("Refreshing offers and campaigns from Voluum API...")
        offers = self.voluum.get_offers_data()
        campaigns = self.voluum.get_campaigns_data()
        self.db.upsert_offers(offers)
        self.db.upsert_campaigns(campaigns)
        self._load_lookup_caches()

    def _resolve_missing_ids(self, events: list[dict]):
        """
        Check if any offer_id or campaign_id in the events batch is missing
        from our cache. If so, refresh from Voluum API.
        """
        missing_offer = False
        missing_campaign = False

        for e in events:
            offer_id = e.get("offerId")
            campaign_id = e.get("campaignId")
            if offer_id and offer_id not in self.offer_map:
                missing_offer = True
            if campaign_id and campaign_id not in self.campaign_map:
                missing_campaign = True
            if missing_offer and missing_campaign:
                break

        if missing_offer or missing_campaign:
            logger.info(f"Missing IDs detected (offers={missing_offer}, campaigns={missing_campaign}). Refreshing from Voluum API...")
            self._refresh_offers_and_campaigns()

    def _delete_messages(self, receipt_handles: list[str]):
        """Delete processed messages from the queue in batches of 10."""
        for i in range(0, len(receipt_handles), 10):
            batch = receipt_handles[i:i + 10]
            entries = [
                {"Id": str(idx), "ReceiptHandle": handle}
                for idx, handle in enumerate(batch)
            ]
            resp = self.sqs.delete_message_batch(
                QueueUrl=self.queue_url,
                Entries=entries,
            )
            failed = resp.get("Failed", [])
            if failed:
                logger.error(f"Failed to delete {len(failed)} messages: {failed}")

        logger.info(f"Deleted {len(receipt_handles)} messages from queue.")

    def _process_batch(self, messages: list[dict]):
        """
        Process a single batch of SQS messages:
        1. Parse events from messages
        2. Resolve any missing offer/campaign IDs
        3. Insert events into DB
        4. Delete the messages from the queue immediately
        """
        events = []
        receipt_handles = []

        for msg in messages:
            try:
                body = json.loads(msg["Body"])
                raw_events = json.loads(body["Message"])

                if isinstance(raw_events, list):
                    events.extend(raw_events)
                else:
                    events.append(raw_events)

                receipt_handles.append(msg["ReceiptHandle"])
            except Exception as e:
                logger.error(f"Error parsing message: {e}")
                # Still delete bad messages so they don't block the queue
                receipt_handles.append(msg["ReceiptHandle"])

        if events:
            # Insert raw data first (no dedup, stores everything as-is)
            self.db.insert_raw_live_events(events)

            # Check for unknown offer/campaign IDs and refresh if needed
            self._resolve_missing_ids(events)

            # Upsert into processed live events table (dedup on custom_variable_1)
            self.db.insert_live_events(events, self.offer_map, self.campaign_map)

        # # Delete immediately after processing
        # if receipt_handles:
        #     self._delete_messages(receipt_handles)

        return len(events)

    def run(self, sleep_seconds=300):
        """
        Continuous polling loop:
        1. Load offer/campaign caches from DB
        2. Poll queue — for each batch of messages: parse, insert into DB, delete immediately
        3. When queue is empty (3 consecutive empty receives), sleep for 5 minutes
        4. Repeat
        """
        logger.info("SQS Handler starting...")
        self._load_lookup_caches()

        while True:
            try:
                logger.info(f"{'='*60}")
                logger.info("Polling SQS queue...")

                total_events = 0
                empty_receives = 0
                max_empty = 3

                while True:
                    resp = self.sqs.receive_message(
                        QueueUrl=self.queue_url,
                        MaxNumberOfMessages=10,
                        WaitTimeSeconds=20,
                        VisibilityTimeout=60,
                    )

                    messages = resp.get("Messages", [])

                    if not messages:
                        empty_receives += 1
                        logger.info(f"Empty receive #{empty_receives}/{max_empty}")
                        if empty_receives >= max_empty:
                            break
                        continue

                    empty_receives = 0
                    batch_count = self._process_batch(messages)
                    total_events += batch_count
                    logger.info(f"Batch done: {batch_count} events. Total this cycle: {total_events}")

                logger.info(f"Queue drained. Processed {total_events} events total this cycle.")
                logger.info(f"Sleeping for {sleep_seconds} seconds...")
                time.sleep(sleep_seconds)

            except KeyboardInterrupt:
                logger.info("SQS Handler stopped by user.")
                break
            except Exception as e:
                logger.error(f"Error in polling loop: {e}", exc_info=True)
                logger.info(f"Sleeping for {sleep_seconds} seconds before retry...")
                time.sleep(sleep_seconds)

SQSHandler().run()
