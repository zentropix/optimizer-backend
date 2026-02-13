import time
import requests
from datetime import datetime, timedelta, timezone
import logging
from logging.handlers import RotatingFileHandler
import os
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("BACKEND_BASE_URL")
UPDATE_COSTS_API_URL = BASE_URL + "/update-costs-for-mmd"
ADD_DATA_FOR_PREVIOUS_DAY_URL = BASE_URL + "/save-data-for-previous-date"

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

    return logger


update_costs_logger = setup_logger(
    "update_costs",
    "update_costs.log",
)


def get_next_run_time_utc(target_hour=5, target_minute=0):
    """
    Returns datetime of the next 05:00 UTC
    """
    now = datetime.now(timezone.utc)
    next_run = now.replace(
        hour=target_hour,
        minute=target_minute,
        second=0,
        microsecond=0,
    )

    if now >= next_run:
        next_run += timedelta(days=1)

    return next_run


def call_update_costs_api():
    start_time = datetime.now(timezone.utc)

    update_costs_logger.info(
        "REQUEST | time=%s | message=%s",
        start_time.isoformat(),
        "Calling update costs API",
    )

    try:
        response = requests.get(UPDATE_COSTS_API_URL, timeout=300)

        update_costs_logger.info(
            "RESPONSE | status=%s | body=%s",
            response.status_code,
            response.text,
        )

        response.raise_for_status()
        print("✅ Daily cost update API called successfully")

    except requests.RequestException as e:
        update_costs_logger.exception(
            "ERROR | message=%s",
            str(e),
        )
        print(f"❌ Cost update API failed: {e}")

    update_costs_logger.info(
        "REQUEST | time=%s | message=%s",
        start_time.isoformat(),
        "Calling Add data for previous day api",
    )

    try:
        response = requests.get(ADD_DATA_FOR_PREVIOUS_DAY_URL, timeout=300)

        update_costs_logger.info(
            "RESPONSE | status=%s | body=%s",
            response.status_code,
            response.text,
        )

        response.raise_for_status()
        print("✅ Daily data addition API called successfully")

    except requests.RequestException as e:
        update_costs_logger.exception(
            "ERROR | message=%s",
            str(e),
        )
        print(f"❌ add Data API failed: {e}")


def run_daily_at_5am_utc():
    print("🚀 Daily cost updater started (runs every day at 05:00 UTC)")

    while True:
        next_run = get_next_run_time_utc(5, 0)
        sleep_seconds = (next_run - datetime.now(timezone.utc)).total_seconds()

        update_costs_logger.info(
            "SCHEDULER | next_run_utc=%s | sleep_seconds=%s",
            next_run.isoformat(),
            int(sleep_seconds),
        )

        time.sleep(max(sleep_seconds, 0))
        call_update_costs_api()


def main():
    run_daily_at_5am_utc()


if __name__ == "__main__":
    main()