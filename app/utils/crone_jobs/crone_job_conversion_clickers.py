import time
import requests
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, wait
import concurrent.futures
import logging 
from logging.handlers import RotatingFileHandler
import json
import os
from dotenv import load_dotenv

load_dotenv()

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

MAX_WORKERS = 2

BASE_URL = os.getenv("BACKEND_BASE_URL")

REPORTS_API_URL = BASE_URL + "/sync-voluum-reports"
INTERVAL_SECONDS_REPORTS = 60 * 60 * 24 # 24 hours

CONVERSION_API_URL = BASE_URL + "/sync-voluum-conversions"
INTERVAL_SECONDS_CONVERSION = 60 * 5  # 5 minutes

def setup_logger(name: str, filename: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False  # prevent duplicate logs

    handler = RotatingFileHandler(
        filename=os.path.join(LOG_DIR, filename),
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5
    )

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z"
    )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

conversions_logger = setup_logger(
    "conversions",
    "conversions.log"
)

reports_logger = setup_logger(
    "reports",
    "reports.log"
)

def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def get_completed_day_window(now: datetime):
    """
    from_date = start of previous day (00:00 UTC)
    to_date   = start of current day (00:00 UTC)
    """
    now = now.astimezone(timezone.utc)

    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start = today_start - timedelta(days=1)

    return yesterday_start, today_start

def get_completed_hour_window(now: datetime):
    """
    Returns:
      from_date = start of current hour
      to_date   = start of next hour
    """
    now = now.astimezone(timezone.utc)

    current_hour = now.replace(minute=0, second=0, microsecond=0)
    next_hour = current_hour + timedelta(hours=1)

    return current_hour, next_hour

def call_conversions_api(from_date: datetime, to_date: datetime):
    payload = {
        "from_date": iso_utc(from_date),
        "to_date": iso_utc(to_date),
    }
    start_time = datetime.now(timezone.utc)

    conversions_logger.info(
        "REQUEST | time=%s | payload=%s",
        start_time.isoformat(),
        json.dumps(payload)
    )

    try:
        response = requests.post(CONVERSION_API_URL, json=payload, timeout=30)
        conversions_logger.info(
            "RESPONSE | status=%s | body=%s",
            response.status_code,
            response.text
        )

        response.raise_for_status()
        print(f"✅ Hourly Conversion API called with: {payload}, response: {response.content}")
    except requests.RequestException as e:
        print(f"❌ Conversions API call failed: {e}")
        conversions_logger.exception(
            "ERROR | payload=%s",
            json.dumps(payload)
        )


def call_reports_api(from_date: datetime, to_date: datetime):
    payload = {
        "from_date": iso_utc(from_date),
        "to_date": iso_utc(to_date),
    }

    start_time = datetime.now(timezone.utc)

    reports_logger.info(
        "REQUEST | time=%s | payload=%s",
        start_time.isoformat(),
        json.dumps(payload)
    )

    try:
        response = requests.post(REPORTS_API_URL, json=payload, timeout=30)
        reports_logger.info(
            "RESPONSE | status=%s | body=%s",
            response.status_code,
            response.text
        )

        response.raise_for_status()
        print(f"✅ 24-hour Reports API called with: {payload}, response: {response.content}")
    except requests.RequestException as e:
        reports_logger.exception(
            "ERROR | payload=%s",
            json.dumps(payload)
        )
        print(f"❌ Reports API call failed: {e}")

def sync_conversions_every_5_mins():
    print("🚀 Hourly API poller for conversion data started")
    while True:
        now = datetime.now(timezone.utc)
        from_date, to_date = get_completed_hour_window(now)

        call_conversions_api(from_date, to_date)
        time.sleep(max(INTERVAL_SECONDS_CONVERSION, 0))

def sync_reports_every_24_hours():
    print("🚀 24-hour API poller for reports data started")

    while True:
        now = datetime.now(timezone.utc)
        from_date, to_date = get_completed_day_window(now)

        call_reports_api(from_date, to_date)
        time.sleep(max(INTERVAL_SECONDS_REPORTS, 0))

def main():
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(sync_conversions_every_5_mins),
            executor.submit(sync_reports_every_24_hours)
        ]
        wait(futures, return_when=concurrent.futures.ALL_COMPLETED)

if __name__ == "__main__":
    main()
