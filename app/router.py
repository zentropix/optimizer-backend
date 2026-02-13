from fastapi import APIRouter, HTTPException, UploadFile, File, Query, BackgroundTasks
from pydantic import BaseModel
import uuid
from app.utils.voluum_data_handler import VoluumDataHandler
from app.utils.db_handler import DBHandler
from app.utils.ongage_data_handler import OngageDataHandler
from app.utils.mmd_data_handler import MMDDataHandler
from app.schema import RecordRequest, SyncDataInRangeRequest, SyncDate
from fastapi.responses import JSONResponse, FileResponse
from time import time
import csv
import io
import os
from dotenv import load_dotenv
import requests
import time as time_mod
from datetime import datetime, timedelta, UTC
from fastapi.responses import StreamingResponse
import json
import pandas as pd

load_dotenv()

UPLOAD_ROOT = os.getenv("UPLOAD_ROOT", "uploads")

RAW_DIR = os.path.join(UPLOAD_ROOT, "raw")
PROCESSED_DIR = os.path.join(UPLOAD_ROOT, "processed")
MANUAL_DIR = os.path.join(UPLOAD_ROOT, "manual_update")
MANUAL_RAW_DIR = os.path.join(MANUAL_DIR, "raw")
MANUAL_RESULT_DIR = os.path.join(MANUAL_DIR, "results")
HLR_DIR = os.path.join(UPLOAD_ROOT, "hlr")

DB_ROOT = os.getenv("DB_ROOT", "db_files")
DB_DIR = os.path.join(DB_ROOT, "exports")

RAW_ENCRYPTED_DIR = os.path.join(UPLOAD_ROOT, "raw_encrypted")
PROCESSED_ENCRYPTED_DIR = os.path.join(UPLOAD_ROOT, "processed_encrypted")

HLR_APIKEY = os.getenv("HLRLOOKUP_APIKEY")
HLR_SECRET = os.getenv("HLRLOOKUP_SECRET")

BROADCAST_ROOT = os.path.join(UPLOAD_ROOT, "broadcasts")

RAW_SMART_CLEANING_VOLUUM_DIR = os.path.join(UPLOAD_ROOT, "raw_smart_cleaning_voluum")
PROCESSED_SMART_CLEANING_VOLUUM_DIR = os.path.join(UPLOAD_ROOT, "processed_smart_cleaning_voluum")

PROCESSED_REG_SEARCH_DIR = os.path.join(UPLOAD_ROOT, "processed_reg_search")

router = APIRouter()
data_handler = VoluumDataHandler()
db_handler = DBHandler(50000)
ongage_data_handler = OngageDataHandler()
mmd_data_handler = MMDDataHandler()

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

os.makedirs(MANUAL_RAW_DIR, exist_ok=True)
os.makedirs(MANUAL_RESULT_DIR, exist_ok=True)

os.makedirs(HLR_DIR, exist_ok=True)
os.makedirs(DB_DIR, exist_ok=True)

os.makedirs(RAW_ENCRYPTED_DIR, exist_ok=True)
os.makedirs(PROCESSED_ENCRYPTED_DIR, exist_ok=True)

os.makedirs(BROADCAST_ROOT, exist_ok=True)

os.makedirs(RAW_SMART_CLEANING_VOLUUM_DIR, exist_ok=True)
os.makedirs(PROCESSED_SMART_CLEANING_VOLUUM_DIR, exist_ok=True)

os.makedirs(PROCESSED_REG_SEARCH_DIR, exist_ok=True)

@router.post("/sync-voluum-reports")
def sync_voluum_reports(request_data: SyncDate):
    # data = data_handler.get_cleaned_data()
    start_time = time()
    data = data_handler.get_cleaned_data(from_date=request_data.from_date, to_date=request_data.to_date)
    print(f"Data fetched and cleaned in {time() - start_time} seconds.")
    start_time = time()
    ts_source_row_count = db_handler.insert_raw_data_into_ts_source(data.to_numpy())
    print(f"Inserted {ts_source_row_count} rows into ts_source in {time() - start_time} seconds.")
    blacklist_records = data[data['category'] == 'BLACKLIST'].to_numpy()
    whitelist_records = data[data['category'] == 'WHITELIST'].to_numpy()
    monitor_records = data[data['category'] == 'MONITOR'].to_numpy()
    start_time = time()
    blacklist_row_count = db_handler.upsert_data_into_blacklist_and_monitor('blacklist', blacklist_records)
    print(f"Upserted {blacklist_row_count} rows into blacklist in {time() - start_time} seconds.")
    start_time = time()
    whitelist_row_count = db_handler.upsert_data_into_whitelist('whitelist', whitelist_records)
    print(f"Upserted {whitelist_row_count} rows into whitelist in {time() - start_time} seconds.")
    start_time = time()
    monitor_row_count = db_handler.upsert_data_into_blacklist_and_monitor('monitor', monitor_records)
    print(f"Upserted {monitor_row_count} rows into monitor in {time() - start_time} seconds.")
    return {
        "row_counts": {
            "blacklist_rows": blacklist_row_count,
            "monitor_rows": monitor_row_count,
            "whitelist_rows": whitelist_row_count
        }
    }

@router.post("/sync-voluum-reports_for_range")
def sync_voluum_reports_for_range(request_data: SyncDataInRangeRequest):
    date_format = "{year}-{month:02d}-{day:02d}T00:00:00.000Z"
    for day in range(request_data.start_day, request_data.end_day):
        from_date_str = date_format.format(day=day, month=request_data.month, year=request_data.year)
        to_date_str = date_format.format(day=day+1, month=request_data.month, year=request_data.year)
        print(f"Syncing data from {from_date_str} to {to_date_str}")
        start_time = time()
        data = data_handler.get_cleaned_data(from_date=from_date_str, to_date=to_date_str)
        print(f"Data fetched and cleaned in {time() - start_time} seconds.")
        start_time = time()
        db_handler.insert_raw_data_into_ts_source(data.to_numpy())
        print(f"Inserted rows into ts_source in {time() - start_time} seconds.")
        blacklist_records = data[data['category'] == 'BLACKLIST'].to_numpy()
        whitelist_records = data[data['category'] == 'WHITELIST'].to_numpy()
        monitor_records = data[data['category'] == 'MONITOR'].to_numpy()
        start_time = time()
        db_handler.upsert_data_into_blacklist_and_monitor('blacklist', blacklist_records)
        print(f"Upserted rows into blacklist in {time() - start_time} seconds.")
        start_time = time()
        db_handler.upsert_data_into_whitelist('whitelist', whitelist_records)
        print(f"Upserted rows into whitelist in {time() - start_time} seconds.")
        start_time = time()
        db_handler.upsert_data_into_blacklist_and_monitor('monitor', monitor_records)
        print(f"Upserted rows into monitor in {time() - start_time} seconds.")
    return {
        "status": "success"
    }

@router.post("/find_records")
def find_records(body: RecordRequest):
    results = db_handler.find_records_in_ts_source(body.phone_number)
    return {"results": results}

@router.post("/sync-voluum-conversions")
def sync_voluum_conversions(data: SyncDate):
    data = data_handler.get_conversions_data_as_dataframe(from_date=data.from_date, to_date=data.to_date)
    print(f"Data fetched and cleaned.")
    start_time = time()
    db_handler.insert_conversions(data.to_numpy())
    unsuccesful_email_count = 0
    succesful_email_count = 0
    unsuccesful_emails = ongage_data_handler.prepare_data(data.to_numpy())
    db_handler.update_unsuccessful_conversions(unsuccesful_emails)
    unsuccesful_email_count += len(unsuccesful_emails)
    email_parts, statuses = ongage_data_handler.sync_list()
    for emails, status in zip(email_parts, statuses):
        if status:
            db_handler.update_successful_conversions(emails)
            succesful_email_count += len(emails)
        else:
            db_handler.update_unsuccessful_conversions(emails, "ONGAGE_INSERTION_FAILED")
            unsuccesful_email_count += len(emails)
    print(f"Inserted rows into conversions in {time() - start_time} seconds.")
    return {
        "row_counts": {
            "successful_rows": succesful_email_count,
            "unsuccessful_rows": unsuccesful_email_count
        }
    }

@router.post("/sync-voluum-conversions-for-range")
def sync_voluum_conversions(request_data: SyncDataInRangeRequest):
    date_format = "{year}-{month:02d}-{day:02d}T00:00:00.000Z"
    for day in range(request_data.start_day, request_data.end_day):
        from_date_str = date_format.format(day=day, month=request_data.month, year=request_data.year)
        to_date_str = date_format.format(day=day+1, month=request_data.month, year=request_data.year)
        data = data_handler.get_conversions_data_as_dataframe(from_date=from_date_str, to_date=to_date_str)
        print(f"Data fetched and cleaned for date {from_date_str} to {to_date_str}.")
        start_time = time()
        # data = data[:20]
        db_handler.insert_conversions(data.to_numpy())
        unsuccesful_emails = ongage_data_handler.prepare_data(data.to_numpy())
        db_handler.update_unsuccessful_conversions(unsuccesful_emails)
        email_parts, statuses = ongage_data_handler.sync_list()
        for emails, status in zip(email_parts, statuses):
            if status:
                db_handler.update_successful_conversions(emails)
            else:
                db_handler.update_unsuccessful_conversions(emails, "ONGAGE_INSERTION_FAILED")
        print(f"Inserted rows into conversions in {time() - start_time} seconds.")
        print("-"*30)

@router.get("/get-metrics")
def get_metrics():
    metrics_dict = {}
    tables = ["blacklist", "whitelist", "monitor", "main_database"]
    for table in tables:
        metrics_dict[table] = db_handler.get_counts(table, "timestamp_created")
    
    metrics_dict["api_voluum_conversions"] = db_handler.get_counts("api_voluum_conversions", "processed_at")
    metrics_dict["api_hlr_live_numbers"] = db_handler.get_counts("api_hlr_live_numbers", "timestamp")
    return metrics_dict

@router.post("/upload")
async def process_csv(file: UploadFile = File(...)):
    # Read CSV text

    try:
        content = (await file.read())
        file_id = str(uuid.uuid4())
        raw_path = os.path.join(RAW_DIR, f"{file_id}.csv")

        # Save file to disk
        with open(raw_path, "wb") as f:
            f.write(content)
        
        text = content.decode("utf-8", errors="replace")
        row_count = sum(1 for line in text.splitlines() if line.strip())

        return db_handler.create_file_entry(file_id, file.filename, raw_path, row_count)
    except Exception as e:  
        raise HTTPException(status_code=500, detail=str(e))

def _process_file_job(file_id: str, include_main_database, include_conversion, include_blacklist, include_monitor):
    """
    Runs in background. Reads raw CSV, cleans, writes processed CSV,
    computes stats, updates DB.
    """
    try:
        meta = db_handler.get_file_paths(file_id)
        if not meta:
            return

        raw_path = meta["raw_file_path"]
        processed_path = os.path.join(PROCESSED_DIR, f"{file_id}.csv")

        # Read raw CSV
        with open(raw_path, "r", encoding="utf-8", newline="") as f:
            content = f.read()

        # Your earlier parser used fieldnames=["record"].
        # If your CSV is single-column lines, keep it:
        content = content.replace("\ufeff", "")
        reader = csv.DictReader(io.StringIO(content), fieldnames=["record"])
        rows = list(reader)
        total = len(rows)
        processed_rows, removed_blacklist, removed_monitor, removed_conversion, removed_maindatabase = db_handler.clean_csv_records(rows, include_main_database, include_conversion, include_blacklist, include_monitor)

        # Compute basic stats (adjust to your real logic)
        # If clean_csv_records removes records, we can infer removed = total - clean
        clean_count = len(processed_rows)

        # Write processed rows to CSV (1 column)
        with open(processed_path, "w", encoding="utf-8", newline="") as out:
            w = csv.writer(out)
            for pr in processed_rows:
                # If pr is dict-like, adapt accordingly.
                # If it's already the value for "record", write single column:
                w.writerow([pr])

        db_handler.upsert_stats(
            file_id=file_id,
            total=total,
            rb=removed_blacklist,
            rm=removed_monitor,
            rc=removed_conversion,
            rmd=removed_maindatabase,
            final_clean=clean_count,
        )
        db_handler.mark_processed(
            file_id=file_id,
            processed_path=processed_path,
            total=total,
            clean=clean_count,
        )

    except Exception as e:
        print(e)
        db_handler.mark_failed(file_id, str(e))

@router.get("/files")
def list_files(status: str | None = Query(default=None)):
    db = DBHandler()
    try:
        if status:
            statuses = status.split(",")
            rows = db.list_files(statuses)
        else:
            rows = db.list_files()
        return {"items": rows}
    except Exception as e:
        print(e)

@router.post("/{file_id}/process")
def start_processing(file_id: str, background_tasks: BackgroundTasks, include_conversion: bool = Query(False), include_main_database: bool = Query(False), include_blacklist: bool = Query(False), include_monitor: bool = Query(False)):
    try:
        # Try to mark processing (fails if another processing exists due to DB unique index)
        try:
            ok = db_handler.mark_processing(file_id=file_id, lock_owner="api")
        except Exception:
            # Unique index conflict or other psycopg2 error
            raise HTTPException(status_code=409, detail="Another file is already processing")

        if not ok:
            raise HTTPException(status_code=400, detail="File not found or not eligible for processing")

        background_tasks.add_task(_process_file_job, file_id, include_main_database, include_conversion, include_blacklist, include_monitor)
        return {"ok": True, "file_id": file_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{file_id}/stats")
def get_stats(file_id: str):
    try:
        row = db_handler.get_stats(file_id)
        if not row:
            raise HTTPException(status_code=404, detail="Stats not found")
        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{file_id}/download")
def download_processed(file_id: str):
    try:
        meta = db_handler.get_file_paths(file_id)
        if not meta:
            raise HTTPException(status_code=404, detail="File not found")

        if meta["status"] != "processed" or not meta["processed_file_path"]:
            raise HTTPException(status_code=400, detail="File not processed yet")

        path = meta["processed_file_path"]
        if not os.path.exists(path):
            raise HTTPException(status_code=404, detail="Processed file missing on disk")

        download_name = f"processed_{meta['original_filename']}"
        return FileResponse(
            path,
            media_type="text/csv",
            filename=download_name
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/{file_id}/archive")
def archive_file(file_id: str):
    try:
        db_handler.archive_file(file_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/{file_id}/delete")
def delete_file(file_id: str):
    try:
        db_handler.delete_file_entry(file_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def _normalize_header(s: str) -> str:
    return (s or "").strip().lower().replace(" ", "").replace("_", "")

@router.post("/db-manual-update/import")
async def manual_update_import(
    target: str = Query(..., description="blacklist|monitor"),
    file: UploadFile = File(...)
):
    if target not in ("blacklist", "monitor"):
        raise HTTPException(status_code=400, detail="target must be blacklist or monitor")

    try:
        content = await file.read()
        import_id = str(uuid.uuid4())

        raw_path = os.path.join(MANUAL_RAW_DIR, f"{import_id}.csv")
        with open(raw_path, "wb") as f:
            f.write(content)

        text = content.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text))

        # normalize headers
        rows = []
        uploaded_count = 0

        for r in reader:
            print(r)
            uploaded_count += 1
            nr = {}
            for k, v in (r or {}).items():
                print(k, v)
                nr[_normalize_header(k)] = (v or "").strip()
            rows.append(nr)

        # required: phone, reason, source
        valid = []
        rejected = []
        for r in rows:
            phone = r.get("phone", "").strip()
            reason = r.get("reason", "").strip()
            source = r.get("source", "").strip()

            if not phone or not reason or not source:
                rejected.append((phone or "", "rejected_missing_required"))
                continue

            valid.append({
                "phone": phone,
                "reason": reason,
                "source": source,
                "os_version": r.get("osversion", "").strip() or None,
                "user_agent": r.get("useragent", "").strip() or None,
                "ip": r.get("ip", "").strip() or None,
                "isp": r.get("isp", "").strip() or None,
            })
        # insert only if not already in blacklist/monitor (duplicate rejection)
        inserted_set, duplicates_set = db_handler.manual_update_insert_dedup(target, valid)

        accepted_count = len(inserted_set)
        rejected_dup = [(p, "rejected_duplicate") for p in duplicates_set]
        rejected_all = rejected + rejected_dup
        rejected_count = len(rejected_all)

        # write result CSV
        result_path = os.path.join(MANUAL_RESULT_DIR, f"{import_id}.csv")
        with open(result_path, "w", encoding="utf-8", newline="") as out:
            w = csv.writer(out)
            w.writerow(["phone", "result"])
            # accepted
            for p in inserted_set:
                w.writerow([p, f"added_to_{target}"])
            # rejected
            for p, reason in rejected_all:
                w.writerow([p, reason])

        # store import history in DB (new table)
        db_handler.create_manual_update_import(
            import_id=import_id,
            original_filename=file.filename,
            target=target,
            raw_file_path=raw_path,
            result_file_path=result_path,
            uploaded_count=uploaded_count,
            accepted_count=accepted_count,
            rejected_count=rejected_count,
        )

        return JSONResponse({
            "id": import_id,
            "target": target,
            "original_filename": file.filename,
            "uploaded": uploaded_count,
            "accepted": accepted_count,
            "rejected": rejected_count,
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/db-manual-update/history")
def manual_update_history():
    try:
        items = db_handler.list_manual_update_imports(limit=50)
        return {"items": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/db-manual-update/{import_id}/download")
def manual_update_download(import_id: str):
    meta = db_handler.get_manual_update_import(import_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Import not found")

    path = meta["result_file_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Result file missing on disk")

    download_name = f"result_{meta['original_filename']}"
    return FileResponse(path, media_type="text/csv", filename=download_name)

def _hlr_job(file_id: str):
    """
    Runs in background.
    Uploads processed CSV to HLRLookup Batch API, starts processing, polls until complete,
    downloads results to disk, updates DB.
    """
    try:
        if not HLR_APIKEY or not HLR_SECRET:
            raise Exception("Missing HLRLOOKUP_APIKEY/HLRLOOKUP_SECRET env vars")

        meta = db_handler.get_file_paths(file_id)
        if not meta:
            raise Exception("File not found")

        processed_path = meta.get("processed_file_path")
        if meta.get("status") != "processed" or not processed_path or not os.path.exists(processed_path):
            raise Exception("File not processed or processed file missing")

        # 1) Create batch
        create_url = f"https://batches.hlrlookup.com/batches/?apikey={HLR_APIKEY}&secret={HLR_SECRET}"
        payload = {
            "filename": f"{file_id}.csv",
            "type": "HLR_V2",
            "batchArguments": {
                "cache_days_private": 30,
                "cache_days_global": 30,
                "save_to_cache": "YES",
            }
        }
        r = requests.post(create_url, json=payload, timeout=60)
        r.raise_for_status()
        batch = r.json()
        batch_id = str(batch.get("id"))
        if not batch_id:
            raise Exception(f"HLR batch create failed: {batch}")

        db_handler.set_hlr_batch_id(file_id, batch_id)

        # 2) Upload source file
        db_handler.set_hlr_status(file_id, "uploading")
        upload_url = f"https://batches.hlrlookup.com/batches/{batch_id}/source?apikey={HLR_APIKEY}&secret={HLR_SECRET}"

        with open(processed_path, "rb") as f:
            ur = requests.post(
                upload_url,
                data=f,
                headers={"Content-Type": "text/csv"},
                timeout=300,
            )
        ur.raise_for_status()

        # 3) Start processing
        start_url = f"https://batches.hlrlookup.com/batches/{batch_id}/?apikey={HLR_APIKEY}&secret={HLR_SECRET}"
        sr = requests.put(start_url, json={"status": "PROCESSING"}, timeout=60)
        sr.raise_for_status()

        db_handler.set_hlr_status(file_id, "processing")

        # 4) Poll status until COMPLETE (or FAILED)
        status_url = f"https://batches.hlrlookup.com/batches/{batch_id}?apikey={HLR_APIKEY}&secret={HLR_SECRET}"

        max_wait_seconds = int(os.getenv("HLR_POLL_MAX_SECONDS", "18000"))  # 5 hour default
        poll_every = int(os.getenv("HLR_POLL_EVERY_SECONDS", "5"))
        elapsed = 0

        while True:
            gr = requests.get(status_url, timeout=60)
            gr.raise_for_status()
            info = gr.json()

            api_status = (info.get("status") or "").upper()
            num_items = info.get("numItems")
            num_complete = info.get("numComplete")

            # keep progress in DB
            mapped = "processing"
            if api_status in ("COMPLETE", "COMPLETED"):
                mapped = "complete"
            elif api_status in ("FAILED", "ERROR"):
                mapped = "failed"

            db_handler.update_hlr_progress(file_id, mapped if mapped != "complete" else "processing", num_items, num_complete)

            if mapped == "complete":
                break
            if mapped == "failed":
                raise Exception(f"HLR batch failed: {info}")

            time_mod.sleep(poll_every)
            elapsed += poll_every
            if elapsed >= max_wait_seconds:
                raise Exception("HLR polling timed out")

        # 5) Download results
        results_url = f"https://batches.hlrlookup.com/batches/{batch_id}/results?apikey={HLR_APIKEY}&secret={HLR_SECRET}"
        rr = requests.get(results_url, timeout=300)
        rr.raise_for_status()

        hlr_raw_path = os.path.join(HLR_DIR, f"{file_id}_raw.csv")
        with open(hlr_raw_path, "wb") as f:
            f.write(rr.content)

        db_handler.set_hlr_raw_path(file_id, hlr_raw_path)

        # -----------------------------
        # PARSE CSV
        # -----------------------------

        live_numbers: list[str] = []
        dead_numbers: list[str] = []
        monitor_numbers: list[str] = []
        risky_networks = json.load(open('app/utils/risky_operators.json', 'r'))["risky_networks"]
        risky_numbers = []
        hlr_live_data = []
        with open(hlr_raw_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                status = (row.get("live_status") or "").upper()

                number = (
                    row.get("formatted_telephone_number")
                    or row.get("detected_telephone_number")
                    or ""
                ).strip()

                if not number:
                    continue

                if status == "LIVE":
                    current_network_name = (row.get("current_network_details.name") or "").lower().replace(" ", "")
                    if current_network_name in risky_networks:
                        risky_numbers.append(number)
                        continue
                    live_numbers.append(number)
                    hlr_live_data.append(row)
                elif status in ["NOT_AVAILABLE_NETWORK_ONLY", "NOT_APPLICABLE", "NO_TELESERVICE_PROVISIONED", "DEAD"]:
                    dead_numbers.append(number)
                elif status in ["NO_COVERAGE", "INCONCLUSIVE", "ABSENT_SUBSCRIBER"]:
                    monitor_numbers.append(number)
        db_handler.insert_raw_hlr_data(hlr_live_data)
        # -----------------------------
        # WRITE FINAL CLEAN FILE
        # -----------------------------

        final_hlr_path = os.path.join(HLR_DIR, f"{file_id}_hlr_clean.csv")

        with open(final_hlr_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["phone_number"])
            for n in live_numbers:
                writer.writerow([n])

        # -----------------------------
        # INSERT NON-LIVE INTO BLACKLIST
        # -----------------------------

        if dead_numbers:
            db_handler.insert_hlr_data("blacklist", dead_numbers, "CSV_CLEAN_UPLOAD", "HLR_FAILED")
        if monitor_numbers:
            db_handler.insert_hlr_data("monitor", monitor_numbers, "CSV_CLEAN_UPLOAD", "HLR_MONITORED")
        if risky_numbers:
            db_handler.insert_hlr_data("blacklist", risky_numbers, "CSV_CLEAN_UPLOAD", "HLR_RISKY_NETWORK")

        # -----------------------------
        # MARK COMPLETE
        # -----------------------------

        db_handler.mark_hlr_complete(
            file_id=file_id,
            result_path=final_hlr_path,
        )

    except Exception as e:
        db_handler.mark_hlr_failed(file_id, str(e))

@router.post("/{file_id}/hlr")
def start_hlr(file_id: str, background_tasks: BackgroundTasks):
    try:
        try:
            ok = db_handler.mark_hlr_processing(file_id=file_id, lock_owner="hlr")
        except Exception:
            raise HTTPException(status_code=409, detail="Another file is already running HLR")

        if not ok:
            raise HTTPException(status_code=400, detail="File not eligible for HLR (must be processed and not already active)")

        background_tasks.add_task(_hlr_job, file_id)
        return {"ok": True, "file_id": file_id, "hlr_status": "processing"}
    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{file_id}/hlr")
def get_hlr(file_id: str):
    try:
        row = db_handler.get_hlr_info(file_id)
        if not row:
            raise HTTPException(status_code=404, detail="File not found")
        return row
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{file_id}/hlr/download")
def download_hlr(file_id: str):
    try:
        meta = db_handler.get_file_paths(file_id)
        if not meta:
            raise HTTPException(status_code=404, detail="File not found")

        if meta.get("hlr_status") != "complete" or not meta.get("hlr_result_file_path"):
            raise HTTPException(status_code=400, detail="HLR not complete yet")

        path = meta["hlr_result_file_path"]
        if not os.path.exists(path):
            raise HTTPException(status_code=404, detail="HLR result file missing on disk")

        download_name = f"hlr_{meta['original_filename']}"
        return FileResponse(path, media_type="text/csv", filename=download_name)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/{file_id}/hlr/raw")
def download_hlr_raw(file_id: str):
    try:
        meta = db_handler.get_file_paths(file_id)
        if not meta:
            raise HTTPException(status_code=404, detail="File not found")

        path = meta.get("hlr_raw_file_path")
        if not path:
            raise HTTPException(status_code=400, detail="Raw HLR file not available")

        if not os.path.exists(path):
            raise HTTPException(status_code=404, detail="Raw HLR file missing")

        filename = f"hlr_raw_{meta['original_filename']}"
        return FileResponse(path, media_type="text/csv", filename=filename)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/process_db")
async def process_csv(background_tasks: BackgroundTasks, table_name: str | None = Query(default=None)):
    # Read CSV text

    try:
        file_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        csv_path = os.path.join(DB_DIR, f"{table_name}_{timestamp}.csv")
        
        db_handler.create_db_entry(table_name, file_id, csv_path)
        print(f"csv_path: {csv_path}")
        background_tasks.add_task(_process_db_export_job, file_id, table_name)
        return {"file_id": file_id, "table_name": table_name, "status": "processing"}
    except Exception as e:  
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/fetch-latest-db-export")
async def process_fetch_latest_db_export(table_name: str | None = Query(default=None)):
    try:
        return db_handler.fetch_latest_db_export(table_name)
    except Exception as e:  
        raise HTTPException(status_code=500, detail=str(e))

def _process_db_export_job(file_id: str, table_name: str):
    """
    Runs in background. Exports the specified table to CSV,
    """
    try:
        meta = db_handler.get_db_exports_path(file_id)
        if not meta:
            return

        file_path = meta["file_path"]
        table_name = meta["table_name"]
        db_export_status = db_handler.export_db_to_csv(table_name, file_path)

        if db_export_status:
            db_handler.upsert_db_export_status(
                file_id=file_id,
                status="completed",
            )

    except Exception as e:
        print(e)
        db_handler.upsert_db_export_status(
            file_id = file_id, 
            status="failed",
        )

@router.get("/db-download")
def download_processed(table_name: str | None = Query(default=None)):
    try:
        meta = db_handler.fetch_latest_db_export(table_name)
        if not meta:
            raise HTTPException(status_code=404, detail="File not found")
        
        if meta["status"] != "completed" or not meta["file_path"]:
            raise HTTPException(status_code=400, detail="File not processed yet")

        path = meta["file_path"]
        if not os.path.exists(path):
            raise HTTPException(status_code=404, detail="Processed file missing on disk")

        download_name = f"{meta['file_path']}"
        return FileResponse(
            path,
            media_type="text/csv",
            filename=download_name
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/get-live-broadcasts-stream")
def get_live_broadcasts_stream(search: str | None = Query(default=None)):
    date = datetime.utcnow().strftime("%Y-%m-%d")
    handler = MMDDataHandler()

    def event_generator():
        # --- step 1: get broadcasts ---
        broadcasts = handler.get_broadcasts(date)
        handler.get_links()

        broadcast_df = pd.DataFrame(broadcasts)
        broadcast_df["link_id"] = broadcast_df["message_body"].apply(
            lambda x: x.split("{{link:")[1].split("}}")[0]
        )
        broadcast_df["url"] = broadcast_df["link_id"].apply(
            lambda x: handler.links_dict.get(int(x), None)
        )
        broadcast_df["campaign_id"] = broadcast_df["url"].apply(
            lambda x: x.split("m/")[1] if x else None
        )
        broadcast_df["real_price"] = broadcast_df["real_price"].apply(
            lambda price: price / 100
        )

        # --- step 2: filter & limit (NO API CALLS YET) ---
        grouped = {}
        for _, row in broadcast_df.iterrows():
            if row["state"] in [0, 2]:
                continue
            if search and search.lower() not in row["name"].lower():
                continue

            name = row["name"].split("_chunk")[0]
            grouped.setdefault(name, [])

            # if len(grouped[name]) >= 10:
            #     continue

            grouped[name].append(row)
        
        grouped_rows = {}
        # --- step 3: sequential processing + streaming ---
        for broadcast_name, rows in grouped.items():
            results = []

            for row in rows:
                try:
                    grouped_rows, data = handler.process_single_row(row, grouped_rows)
                    if data:
                        results.append(data)

                        # 🔥 SEND PARTIAL UPDATE
                        yield f"data: {json.dumps({'type': 'row','broadcast': broadcast_name,'row': data})}\n\n"

                except Exception as e:
                    yield f"data: {json.dumps({'type': 'error','broadcast': broadcast_name,'error': str(e)})}\n\n"

                # t.sleep(0.25)  # <-- natural rate limiting

            # 🔥 SEND BROADCAST COMPLETE EVENT
            yield f"data: {json.dumps({'type': 'broadcast_done','broadcast': broadcast_name})}\n\n"

        yield "event: done\ndata: completed\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream"
    )

@router.get("/save-data-for-previous-date")
def save_data_for_previous_date():
    file_id = str(uuid.uuid4())
    previous_date = (datetime.now(UTC) - timedelta(days=1)).strftime("%Y-%m-%d")
    total_rows_df, broadcast_count = mmd_data_handler.save_data_for_day(previous_date)
    filename = f"MMD_Data_{previous_date}.csv"
    raw_file_path = os.path.join(BROADCAST_ROOT, filename)
    total_rows_df.to_csv(raw_file_path)
    db_handler.create_broadcasts_file_entry(file_id, filename, raw_file_path, f"{previous_date}/T00:00:00.000Z", broadcast_count)
    db_handler.upsert_broadcast_campaign_stats(total_rows_df)
    return {"broadcast_count": broadcast_count}

@router.get("/save-data-for-specific-date")
def save_data_for_previous_date(previous_date: str):
    file_id = str(uuid.uuid4())
    total_rows_df, broadcast_count = mmd_data_handler.save_data_for_day(previous_date)
    filename = f"MMD_Data_{previous_date}.csv"
    raw_file_path = os.path.join(BROADCAST_ROOT, filename)
    total_rows_df.to_csv(raw_file_path)
    db_handler.create_broadcasts_file_entry(file_id, filename, raw_file_path, f"{previous_date}/T00:00:00.000Z", broadcast_count)
    db_handler.upsert_broadcast_campaign_stats(total_rows_df)
    return {"broadcast_count": broadcast_count}

@router.get("/update-costs-for-mmd")
def update_costs_for_mmd():
    return mmd_data_handler.update_prices()

# ------------ Encryption Routes --------------
@router.post("/upload-encrypted")
async def process_csv(file: UploadFile = File(...)):
    # Read CSV text

    try:
        content = (await file.read())
        file_id = str(uuid.uuid4())
        raw_path = os.path.join(RAW_ENCRYPTED_DIR, f"{file_id}.csv")

        # Save file to disk
        with open(raw_path, "wb") as f:
            f.write(content)
        
        return db_handler.create_encrypted_file_entry(file_id, file.filename, raw_path)
    except Exception as e:  
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/encrypted-files")
def list_encrypted_files(status: str | None = Query(default=None)):
    try:
        if status:
            statuses = status.split(",")
            rows = db_handler.list_encrypted_files(statuses)
        else:
            rows = db_handler.list_encrypted_files()
        return {"items": rows}
    except Exception as e:
        print(e)

@router.post("/{file_id}/process-encrypted")
def start_processing(file_id: str, background_tasks: BackgroundTasks):
    try:
        # Try to mark processing (fails if another processing exists due to DB unique index)
        try:
            ok = db_handler.mark_encrypted_processing(file_id=file_id)
        except Exception as e:
            # Unique index conflict or other psycopg2 error
            raise HTTPException(status_code=409, detail="Another file is already processing")
        if not ok:
            raise HTTPException(status_code=400, detail="File not found or not eligible for processing")

        background_tasks.add_task(_process_encrypted_file_job, file_id)
        return {"ok": True, "file_id": file_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{file_id}/download-encrypted")
def download_processed(file_id: str):
    try:
        meta = db_handler.get_encrypted_file_paths(file_id)
        if not meta:
            raise HTTPException(status_code=404, detail="File not found")

        if meta["status"] != "processed" or not meta["encrypted_file_path"]:
            raise HTTPException(status_code=400, detail="File not processed yet")

        path = meta["encrypted_file_path"]
        if not os.path.exists(path):
            raise HTTPException(status_code=404, detail="Processed file missing on disk")

        download_name = f"encrypted_{meta['original_filename']}"
        print(download_name)
        return FileResponse(
            path,
            media_type="text/csv",
            filename=download_name
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/{file_id}/archive-encrypted")
def archive_file(file_id: str):
    try:
        db_handler.archive_encrypted_file(file_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/{file_id}/delete-encrypted")
def delete_file(file_id: str):
    try:
        db_handler.delete_encrypted_file_entry(file_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def _process_encrypted_file_job(file_id: str):
    """
    Runs in background. Reads raw CSV, cleans, writes processed CSV,
    computes stats, updates DB.
    """
    try:
        meta = db_handler.get_encrypted_file_paths(file_id)
        if not meta:
            return

        raw_path = meta["raw_file_path"]
        processed_path = os.path.join(PROCESSED_ENCRYPTED_DIR, f"{file_id}.csv")

        # Read raw CSV
        with open(raw_path, "r", encoding="utf-8", newline="") as f:
            content = f.read()
        # Your earlier parser used fieldnames=["record"].
        # If your CSV is single-column lines, keep it:
        reader = csv.DictReader(io.StringIO(content), fieldnames=["record"])
        rows = list(reader)
        print("Starting")
        values = [row['record'] for row in rows]
        print(values)
        encrypted_rows = db_handler.encrypt_and_save_csv(values)
        print("Ending", encrypted_rows)

        # Write processed rows to CSV (1 column)
        with open(processed_path, "w", encoding="utf-8", newline="") as out:
            w = csv.writer(out)
            for pr, en_r in zip(values, encrypted_rows):
                # If pr is dict-like, adapt accordingly.
                # If it's already the value for "record", write single column:
                w.writerow([pr, en_r])

        db_handler.mark_encrypted_processed(
            file_id=file_id,
            processed_path=processed_path,
        )

    except Exception as e:
        db_handler.mark_encrypted_failed(file_id, str(e))

# --------------------------------------------
@router.get("/files-broadcasts")
def list_files():
        rows = db_handler.list_broadcasts()
        return {"items": rows}

@router.get("/{file_id}/download-broadcasts-history")
def download_processed(file_id: str):
    try:
        meta = db_handler.get_broadcast_history_file_path(file_id)
        if not meta:
            raise HTTPException(status_code=404, detail="File not found")

        path = meta["raw_file_path"]
        if not os.path.exists(path):
            raise HTTPException(status_code=404, detail="Processed file missing on disk")

        download_name = f"{meta['filename']}"
        return FileResponse(
            path,
            media_type="text/csv",
            filename=download_name
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/csv-jobs/upload")
async def upload_csv_job(file: UploadFile = File(...)):
    """
    Upload CSV, create job, insert active records.
    No processing is started here.
    """
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    try:
        content = await file.read()
        text = content.decode("utf-8", errors="ignore")
        text = text.replace("\ufeff", "")
        reader = csv.DictReader(io.StringIO(text))
        required_headers = {
            "List_ID",
            "Email",
            "Country",
            "Source",
            "Mobile",
            "conversion_type",
            "Original_source",
            "HLR_Status",
            "Uuid",
        }

        csv_headers = set(reader.fieldnames or [])
        missing = required_headers - csv_headers
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"Missing required CSV columns: {', '.join(missing)}"
            )

        # -----------------------------
        # 1) Create csv_jobs row
        # -----------------------------
        job_id = str(uuid.uuid4())

        db_handler.create_csv_job(
            job_id=job_id,
            original_filename=file.filename,
            interval_seconds=60,     # default, user will edit later
            lower_limit=1,           # default
            upper_limit=1            # default
        )
        # -----------------------------
        # 2) Prepare active records
        # -----------------------------
        rows_to_insert = []
        for row in reader:
            rows_to_insert.append((
                job_id,
                row.get("List_ID"),
                row.get("Email"),
                row.get("Country"),
                row.get("Source"),
                row.get("Mobile"),
                row.get("conversion_type"),
                row.get("Original_source"),
                row.get("HLR_Status"),
                row.get("Uuid"),
            ))

        if not rows_to_insert:
            raise HTTPException(status_code=400, detail="CSV file is empty")

        # -----------------------------
        # 3) Bulk insert active records
        # -----------------------------
        inserted_count = db_handler.insert_csv_active_records(rows_to_insert)

        # -----------------------------
        # 4) Update job total_records
        # -----------------------------
        db_handler.update_csv_job_total_records(
            job_id=job_id,
            total_records=inserted_count
        )

        return {
            "ok": True,
            "job_id": job_id,
            "original_filename": file.filename,
            "total_records": inserted_count,
            "status": "draft"
        }

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/csv-jobs")
def list_csv_jobs():
    rows = db_handler.list_all_csv_ongage_files()
    if rows:
        return rows
    else: raise HTTPException(status_code=500, detail="An error occured while fetching all files")

@router.patch("/csv-jobs/{job_id}")
def update_csv_job(job_id: str,interval_seconds: int = Query(..., ge=10),lower_limit: int = Query(..., ge=1),upper_limit: int = Query(..., ge=1),):
    if upper_limit < lower_limit:
        raise HTTPException(status_code=400, detail="upper_limit must be >= lower_limit")
    
    if interval_seconds < 60:
        raise HTTPException(status_code=400, detail="Interval Time must be greater than 60 seconds")

    result = db_handler.update_ongage_csv_config(interval_seconds, lower_limit, upper_limit, job_id)
    if result["ok"]:
        return result
    else:
        raise HTTPException(status_code=500, detail=str(result))
    
@router.post("/csv-jobs/{job_id}/start")
def start_csv_job(job_id: str):
    try:
        updated = db_handler.start_ongage_csv_job(job_id)
        if updated == 0:
            raise HTTPException(
                status_code=400,
                detail="Job not in a startable state"
            )

        return {"ok": True, "status": "running"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/csv-jobs/{job_id}/pause")
def pause_csv_job(job_id: str):
    try:
        updated = db_handler.pause_ongage_csv_job(job_id)

        if updated == 0:
            raise HTTPException(
                status_code=400,
                detail="Job not running"
            )

        return {"ok": True, "status": "paused"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/csv-jobs/{job_id}/resume")
def resume_csv_job(job_id: str):
    try:
        updated = db_handler.resume_ongage_csv_job(job_id)
        if updated == 0:
            raise HTTPException(
                status_code=400,
                detail="Job not paused"
            )

        return {"ok": True, "status": "running"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ------------ Smart Cleaning Voluum Routes --------------
@router.post("/find-smart-cleaning-voluum")
async def find_smart_cleaning_voluum(country_code: str | None = Query(default=None), from_date: str | None = Query(default=None), to_date: str | None = Query(default=None)):
    try:
        file_id = str(uuid.uuid4())
        raw_path = os.path.join(RAW_SMART_CLEANING_VOLUUM_DIR, f"{file_id}.csv")
        original_filename = country_code + f"_{datetime.now().strftime('%Y-%m-%d')}.csv"

        if not db_handler.create_smart_cleaning_voluum_file_entry(file_id, raw_path, original_filename, country_code, from_date, to_date):
            raise HTTPException(status_code=404, detail= f"No offers found for the country code: {country_code}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/upload-smart-cleaning-voluum")
async def upload_smart_cleaning_voluum(file: UploadFile = File(...)):
    try:
        if not file.filename.lower().endswith(".csv"):
            raise HTTPException(status_code=400, detail="Only CSV files are supported")

        content = await file.read()
        file_id = str(uuid.uuid4())
        raw_path = os.path.join(RAW_SMART_CLEANING_VOLUUM_DIR, f"{file_id}.csv")

        with open(raw_path, "wb") as f:
            f.write(content)

        text = content.decode("utf-8", errors="replace")
        text = text.replace("\ufeff", "")
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        row_count = len(lines)

        if row_count == 0:
            raise HTTPException(status_code=400, detail="CSV file is empty")

        # Get all distinct offers across all conversions (not country-specific)
        result = db_handler.create_smart_cleaning_voluum_file_entry_from_upload(
            file_id, raw_path, file.filename, row_count
        )
        if not result:
            raise HTTPException(status_code=404, detail="No offers found in the conversions database")

        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/smart-cleaning-files-voluum")
def list_encrypted_files(status: str | None = Query(default=None)):
    try:
        if status:
            statuses = status.split(",")
            rows = db_handler.list_smart_cleaning_voluum_files(statuses)
        else:
            rows = db_handler.list_smart_cleaning_voluum_files()
        return {"items": rows}
    except Exception as e:
        print(e)

class SmartCleaningProcessRequest(BaseModel):
    offers: list[str]

@router.post("/{file_id}/process-smart-cleaning-files-voluum")
def start_processing_smart_cleaning_voluum(file_id: str, body: SmartCleaningProcessRequest, background_tasks: BackgroundTasks, filter_regs: bool = Query(False), filter_last_24h: bool = Query(False)):
    if not body.offers:
        raise HTTPException(status_code=400, detail="At least one offer is required")
    try:
        ok = db_handler.mark_smart_cleaning_files_voluum_processing(file_id=file_id)
    except Exception:
        raise HTTPException(status_code=409, detail="Another file is already processing")
    if not ok:
        raise HTTPException(status_code=400, detail="File not found or not eligible for processing")

    background_tasks.add_task(_process_smart_cleaning_files_voluum_job, file_id, body.offers, filter_regs, filter_last_24h)
    return {"ok": True, "file_id": file_id, "status": "processing"}

@router.post("/{file_id}/archive-smart-cleaning-files-voluum")
def archive_smart_cleaning_voluum_file(file_id: str):
    try:
        db_handler.archive_smart_cleaning_voluum_file(file_id)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/{file_id}/delete-smart-cleaning-files-voluum")
def delete_smart_cleaning_voluum_file(file_id: str):
    try:
        db_handler.delete_smart_cleaning_voluum_file_entry(file_id)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{file_id}/download-smart-cleaning-files-voluum")
def download_processed(file_id: str):
    try:
        meta = db_handler.get_smart_cleaning_files_voluum_file_paths(file_id)
        print(meta)
        if not meta:
            raise HTTPException(status_code=404, detail="File not found")
        if meta["status"] != "processed" or not meta["cleaned_file_path"]:
            raise HTTPException(status_code=400, detail="File not processed yet")

        path = meta["cleaned_file_path"]
        if not os.path.exists(path):
            raise HTTPException(status_code=404, detail="Processed file missing on disk")

        download_name = f"processed_voluum_click_{meta['original_filename']}"
        print(download_name)
        return FileResponse(
            path,
            media_type="text/csv",
            filename=download_name
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def _process_smart_cleaning_files_voluum_job(file_id: str, offer_names: list[str], filter_regs: bool = False, filter_last_24h: bool = False):
    """
    Runs in background. Reads raw CSV, cleans, writes processed CSV,
    computes stats, updates DB. Supports multiple offers.
    """
    try:
        meta = db_handler.get_smart_cleaning_files_voluum_file_paths(file_id)
        if not meta:
            return
        raw_path = meta["raw_file_path"]
        processed_path = os.path.join(PROCESSED_SMART_CLEANING_VOLUUM_DIR, f"{file_id}.csv")
        # Read raw CSV — may have 1 column (uploaded) or 2 columns (country-code search)
        with open(raw_path, "r", encoding="utf-8", newline="") as f:
            content = f.read()

        # Detect if CSV has 2 columns (record, timestamp_created) or 1 column (record only)
        first_line = content.split("\n")[0] if content else ""
        has_timestamp = "," in first_line

        if has_timestamp:
            reader = csv.DictReader(io.StringIO(content), fieldnames=["record", "timestamp_created"])
            rows = list(reader)
            values = [row['record'] for row in rows]
            # Build a dict mapping phone number -> timestamp string
            timestamp_map = {}
            for row in rows:
                timestamp_map[row['record']] = row['timestamp_created']
        else:
            reader = csv.DictReader(io.StringIO(content), fieldnames=["record"])
            rows = list(reader)
            values = [row['record'] for row in rows]
            timestamp_map = {}

        # If filter_last_24h, remove numbers that appear in voluum_live_events
        # within the last 24 hours for the selected offer(s)
        if filter_last_24h:
            live_24h_numbers = db_handler.get_live_events_last_24h(offer_names)
            before_count = len(values)
            values = [v for v in values if v not in live_24h_numbers]
            print(f"After last-24h SNS filtering: {len(values)} keys remain (removed {before_count - len(values)})")

        # Filter across all selected offers
        filtered_rows = values
        for offer_name in offer_names:
            filtered_rows = db_handler.filter_phone_numbers_offers(filtered_rows, offer_name, filter_regs)

        count = len(filtered_rows)
        if filtered_rows:
            with open(processed_path, "w", encoding="utf-8", newline="") as out:
                w = csv.writer(out)
                for pr in filtered_rows:
                    ts = timestamp_map.get(pr, "")
                    if ts:
                        w.writerow([pr, ts])
                    else:
                        w.writerow([pr])

        combined_offer_name = "^".join(offer_names)
        db_handler.mark_smart_cleaning_files_voluum_processed(
            file_id=file_id,
            processed_path=processed_path,
            record_count=count,
            offer_name=combined_offer_name
        )

    except Exception as e:
        db_handler.mark_smart_cleaning_files_voluum_failed(file_id, str(e))

# ------------ REG Search Routes --------------

@router.post("/reg-search/search")
async def reg_search_by_country(country_code: str | None = Query(default=None), from_date: str | None = Query(default=None), to_date: str | None = Query(default=None)):
    try:
        if not country_code:
            raise HTTPException(status_code=400, detail="country_code is required")

        file_id = str(uuid.uuid4())
        original_filename = f"REG_{country_code}_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}"

        result = db_handler.create_reg_search_entry(file_id, original_filename, country_code, from_date, to_date)
        if not result:
            raise HTTPException(status_code=404, detail=f"No offers found for country code: {country_code}")

        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/reg-search/files")
def list_reg_search_files(status: str | None = Query(default=None)):
    try:
        if status:
            statuses = status.split(",")
            rows = db_handler.list_reg_search_files(statuses)
        else:
            rows = db_handler.list_reg_search_files()
        return {"items": rows}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))

class RegSearchProcessRequest(BaseModel):
    offers: list[str]

@router.post("/reg-search/{file_id}/process")
def process_reg_search(file_id: str, body: RegSearchProcessRequest, background_tasks: BackgroundTasks):
    if not body.offers:
        raise HTTPException(status_code=400, detail="At least one offer is required")
    try:
        ok = db_handler.mark_reg_search_processing(file_id=file_id)
    except Exception:
        raise HTTPException(status_code=409, detail="Another search is already processing")
    if not ok:
        raise HTTPException(status_code=400, detail="Entry not found or not eligible for processing")

    background_tasks.add_task(_process_reg_search_job, file_id, body.offers)
    return {"ok": True, "file_id": file_id, "status": "processing"}

@router.get("/reg-search/{file_id}/download")
def download_reg_search(file_id: str):
    try:
        meta = db_handler.get_reg_search_meta(file_id)
        if not meta:
            raise HTTPException(status_code=404, detail="Entry not found")
        if meta["status"] != "processed" or not meta["cleaned_file_path"]:
            raise HTTPException(status_code=400, detail="Not processed yet")

        path = meta["cleaned_file_path"]
        if not os.path.exists(path):
            raise HTTPException(status_code=404, detail="Processed file missing on disk")

        download_name = f"reg_only_{meta['original_filename']}.csv"
        return FileResponse(path, media_type="text/csv", filename=download_name)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/reg-search/{file_id}/archive")
def archive_reg_search(file_id: str):
    try:
        db_handler.archive_reg_search_file(file_id)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/reg-search/{file_id}/delete")
def delete_reg_search(file_id: str):
    try:
        db_handler.delete_reg_search_file(file_id)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def _process_reg_search_job(file_id: str, offer_names: list[str]):
    """
    Background job: find REG-only numbers for the given country + multiple offers,
    write them to CSV, update DB. Reads from_date/to_date from DB.
    """
    try:
        meta = db_handler.get_reg_search_meta(file_id)
        if not meta:
            return

        country_code = meta["country_code"]
        from_date = meta.get("from_date")
        to_date = meta.get("to_date")
        processed_path = os.path.join(PROCESSED_REG_SEARCH_DIR, f"{file_id}.csv")

        reg_only_numbers = db_handler.find_reg_only_numbers(country_code, offer_names, from_date, to_date)
        count = len(reg_only_numbers)

        # Write to CSV
        with open(processed_path, "w", encoding="utf-8", newline="") as out:
            w = csv.writer(out)
            for num in reg_only_numbers:
                w.writerow([num])

        offers_joined = "^".join(offer_names)
        db_handler.mark_reg_search_processed(
            file_id=file_id,
            cleaned_path=processed_path,
            total_regs=count,
            clean_count=count,
            offer_name=offers_joined,
        )

    except Exception as e:
        db_handler.mark_reg_search_failed(file_id, str(e))

# ------------ Blacklist CSV Import Routes --------------

@router.post("/import-blacklist-csv")
async def import_blacklist_csv(file: UploadFile = File(...)):
    """
    Upload a CSV file and import all rows into the blacklist table.
    CSV must have columns matching the blacklist table schema.
    """
    try:
        if not file.filename.lower().endswith(".csv"):
            raise HTTPException(status_code=400, detail="Only CSV files are supported")

        content = await file.read()
        file_id = str(uuid.uuid4())
        csv_path = os.path.join(RAW_DIR, f"blacklist_import_{file_id}.csv")

        with open(csv_path, "wb") as f:
            f.write(content)

        row_count = db_handler.import_csv_into_blacklist(csv_path)

        return {
            "ok": True,
            "original_filename": file.filename,
            "rows_imported": row_count,
        }
    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))

# --------------------------------------------