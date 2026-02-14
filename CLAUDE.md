# CLAUDE.md — Optimizer Backend Project Memory

## Project Overview

**Name:** optimizer-backend
**Purpose:** Phone number optimization platform — ingests ad traffic data from Voluum, classifies phone numbers by OS version into whitelist/blacklist/monitor categories, provides CSV cleaning, HLR phone validation, AES encryption, SMS broadcast analytics (MessageWhiz/MMD), and Ongage email marketing sync.
**Stack:** Python 3.11 · FastAPI · PostgreSQL (psycopg2, raw SQL) · Docker · AWS SQS
**Location:** `/Users/michaelfinderup/Documents/GitHub/optimizer-backend/`

---

## Architecture At a Glance

```
FastAPI (router.py, 1549 lines, 57 endpoints, ZERO auth)
    ├── VoluumDataHandler    → Voluum REST API (traffic, conversions, offers, campaigns)
    ├── OngageDataHandler    → Ongage REST API (email list sync, GB only)
    ├── MMDDataHandler       → MessageWhiz REST API (SMS broadcasts, campaign stats)
    ├── EncryptionHandler    → AES-256-GCM phone encryption
    ├── DBHandler            → PostgreSQL (2527 lines, 60+ methods, 22 tables, raw SQL)
    └── SQSHandler           → AWS SQS consumer (real-time Voluum SNS events)

Cron (Python threads, not a scheduler):
    ├── crone_job_conversion_clickers.py → conversions every 5min, reports every 24h
    └── update_costs.py                 → daily 05:00 UTC cost sync
```

---

## Key Files & Their Roles

| File | Lines | Purpose |
|------|-------|---------|
| `main.py` | 17 | FastAPI entry point. Root `/` and `/health`. Uvicorn on port 8800 (local) / 8000 (Docker). |
| `app/router.py` | 1,549 | ALL 57 endpoints in one file. Uses BackgroundTasks for heavy processing. Global handler instances at module level. |
| `app/schema.py` | ~30 | Minimal Pydantic models: `RecordRequest`, `SyncDataInRangeRequest`, `SyncDate`, `CSVDataSyncRequest`. No response schemas. |
| `app/utils/db_handler.py` | 2,527 | Monolithic `DBHandler` class — 60+ methods covering all 22 tables. Raw psycopg2, no ORM, no pooling. |
| `app/utils/voluum_data_handler.py` | 245 | Voluum API — traffic source fetching, pagination, OS-version classification, phone decryption. |
| `app/utils/voluum_access_key_handler.py` | 55 | Voluum session token cache with 4-hour TTL. Infinite retry loop on failure. |
| `app/utils/ongage_data_handler.py` | 91 | Ongage email marketing — syncs GB conversions as `+{phone}@yourmobile.com` in batches of 500. |
| `app/utils/mmd_data_handler.py` | 489 | MessageWhiz/MMD — broadcast data, campaign stats, link tracking. Rate-limited concurrent requests. |
| `app/utils/encryption_handler.py` | 35 | AES-256-GCM encrypt/decrypt using `AES_KEY` env var (base64-encoded). |
| `app/utils/sqs_handler.py` | 216 | AWS SQS continuous polling consumer. **Auto-executes on import (line 216).** Only file with proper logging. |
| `app/utils/crone_jobs/crone_job_conversion_clickers.py` | 177 | Two-thread cron: conversions (5min) + reports (24h). Calls back to own API via HTTP. |
| `app/utils/crone_jobs/update_costs.py` | 142 | Daily cron at 05:00 UTC. Calls `/update-costs-for-mmd` and `/save-data-for-previous-date`. |
| `sql/create_live_events_tables.sql` | — | DDL for `voluum_offers`, `voluum_campaigns`, `voluum_live_events`, `raw_live_voluum_sns_data`. |
| `app/utils/risky_operators.json` | — | ~270 risky mobile network operator names for HLR filtering. |
| `app/utils/country_codes.json` | — | Country name → ISO code mapping. |
| `app/utils/calling_codes.json` | — | Phone prefix → country mapping. |

---

## Database Tables (22)

1. `api_voluum_ts_sources` — Raw traffic source data
2. `blacklist` — Blacklisted phones (UNIQUE on custom_variable_1)
3. `whitelist` — Whitelisted phones (UNIQUE on custom_variable_1)
4. `monitor` — Monitored phones (UNIQUE on custom_variable_1)
5. `main_database` — Main phone database
6. `api_voluum_conversions` — Conversion tracking
7. `uploaded_csv_files` — CSV file management with processing locks
8. `csv_processing_stats` — Per-file processing statistics
9. `manual_update_imports` — Manual import history
10. `db_exports` — Database export tracking
11. `uploaded_encryption_files` — Encryption file management
12. `api_mmd_broadcasts_files` — MMD broadcast files
13. `broadcast_campaign_stats` — Broadcast campaign stats (UNIQUE on broadcast_id)
14. `api_hlr_live_numbers` — HLR lookup results
15. `csv_jobs` — CSV job management for Ongage (state machine: draft→running→paused)
16. `csv_active_records` — Active CSV records for Ongage processing
17. `uploaded_smart_cleaning_voluum_files` — Smart cleaning file management
18. `uploaded_reg_search_files` — REG search file management
19. `voluum_offers` — Offer ID→name lookup
20. `voluum_campaigns` — Campaign ID→name lookup
21. `voluum_live_events` — Live SNS events deduped by custom_variable_1
22. `raw_live_voluum_sns_data` — Raw SNS events (no dedup)

No migration framework. All schema managed via manual SQL.

---

## Environment Variables (22 required)

```bash
# PostgreSQL
DB_HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=

# Voluum API
VOLUUM_ACCESS_ID=
VOLUUM_ACCESS_KEY=

# Ongage API
LIST_ID=
X_USERNAME=
X_PASSWORD=
X_ACCOUNT_CODE=

# MessageWhiz / MMD API
MMD_API_KEY=                # CAUTION: partially hardcoded in mmd_data_handler.py

# HLR Lookup
HLRLOOKUP_APIKEY=
HLRLOOKUP_SECRET=

# AWS SQS
AWS_REGION=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
SQS_QUEUE_URL=

# Encryption
AES_KEY=                    # Base64-encoded AES-256 key

# File Storage (have defaults)
UPLOAD_ROOT=uploads
DB_ROOT=db_files
STORAGE_DIR=/app/storage

# Cron / Internal
BACKEND_BASE_URL=           # API callback URL for cron jobs

# HLR Polling (optional, have defaults)
HLR_POLL_MAX_SECONDS=18000
HLR_POLL_EVERY_SECONDS=5
```

No `.env.example` file exists in the project.

---

## API Endpoints (57 total)

### Health
- `GET /` — Root health check
- `GET /health` — Health status
- `GET /get-metrics` — Row counts for core tables

### Voluum Data Sync
- `POST /sync-voluum-reports` — Sync traffic source data
- `POST /sync-voluum-reports_for_range` — Sync for date range
- `POST /find_records` — Find phone in ts_source
- `POST /sync-voluum-conversions` — Sync conversions + Ongage
- `POST /sync-voluum-conversions-for-range` — Sync conversions for range

### CSV Upload & Processing
- `POST /upload` — Upload CSV for cleaning
- `GET /files` — List uploaded files
- `POST /{file_id}/process` — Start background processing
- `GET /{file_id}/stats` — Get processing stats
- `GET /{file_id}/download` — Download processed CSV
- `POST /{file_id}/archive` — Archive file
- `POST /{file_id}/delete` — Delete file

### Manual Database Updates
- `POST /db-manual-update/import` — Manual blacklist/monitor CSV import
- `GET /db-manual-update/history` — List import history
- `GET /db-manual-update/{import_id}/download` — Download import results

### HLR Lookup
- `POST /{file_id}/hlr` — Start HLR batch lookup
- `GET /{file_id}/hlr` — Get HLR status
- `GET /{file_id}/hlr/download` — Download HLR clean results
- `GET /{file_id}/hlr/raw` — Download raw HLR data

### Database Export
- `POST /process_db` — Export table to CSV
- `POST /fetch-latest-db-export` — Get latest export metadata
- `GET /db-download` — Download export CSV

### MMD / Broadcasts
- `GET /get-live-broadcasts-stream` — SSE live broadcast stream
- `GET /save-data-for-previous-date` — Save yesterday's MMD data
- `GET /save-data-for-specific-date` — Save specific date MMD data
- `GET /update-costs-for-mmd` — Update Voluum costs from MMD
- `GET /files-broadcasts` — List broadcast files
- `GET /{file_id}/download-broadcasts-history` — Download broadcast history

### Encryption
- `POST /upload-encrypted` — Upload file for encryption
- `GET /encrypted-files` — List encryption files
- `POST /{file_id}/process-encrypted` — Start encryption
- `GET /{file_id}/download-encrypted` — Download encrypted file
- `POST /{file_id}/archive-encrypted` — Archive
- `POST /{file_id}/delete-encrypted` — Delete

### CSV Jobs (Ongage Sync)
- `POST /csv-jobs/upload` — Upload CSV job
- `GET /csv-jobs` — List jobs
- `PATCH /csv-jobs/{job_id}` — Update config
- `POST /csv-jobs/{job_id}/start` — Start job
- `POST /csv-jobs/{job_id}/pause` — Pause job
- `POST /csv-jobs/{job_id}/resume` — Resume job

### Smart Cleaning (Voluum)
- `POST /find-smart-cleaning-voluum` — Find by country
- `POST /upload-smart-cleaning-voluum` — Upload CSV
- `GET /smart-cleaning-files-voluum` — List files
- `POST /{file_id}/process-smart-cleaning-files-voluum` — Process
- `POST /{file_id}/archive-smart-cleaning-files-voluum` — Archive
- `POST /{file_id}/delete-smart-cleaning-files-voluum` — Delete
- `GET /{file_id}/download-smart-cleaning-files-voluum` — Download

### REG Search
- `POST /reg-search/search` — Search REG numbers by country
- `GET /reg-search/files` — List files
- `POST /reg-search/{file_id}/process` — Process
- `GET /reg-search/{file_id}/download` — Download
- `POST /reg-search/{file_id}/archive` — Archive
- `POST /reg-search/{file_id}/delete` — Delete

### Blacklist Import
- `POST /import-blacklist-csv` — Bulk CSV import (**BUG: inserts into monitor table**)

---

## Known Critical Issues (P1)

1. **HARDCODED API KEY** — `mmd_data_handler.py` lines 50–51, 104: `"fc416a66-fdcc-480b-a693-f85007723364"` for MessageWhiz. Some methods use env var, some use hardcoded value.
2. **SQL INJECTION** — `db_handler.py` line 358: f-string interpolation in WHERE clause. Use parameterized queries.
3. **ZERO AUTHENTICATION** — All 57 endpoints have no auth. Anyone with network access can read/write/delete everything.
4. **SQS AUTO-EXECUTE** — `sqs_handler.py` line 216: `SQSHandler().run()` starts infinite polling loop at module import time.
5. **MISSING .env.example** — 22 required environment variables with zero documentation for new developers.

---

## Known Bugs (P2)

- **Function name collisions** in `router.py`: `process_csv` defined 3x, `download_processed` 5x, `start_processing` 2x, `list_files` 2x, `archive_file` 2x, `delete_file` 2x. Python silently overwrites — only last definition is active.
- **`import_csv_into_blacklist()`** in `db_handler.py` line 2176 inserts into `monitor` table, NOT `blacklist`.
- **Port mismatch**: `main.py` uses port 8800, Dockerfile uses 8000.
- **`boto3` missing** from `requirements.txt` — SQS handler cannot import.
- **`flask` in requirements.txt** but never used anywhere.
- **SQS message deletion commented out** (lines 156–158) — messages reprocess infinitely.
- **Module-level API call** in `mmd_data_handler.py` lines 15–16 — triggers Voluum token fetch on import.
- **Typo** in `db_handler.py` line 465: `reason='INAVLID_PHONE_NUMBER'`.

---

## Architecture Debt

- `router.py` (1,549 lines) — monolithic, all 57 endpoints in one file. Split into domain routers.
- `db_handler.py` (2,527 lines) — god object with 60+ methods. Split into domain repositories.
- No connection pooling — single persistent psycopg2 connection per DBHandler instance.
- No ORM, no migration framework — all schema managed manually.
- `print()` everywhere instead of `logging` module (except sqs_handler.py).
- Zero test files — no pytest, no test infrastructure.
- Cron jobs use raw `threading.Thread` and call back to own API via HTTP instead of direct function imports.
- No Pydantic response schemas — most endpoints return unvalidated dicts.
- No dependency injection — global handler instances shared across all requests.
- Hardcoded business logic: traffic source IDs in `voluum_data_handler.py:114`, country filter `'GB'` in `ongage_data_handler.py:31`.
- `datetime.utcnow()` usage (deprecated since Python 3.12).

---

## OS Version Classification Logic

Used in `voluum_data_handler.py` to categorize phone numbers:

| Category | OS Versions |
|----------|-------------|
| **BLACKLIST** | iOS ≤10, Android ≤7, Windows 7/XP, Ubuntu |
| **MONITOR** | Android 8–9/unknown/17+, Windows 10/11, Linux |
| **WHITELIST** | iOS 11+, macOS, Android 10–16 |

---

## File Storage Directories

All relative to `UPLOAD_ROOT` (default: `uploads`):

```
uploads/
├── raw/                          # Uploaded CSV files
├── processed/                    # Cleaned CSV output
├── manual_update/                # Manual import files
├── hlr/                          # HLR lookup results
├── raw_encrypted/                # Files pending encryption
├── processed_encrypted/          # Encrypted output
├── broadcasts/                   # MMD broadcast history
├── raw_smart_cleaning_voluum/    # Smart cleaning input
├── processed_smart_cleaning_voluum/  # Smart cleaning output
└── processed_reg_search/         # REG search results

db_files/
└── exports/                      # Database export CSVs
```

In Docker, `UPLOAD_ROOT=/app/storage/uploads` and files persist via named volume `optimizer_storage`.

---

## Docker Configuration

- **Base image:** `python:3.11-slim`
- **Exposed port:** 8000
- **docker-compose port mapping:** 80:8000
- **Volumes:** `optimizer_storage` (named) at `/app/storage`, `./logs` at `/app/logs`
- **env_file:** `.env`
- **Restart policy:** `always`

---

## External API Dependencies

| API | Base URL | Auth | Rate Limits |
|-----|----------|------|-------------|
| Voluum | `https://api.voluum.com` | Session token (4h TTL) from Access ID/Key | Standard API limits |
| Ongage | `https://api.ongage.net` | Username/Password/AccountCode headers | Batches of 500 |
| MessageWhiz/MMD | `https://broadcasts.messagewhiz.com` | API Key header | 10 concurrent, 0.25s delay |
| HLR Lookup | `https://www.hlrlookup.com` | API Key + Secret | Batch processing with polling |
| AWS SQS | Region-specific endpoint | IAM Access Key/Secret | Standard SQS limits |

---

## Deliverables Produced

1. **`TECHNICAL_DEEP_DIVE.md`** — Full 5-phase code review: Project Discovery, Architecture Analysis, AWS Deployment Map, Endpoint Documentation (57 endpoints), Error & Improvement Matrix (31 issues).
2. **`DEPLOYMENT_BRIEF.md`** — Executive summary for Head of Back-end: Stack Overview, Deployment Checklist, Environment Variables, Critical Path (2–3 week timeline), Known Risks, Quick Wins (9 fixes, ~2.5 hours).

---

## Quick Commands

```bash
# Run locally
python main.py                    # Starts on port 8800

# Run with Docker
docker-compose up --build         # Starts on port 80 → 8000

# Health check
curl http://localhost:8000/health

# Get table metrics
curl http://localhost:8000/get-metrics
```

---

## Review Verdict (2026-02-13)

**NOT production-ready.** 5 P1 critical issues block deployment. 9 quick-win fixes (~2.5 hours) address the most urgent security and reliability concerns. Full remediation estimated at 2–3 weeks with parallel developer (code fixes) and DevOps (infra provisioning) tracks.
