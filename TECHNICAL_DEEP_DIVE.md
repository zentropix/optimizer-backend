# Optimizer Backend — Technical Deep-Dive

**Review Date:** 2026-02-13
**Reviewer:** Senior DevOps Engineer — Code Review & Deployment Readiness Assessment
**Project:** optimizer-backend
**Stack:** Python 3.11 / FastAPI / PostgreSQL / Docker

---

## Table of Contents

1. [Phase 1 — Project Discovery](#phase-1--project-discovery)
2. [Phase 2 — Code Architecture Analysis](#phase-2--code-architecture-analysis)
3. [Phase 3 — AWS Deployment Map](#phase-3--aws-deployment-map)
4. [Phase 4 — Endpoint Documentation](#phase-4--endpoint-documentation)
5. [Phase 5 — Error & Improvement Matrix](#phase-5--error--improvement-matrix)

---

## Phase 1 — Project Discovery

### 1.1 Stack Summary

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11 |
| Framework | FastAPI + Uvicorn |
| Database | PostgreSQL (psycopg2-binary, raw SQL) |
| Containerization | Docker + docker-compose |
| Queue | AWS SQS (boto3 — **missing from requirements.txt**) |
| Encryption | AES-256-GCM (cryptography library) |
| External APIs | Voluum, Ongage, MessageWhiz/MMD, HLR Lookup |
| Cron | Custom Python threads (no scheduler framework) |

### 1.2 Entry Points

| Entry Point | File | Purpose |
|-------------|------|---------|
| Web API | `main.py` → `uvicorn` | FastAPI application on port 8000 (Docker) / 8800 (local) |
| SQS Consumer | `app/utils/sqs_handler.py` | Continuous SQS polling loop (**auto-executes on import**) |
| Cron: Conversions | `app/utils/crone_jobs/crone_job_conversion_clickers.py` | 5-min conversions + 24h reports |
| Cron: Costs | `app/utils/crone_jobs/update_costs.py` | Daily 05:00 UTC cost sync |

### 1.3 Project Structure

```
optimizer-backend/
├── main.py                          # FastAPI entry point (17 lines)
├── Dockerfile                       # Python 3.11-slim container
├── docker-compose.yml               # Port 80→8000, .env, volumes
├── requirements.txt                 # Dependencies (issues noted below)
├── pyproject.toml                   # Minimal project metadata
├── README.md                        # Empty (only header)
├── .gitignore                       # Properly ignores .env
├── sql/
│   └── create_live_events_tables.sql
├── app/
│   ├── router.py                    # ALL endpoints (1,549 lines)
│   ├── schema.py                    # Pydantic models (minimal)
│   └── utils/
│       ├── db_handler.py            # Database operations (2,527 lines)
│       ├── voluum_data_handler.py   # Voluum API client (245 lines)
│       ├── voluum_access_key_handler.py  # Token caching (55 lines)
│       ├── ongage_data_handler.py   # Ongage email API (91 lines)
│       ├── mmd_data_handler.py      # MessageWhiz/MMD API (489 lines)
│       ├── encryption_handler.py    # AES-GCM encryption (35 lines)
│       ├── sqs_handler.py           # AWS SQS consumer (216 lines)
│       ├── country_codes.json       # Country name → ISO mapping
│       ├── calling_codes.json       # Phone prefix → country mapping
│       ├── risky_operators.json     # ~270 risky MNO names
│       └── crone_jobs/
│           ├── crone_job_conversion_clickers.py  # (177 lines)
│           └── update_costs.py                   # (142 lines)
```

### 1.4 Database Schema (22 Tables)

| # | Table | Purpose | Key Columns |
|---|-------|---------|-------------|
| 1 | `api_voluum_ts_sources` | Raw traffic source data | custom_variable_1, os_version, category |
| 2 | `blacklist` | Blacklisted phone numbers | custom_variable_1 (UNIQUE) |
| 3 | `whitelist` | Whitelisted phone numbers | custom_variable_1 (UNIQUE) |
| 4 | `monitor` | Monitored phone numbers | custom_variable_1 (UNIQUE) |
| 5 | `main_database` | Main phone database | phone, country, status |
| 6 | `api_voluum_conversions` | Conversion tracking | click_id, conversion_type |
| 7 | `uploaded_csv_files` | CSV file management | id, filename, status, processing_lock |
| 8 | `csv_processing_stats` | Processing statistics per file | file_id, total, blacklisted, whitelisted |
| 9 | `manual_update_imports` | Manual import history | id, type, filename |
| 10 | `db_exports` | Database export tracking | id, table_name, export_path |
| 11 | `uploaded_encryption_files` | Encryption file mgmt | id, filename, status |
| 12 | `api_mmd_broadcasts_files` | MMD broadcast files | id, date, file_path |
| 13 | `broadcast_campaign_stats` | Broadcast campaign stats | broadcast_id (UNIQUE), campaign data |
| 14 | `api_hlr_live_numbers` | HLR lookup results | phone, mcc, mnc, operator |
| 15 | `csv_jobs` | CSV job mgmt for Ongage | id, state (draft/running/paused) |
| 16 | `csv_active_records` | Active Ongage records | job_id, record_data |
| 17 | `uploaded_smart_cleaning_voluum_files` | Smart cleaning files | id, filename, status |
| 18 | `uploaded_reg_search_files` | REG search files | id, filename, status |
| 19 | `voluum_offers` | Offer ID→name lookup | offer_id, offer_name |
| 20 | `voluum_campaigns` | Campaign ID→name lookup | campaign_id, campaign_name |
| 21 | `voluum_live_events` | Live SNS events (deduped) | custom_variable_1 (UNIQUE) |
| 22 | `raw_live_voluum_sns_data` | Raw SNS events (no dedup) | raw payload |

### 1.5 External Integrations

| Service | Handler | Auth Method | Purpose |
|---------|---------|-------------|---------|
| Voluum API | `voluum_data_handler.py` | Access ID/Key → Session Token (4h TTL) | Traffic data, conversions, offers, campaigns |
| Ongage API | `ongage_data_handler.py` | Username/Password/Account Code headers | Email list sync for GB conversions |
| MessageWhiz/MMD | `mmd_data_handler.py` | API Key (partially hardcoded!) | SMS broadcast data, campaign stats |
| HLR Lookup | `db_handler.py` (inline) | API Key + Secret | Phone number validation |
| AWS SQS | `sqs_handler.py` | AWS Access Key/Secret | Real-time Voluum event consumption |

### 1.6 Environment Variables

| Variable | Used In | Required | Notes |
|----------|---------|----------|-------|
| `DB_HOST` | db_handler.py | Yes | PostgreSQL host |
| `DB_NAME` | db_handler.py | Yes | Database name |
| `DB_USER` | db_handler.py | Yes | Database user |
| `DB_PASSWORD` | db_handler.py | Yes | Database password |
| `VOLUUM_ACCESS_ID` | voluum_data_handler.py, voluum_access_key_handler.py | Yes | Voluum API access |
| `VOLUUM_ACCESS_KEY` | voluum_data_handler.py, voluum_access_key_handler.py | Yes | Voluum API secret |
| `LIST_ID` | ongage_data_handler.py | Yes | Ongage list identifier |
| `X_USERNAME` | ongage_data_handler.py | Yes | Ongage username |
| `X_PASSWORD` | ongage_data_handler.py | Yes | Ongage password |
| `X_ACCOUNT_CODE` | ongage_data_handler.py | Yes | Ongage account code |
| `MMD_API_KEY` | mmd_data_handler.py | Partial | **Some methods use hardcoded key instead** |
| `HLRLOOKUP_APIKEY` | db_handler.py | Yes | HLR Lookup API key |
| `HLRLOOKUP_SECRET` | db_handler.py | Yes | HLR Lookup API secret |
| `AWS_REGION` | sqs_handler.py | Yes | AWS region for SQS |
| `AWS_ACCESS_KEY_ID` | sqs_handler.py | Yes | AWS IAM access key |
| `AWS_SECRET_ACCESS_KEY` | sqs_handler.py | Yes | AWS IAM secret key |
| `SQS_QUEUE_URL` | sqs_handler.py | Yes | SQS queue URL |
| `AES_KEY` | encryption_handler.py | Yes | Base64-encoded AES-256 key |
| `UPLOAD_ROOT` | router.py | No | Default: `uploads` |
| `DB_ROOT` | router.py | No | Default: `db_files` |
| `STORAGE_DIR` | docker-compose.yml | No | Set to `/app/storage` in compose |
| `BACKEND_BASE_URL` | cron jobs | Yes | API callback URL |
| `HLR_POLL_MAX_SECONDS` | db_handler.py | No | Default: 18000 (5 hours) |
| `HLR_POLL_EVERY_SECONDS` | db_handler.py | No | Default: 5 |

> **CRITICAL:** No `.env.example` file exists. New developers have zero guidance on required configuration.

---

## Phase 2 — Code Architecture Analysis

### 2.1 Component Analysis

#### `main.py` — Application Entry Point
- **Purpose:** Bootstrap FastAPI, mount router, define health endpoints
- **Dependencies:** FastAPI, app.router
- **Risk:** Port mismatch — `uvicorn.run(port=8800)` vs Dockerfile `EXPOSE 8000`. The `if __name__` block is only for local dev, Docker CMD uses port 8000, but this will confuse developers.

#### `app/router.py` — API Router (1,549 lines)
- **Purpose:** ALL endpoint definitions, request handling, background task orchestration
- **Dependencies:** Every utility module, FastAPI, BackgroundTasks, StreamingResponse
- **Data Flow:** HTTP Request → router handler → utility class method → DB/API → HTTP Response
- **Risk Flags:**
  - **CRITICAL:** Multiple function name collisions — `process_csv` defined 3 times, `download_processed` defined 5 times, `start_processing` 2 times, `list_files` 2 times, `archive_file` 2 times, `delete_file` 2 times. Python silently overwrites previous definitions; only the last definition is active.
  - **CRITICAL:** Zero authentication/authorization on all 57 endpoints
  - **HIGH:** Global handler instantiation at module level — all requests share state
  - **HIGH:** Creates new `DBHandler()` per call in some endpoints (line 276) while using global in others — inconsistent connection management
  - **MEDIUM:** Monolithic file — should be split into domain-specific routers

#### `app/utils/db_handler.py` — Database Layer (2,527 lines)
- **Purpose:** All PostgreSQL operations — CRUD for 22 tables, batch upserts, CSV processing, HLR, exports
- **Dependencies:** psycopg2, pandas, requests (for HLR API calls — violates single-responsibility)
- **Data Flow:** Handler methods → `_cursor()` context manager → raw SQL → psycopg2
- **Risk Flags:**
  - **CRITICAL:** SQL injection on line 358: `f'SELECT * FROM public."api_voluum_ts_sources" WHERE custom_variable_1 = \'{key}\''`
  - **HIGH:** No connection pooling — single persistent connection, reconnect-on-failure
  - **HIGH:** HLR API calls embedded in database handler (SRP violation)
  - **HIGH:** 60+ methods in single class — god object anti-pattern
  - **MEDIUM:** `import_csv_into_blacklist()` method inserts into `monitor` table (line 2176–2214) — misleading name, likely a bug
  - **LOW:** Typo on line 465: `reason='INAVLID_PHONE_NUMBER'`

#### `app/utils/voluum_data_handler.py` — Voluum API Client (245 lines)
- **Purpose:** Fetch traffic data, conversions, offers, campaigns; classify records by OS version
- **Dependencies:** pandas, requests, encryption_handler
- **Data Flow:** API call → JSON pagination → DataFrame → classification → decryption
- **Risk Flags:**
  - **MEDIUM:** Hardcoded traffic source IDs on line 114 — should be configurable
  - **MEDIUM:** Duplicate session token creation — both this file and `voluum_access_key_handler.py` create tokens independently
  - **LOW:** `deprecated datetime.utcnow()` not used here but classification logic may need review for edge cases

#### `app/utils/voluum_access_key_handler.py` — Token Cache (55 lines)
- **Purpose:** Cache Voluum session tokens with 4-hour TTL, retry on failure
- **Dependencies:** requests, dotenv
- **Risk Flags:**
  - **LOW:** Retry loop sleeps 10 seconds indefinitely with no max retry count — could block forever

#### `app/utils/ongage_data_handler.py` — Ongage Email API (91 lines)
- **Purpose:** Sync conversion data to Ongage email marketing lists
- **Dependencies:** requests, dotenv
- **Data Flow:** Conversion records → filter GB only → generate email → batch 500 → POST to Ongage
- **Risk Flags:**
  - **MEDIUM:** Only processes country code 'GB' (line 31) — hardcoded business logic
  - **LOW:** Uses positional array indices (e.g., `record[14]`, `record[39]`) — extremely brittle, no column name references

#### `app/utils/mmd_data_handler.py` — MessageWhiz/MMD API (489 lines)
- **Purpose:** Fetch SMS broadcast data, campaign stats, link tracking
- **Dependencies:** requests, voluum_access_key_handler, concurrent.futures
- **Risk Flags:**
  - **CRITICAL:** Hardcoded API key `"fc416a66-fdcc-480b-a693-f85007723364"` on lines 50–51 and 104. Some methods use `os.getenv("MMD_API_KEY")` (line 150) while others use the hardcoded value — **inconsistent and insecure**
  - **HIGH:** Module-level execution: `VoluumAccessKeyHandler()` instantiated and `get_access_token()` called at import time (lines 15–16) — triggers API call on every module import
  - **MEDIUM:** `datetime.utcnow()` usage (deprecated since Python 3.12)
  - **MEDIUM:** Rate limiting via `time.sleep(0.25)` — fragile, should use proper rate limiter

#### `app/utils/encryption_handler.py` — AES Encryption (35 lines)
- **Purpose:** Encrypt/decrypt phone numbers using AES-256-GCM
- **Dependencies:** cryptography, base64
- **Risk Flags:**
  - **LOW:** Clean implementation, properly uses random nonces

#### `app/utils/sqs_handler.py` — AWS SQS Consumer (216 lines)
- **Purpose:** Consume real-time Voluum SNS events from SQS queue
- **Dependencies:** boto3, json, logging
- **Risk Flags:**
  - **CRITICAL:** Line 216: `SQSHandler().run()` executes at module import — importing this module starts an infinite polling loop
  - **HIGH:** Message deletion is commented out (lines 156–158) — messages will be reprocessed indefinitely
  - **MEDIUM:** `boto3` not in requirements.txt — will fail on fresh install
  - **POSITIVE:** Only file with proper `logging` + `RotatingFileHandler`

#### `app/utils/crone_jobs/` — Scheduled Tasks
- **Purpose:** Automated data sync — conversions every 5 min, costs daily at 05:00 UTC
- **Dependencies:** requests, threading, logging
- **Risk Flags:**
  - **MEDIUM:** Uses raw `threading.Thread` instead of a scheduler (APScheduler, celery-beat)
  - **MEDIUM:** Calls back to own API via HTTP (`BACKEND_BASE_URL`) instead of importing functions directly
  - **LOW:** Misspelled directory name: `crone_jobs` should be `cron_jobs`

#### `app/schema.py` — Pydantic Models
- **Purpose:** Request validation schemas
- **Risk Flags:**
  - **HIGH:** Minimal — only 4 request schemas, zero response schemas. Most endpoints accept raw dicts or have no validation.

### 2.2 Data Flow Diagram

```
                                    ┌─────────────┐
                                    │   Frontend   │
                                    └──────┬───────┘
                                           │ HTTP
                                           ▼
┌──────────────┐    HTTP        ┌─────────────────────┐
│  Cron Jobs   │───────────────▶│    FastAPI Router    │
│  (threads)   │   callback     │   (router.py)        │
└──────────────┘                │   57 endpoints       │
                                │   NO AUTH            │
                                └──┬──┬──┬──┬──┬──────┘
                                   │  │  │  │  │
                    ┌──────────────┘  │  │  │  └──────────────┐
                    ▼                 ▼  │  ▼                  ▼
            ┌──────────────┐  ┌─────────┐│┌──────────┐  ┌──────────┐
            │ VoluumData   │  │ MMDData  │││ Ongage   │  │ Encrypt  │
            │ Handler      │  │ Handler  │││ Handler  │  │ Handler  │
            └──────┬───────┘  └────┬────┘│└────┬─────┘  └──────────┘
                   │               │     │     │
                   ▼               ▼     │     ▼
            ┌──────────────┐  ┌─────────┐│┌──────────┐
            │ Voluum API   │  │ MMD API ││││ Ongage   │
            │ (external)   │  │(external│││ API      │
            └──────────────┘  └─────────┘│└──────────┘
                                         │
                                         ▼
                                  ┌──────────────┐
                                  │  DBHandler    │
                                  │  (2,527 lines)│
                                  │  60+ methods  │
                                  └──────┬───────┘
                                         │ psycopg2
                                         ▼
                                  ┌──────────────┐
                                  │  PostgreSQL   │
                                  │  22 tables    │
                                  └──────────────┘

        ┌──────────────┐
        │  SQS Handler │──▶ AWS SQS ──▶ DBHandler ──▶ PostgreSQL
        │  (auto-runs) │
        └──────────────┘
```

---

## Phase 3 — AWS Deployment Map

### 3.1 Required AWS Services

| Service | Purpose | Configuration |
|---------|---------|---------------|
| **ECS Fargate** or **EC2** | Run FastAPI container | 1 task, 2 vCPU / 4GB RAM minimum |
| **RDS PostgreSQL** | Database | db.t3.medium, Multi-AZ for production |
| **SQS** | Event queue for Voluum SNS | Standard queue, existing |
| **ALB** (Application Load Balancer) | HTTPS termination, routing | Port 443 → 8000 |
| **ECR** | Container registry | Store Docker images |
| **CloudWatch** | Logging + monitoring | Log groups per service |
| **Secrets Manager** or **SSM Parameter Store** | Environment variables | All 22 env vars |
| **S3** | File storage (replace local volumes) | For uploads, exports, encrypted files |
| **Route 53** | DNS | API domain |
| **ACM** | SSL/TLS certificate | For ALB HTTPS |
| **IAM** | Service roles | ECS task role, SQS access |
| **VPC** | Network isolation | Private subnets for RDS + ECS |
| **Security Groups** | Firewall rules | ALB→ECS (8000), ECS→RDS (5432) |

### 3.2 IAM Permissions Required

**ECS Task Role:**
```json
{
  "Effect": "Allow",
  "Action": [
    "sqs:ReceiveMessage",
    "sqs:DeleteMessage",
    "sqs:GetQueueAttributes",
    "s3:GetObject",
    "s3:PutObject",
    "s3:ListBucket",
    "logs:CreateLogStream",
    "logs:PutLogEvents",
    "secretsmanager:GetSecretValue"
  ],
  "Resource": ["*"]
}
```

### 3.3 Port Mapping

| Layer | Port | Protocol |
|-------|------|----------|
| ALB Listener | 443 | HTTPS |
| ALB → Target Group | 8000 | HTTP |
| ECS Container | 8000 | HTTP |
| RDS PostgreSQL | 5432 | TCP (private subnet only) |

### 3.4 Network Architecture

```
Internet
    │
    ▼
┌─────────┐     ┌──────────────┐     ┌─────────────┐
│ Route 53 │────▶│     ALB      │────▶│ ECS Fargate │
│  (DNS)   │     │ (Public SN)  │     │ (Private SN)│
└─────────┘     └──────────────┘     └──────┬──────┘
                                            │
                                     ┌──────▼──────┐
                                     │ RDS Postgres │
                                     │ (Private SN) │
                                     └─────────────┘
```

### 3.5 Migration Blockers

| Blocker | Severity | Action Required |
|---------|----------|-----------------|
| Local file storage (`/app/storage`) | HIGH | Migrate to S3 or EFS |
| `boto3` missing from requirements.txt | HIGH | Add to requirements.txt |
| SQS handler auto-executes on import | CRITICAL | Refactor to explicit startup |
| No health check on `/health` with DB ping | MEDIUM | Add DB connectivity check |
| SSE endpoint (`get-live-broadcasts-stream`) | MEDIUM | Verify ALB timeout settings (default 60s) |
| Cron jobs as threads | MEDIUM | Move to ECS Scheduled Tasks or EventBridge |

---

## Phase 4 — Endpoint Documentation

### 4.1 Core / Health

| Endpoint | Method | Auth | Request Body | Response | Dependencies |
|----------|--------|------|-------------|----------|-------------|
| `/` | GET | None | — | `{"message": "..."}` | — |
| `/health` | GET | None | — | `{"status": "ok"}` | — |
| `/get-metrics` | GET | None | — | `{"table_name": count, ...}` | DBHandler |

### 4.2 Voluum Data Sync

| Endpoint | Method | Auth | Request Body | Response | Dependencies |
|----------|--------|------|-------------|----------|-------------|
| `/sync-voluum-reports` | POST | None | `{"from_date": str, "to_date": str}` | `{"status": "ok", "data": {...}}` | VoluumDataHandler, DBHandler |
| `/sync-voluum-reports_for_range` | POST | None | `SyncDataInRangeRequest` | `{"status": "ok"}` | VoluumDataHandler, DBHandler |
| `/find_records` | POST | None | `RecordRequest` | `{"status": "ok", "data": [...]}` | DBHandler |
| `/sync-voluum-conversions` | POST | None | `{"from_date": str, "to_date": str}` | `{"status": "ok"}` | VoluumDataHandler, OngageDataHandler, DBHandler |
| `/sync-voluum-conversions-for-range` | POST | None | `SyncDataInRangeRequest` | `{"status": "ok"}` | VoluumDataHandler, DBHandler |

### 4.3 CSV Upload & Processing

| Endpoint | Method | Auth | Request Body | Response | Dependencies |
|----------|--------|------|-------------|----------|-------------|
| `/upload` | POST | None | `multipart/form-data (file)` | `{"id": int, "filename": str}` | DBHandler, filesystem |
| `/files` | GET | None | — | `[{file objects}]` | DBHandler |
| `/{file_id}/process` | POST | None | — | `{"status": "processing"}` | BackgroundTasks, DBHandler |
| `/{file_id}/stats` | GET | None | — | `{processing stats}` | DBHandler |
| `/{file_id}/download` | GET | None | — | `FileResponse (CSV)` | filesystem |
| `/{file_id}/archive` | POST | None | — | `{"status": "archived"}` | DBHandler |
| `/{file_id}/delete` | POST | None | — | `{"status": "deleted"}` | DBHandler, filesystem |

### 4.4 Manual Database Updates

| Endpoint | Method | Auth | Request Body | Response | Dependencies |
|----------|--------|------|-------------|----------|-------------|
| `/db-manual-update/import` | POST | None | `multipart/form-data (file, type)` | `{"import_id": int}` | DBHandler |
| `/db-manual-update/history` | GET | None | — | `[{import records}]` | DBHandler |
| `/db-manual-update/{import_id}/download` | GET | None | — | `FileResponse` | filesystem |

### 4.5 HLR Lookup

| Endpoint | Method | Auth | Request Body | Response | Dependencies |
|----------|--------|------|-------------|----------|-------------|
| `/{file_id}/hlr` | POST | None | — | `{"status": "started"}` | BackgroundTasks, DBHandler, HLR API |
| `/{file_id}/hlr` | GET | None | — | `{hlr status}` | DBHandler |
| `/{file_id}/hlr/download` | GET | None | — | `FileResponse (CSV)` | filesystem |
| `/{file_id}/hlr/raw` | GET | None | — | `FileResponse (CSV)` | filesystem |

### 4.6 Database Export

| Endpoint | Method | Auth | Request Body | Response | Dependencies |
|----------|--------|------|-------------|----------|-------------|
| `/process_db` | POST | None | `{"table_name": str}` | `{"status": "processing"}` | BackgroundTasks, DBHandler |
| `/fetch-latest-db-export` | POST | None | `{"table_name": str}` | `{export metadata}` | DBHandler |
| `/db-download` | GET | None | `?path=...` | `FileResponse` | filesystem |

### 4.7 MMD / Broadcasts

| Endpoint | Method | Auth | Request Body | Response | Dependencies |
|----------|--------|------|-------------|----------|-------------|
| `/get-live-broadcasts-stream` | GET | None | — | `SSE text/event-stream` | MMDDataHandler, DBHandler |
| `/save-data-for-previous-date` | GET | None | — | `{"status": "ok"}` | MMDDataHandler, DBHandler |
| `/save-data-for-specific-date` | GET | None | `?date=YYYY-MM-DD` | `{"status": "ok"}` | MMDDataHandler, DBHandler |
| `/update-costs-for-mmd` | GET | None | — | `{"status": "ok"}` | MMDDataHandler, VoluumAccessKeyHandler |
| `/files-broadcasts` | GET | None | — | `[{broadcast files}]` | DBHandler |
| `/{file_id}/download-broadcasts-history` | GET | None | — | `FileResponse` | filesystem |

### 4.8 Encryption

| Endpoint | Method | Auth | Request Body | Response | Dependencies |
|----------|--------|------|-------------|----------|-------------|
| `/upload-encrypted` | POST | None | `multipart/form-data` | `{"id": int}` | DBHandler, filesystem |
| `/encrypted-files` | GET | None | — | `[{file objects}]` | DBHandler |
| `/{file_id}/process-encrypted` | POST | None | — | `{"status": "processing"}` | BackgroundTasks, EncryptionHandler |
| `/{file_id}/download-encrypted` | GET | None | — | `FileResponse` | filesystem |
| `/{file_id}/archive-encrypted` | POST | None | — | `{"status": "archived"}` | DBHandler |
| `/{file_id}/delete-encrypted` | POST | None | — | `{"status": "deleted"}` | DBHandler, filesystem |

### 4.9 CSV Jobs (Ongage Sync)

| Endpoint | Method | Auth | Request Body | Response | Dependencies |
|----------|--------|------|-------------|----------|-------------|
| `/csv-jobs/upload` | POST | None | `multipart/form-data` | `{"job_id": int}` | DBHandler |
| `/csv-jobs` | GET | None | — | `[{job objects}]` | DBHandler |
| `/csv-jobs/{job_id}` | PATCH | None | `{config updates}` | `{"status": "updated"}` | DBHandler |
| `/csv-jobs/{job_id}/start` | POST | None | — | `{"status": "running"}` | BackgroundTasks, OngageDataHandler |
| `/csv-jobs/{job_id}/pause` | POST | None | — | `{"status": "paused"}` | DBHandler |
| `/csv-jobs/{job_id}/resume` | POST | None | — | `{"status": "running"}` | BackgroundTasks, OngageDataHandler |

### 4.10 Smart Cleaning (Voluum)

| Endpoint | Method | Auth | Request Body | Response | Dependencies |
|----------|--------|------|-------------|----------|-------------|
| `/find-smart-cleaning-voluum` | POST | None | `{"country": str}` | `{matching data}` | DBHandler |
| `/upload-smart-cleaning-voluum` | POST | None | `multipart/form-data` | `{"id": int}` | DBHandler |
| `/smart-cleaning-files-voluum` | GET | None | — | `[{file objects}]` | DBHandler |
| `/{file_id}/process-smart-cleaning-files-voluum` | POST | None | — | `{"status": "processing"}` | BackgroundTasks, DBHandler |
| `/{file_id}/archive-smart-cleaning-files-voluum` | POST | None | — | `{"status": "archived"}` | DBHandler |
| `/{file_id}/delete-smart-cleaning-files-voluum` | POST | None | — | `{"status": "deleted"}` | DBHandler |
| `/{file_id}/download-smart-cleaning-files-voluum` | GET | None | — | `FileResponse` | filesystem |

### 4.11 REG Search

| Endpoint | Method | Auth | Request Body | Response | Dependencies |
|----------|--------|------|-------------|----------|-------------|
| `/reg-search/search` | POST | None | `{"country": str, ...}` | `{"file_id": int}` | DBHandler |
| `/reg-search/files` | GET | None | — | `[{file objects}]` | DBHandler |
| `/reg-search/{file_id}/process` | POST | None | — | `{"status": "processing"}` | BackgroundTasks, DBHandler |
| `/reg-search/{file_id}/download` | GET | None | — | `FileResponse` | filesystem |
| `/reg-search/{file_id}/archive` | POST | None | — | `{"status": "archived"}` | DBHandler |
| `/reg-search/{file_id}/delete` | POST | None | — | `{"status": "deleted"}` | DBHandler |

### 4.12 Blacklist Import

| Endpoint | Method | Auth | Request Body | Response | Dependencies |
|----------|--------|------|-------------|----------|-------------|
| `/import-blacklist-csv` | POST | None | `multipart/form-data` | `{"status": "ok", "count": int}` | DBHandler |

> **NOTE:** This endpoint name says "blacklist" but the underlying `import_csv_into_blacklist()` method inserts into the `monitor` table. This is a confirmed bug.

---

## Phase 5 — Error & Improvement Matrix

### Severity Scale
- **P1 — Critical:** Security vulnerability or data loss risk. Must fix before production.
- **P2 — High:** Functional bug or reliability risk. Fix within sprint.
- **P3 — Medium:** Code quality, maintainability, or minor functional issue. Plan for next iteration.
- **P4 — Low:** Style, naming, or minor improvement. Address opportunistically.

### 5.1 Error & Improvement Matrix

| # | Location | Issue Type | Severity | Description | Fix Recommendation | Effort |
|---|----------|-----------|----------|-------------|-------------------|--------|
| 1 | `mmd_data_handler.py:50-51,104` | **Security** | **P1** | Hardcoded API key `fc416a66-fdcc-480b-a693-f85007723364` | Move ALL API key usage to `os.getenv("MMD_API_KEY")` | 30 min |
| 2 | `db_handler.py:358` | **Security** | **P1** | SQL injection via f-string interpolation | Use parameterized query: `cursor.execute("... WHERE custom_variable_1 = %s", (key,))` | 15 min |
| 3 | `router.py` (all endpoints) | **Security** | **P1** | Zero authentication/authorization on all 57 endpoints | Add API key middleware or OAuth2 | 4–8 hours |
| 4 | `sqs_handler.py:216` | **Architecture** | **P1** | `SQSHandler().run()` auto-executes at import — starts infinite loop | Move to explicit `if __name__ == "__main__"` or startup hook | 30 min |
| 5 | Project root | **Security** | **P1** | No `.env.example` file — credentials undocumented | Create `.env.example` with all 22 variables (empty values) | 30 min |
| 6 | `requirements.txt` | **Dependency** | **P2** | `boto3` missing — SQS handler will crash on import | Add `boto3` to requirements.txt | 5 min |
| 7 | `requirements.txt` | **Dependency** | **P2** | `flask` listed but never used | Remove `flask` from requirements.txt | 5 min |
| 8 | `router.py` | **Bug** | **P2** | Multiple function name collisions — `process_csv` defined 3x, `download_processed` 5x, etc. Python silently keeps only the last definition | Rename functions with unique prefixes (e.g., `process_csv_upload`, `process_csv_encryption`) or split into sub-routers | 2–4 hours |
| 9 | `db_handler.py:2176-2214` | **Bug** | **P2** | `import_csv_into_blacklist()` inserts into `monitor` table, not `blacklist` | Change table name to `blacklist` or rename method | 15 min |
| 10 | `main.py:17` vs `Dockerfile` | **Config** | **P2** | Port mismatch: main.py uses 8800, Dockerfile uses 8000 | Align both to 8000 (Docker CMD overrides anyway, but confusing) | 5 min |
| 11 | `db_handler.py` | **Architecture** | **P2** | No connection pooling — single persistent connection | Use `psycopg2.pool.ThreadedConnectionPool` or switch to async `asyncpg` | 4–8 hours |
| 12 | `mmd_data_handler.py:15-16` | **Architecture** | **P2** | Module-level API call — `VoluumAccessKeyHandler().get_access_token()` on import | Move to lazy initialization | 30 min |
| 13 | `sqs_handler.py:156-158` | **Bug** | **P2** | SQS message deletion commented out — messages reprocess infinitely | Uncomment deletion or add visibility timeout handling | 15 min |
| 14 | `router.py` (1,549 lines) | **Architecture** | **P3** | Monolithic single-file router with 57 endpoints | Split into domain routers: `voluum_router.py`, `csv_router.py`, `hlr_router.py`, etc. | 4–8 hours |
| 15 | `db_handler.py` (2,527 lines) | **Architecture** | **P3** | God object — 60+ methods in single class | Split into domain repositories: `VoluumRepository`, `CSVRepository`, `HLRRepository`, etc. | 1–2 days |
| 16 | `db_handler.py` | **Architecture** | **P3** | HLR API calls inside database handler — violates single responsibility | Extract to `HLRService` class | 2–4 hours |
| 17 | All files except sqs_handler.py | **Observability** | **P3** | `print()` statements instead of `logging` module | Replace all print() with structured logging | 2–4 hours |
| 18 | `schema.py` | **Validation** | **P3** | Minimal Pydantic schemas — no response models, most endpoints unvalidated | Add request/response schemas for all endpoints | 1–2 days |
| 19 | `voluum_data_handler.py:114` | **Config** | **P3** | Hardcoded traffic source IDs | Move to environment variable or database config | 30 min |
| 20 | `ongage_data_handler.py:31` | **Config** | **P3** | Hardcoded country filter `'GB'` only | Make configurable via env var or parameter | 15 min |
| 21 | `ongage_data_handler.py` | **Fragility** | **P3** | Positional array indices (`record[14]`, `record[39]`) | Use named columns or dict access | 1–2 hours |
| 22 | `crone_jobs/` | **Architecture** | **P3** | Cron jobs as raw threads calling back to own API via HTTP | Use APScheduler or AWS EventBridge + direct function calls | 4–8 hours |
| 23 | Project | **Testing** | **P3** | Zero test files — no unit tests, integration tests, or test infrastructure | Add pytest + test suite for critical paths | 2–4 days |
| 24 | `docker-compose.yml` | **Storage** | **P3** | Local Docker volume for file storage — not production-ready | Migrate to S3 for file persistence | 4–8 hours |
| 25 | All utility files | **Architecture** | **P3** | No dependency injection — global instances, tight coupling | Introduce FastAPI dependency injection via `Depends()` | 1–2 days |
| 26 | `voluum_access_key_handler.py` | **Reliability** | **P3** | Infinite retry loop with no max attempts | Add max retry count + exponential backoff | 30 min |
| 27 | `mmd_data_handler.py:404,407` | **Deprecation** | **P4** | `datetime.utcnow()` deprecated since Python 3.12 | Use `datetime.now(datetime.UTC)` | 10 min |
| 28 | `db_handler.py:465` | **Typo** | **P4** | `reason='INAVLID_PHONE_NUMBER'` | Fix to `'INVALID_PHONE_NUMBER'` | 5 min |
| 29 | `crone_jobs/` | **Naming** | **P4** | Directory named `crone_jobs` | Rename to `cron_jobs` | 5 min |
| 30 | `README.md` | **Documentation** | **P4** | Empty — only contains `# optimizer-backend` | Add setup instructions, API overview, architecture diagram | 2–4 hours |
| 31 | `db_handler.py:38-52` | **Reliability** | **P4** | Connection retry catches Exception but retries identically | Add exponential backoff and specific exception handling | 30 min |

### 5.2 Priority Summary

| Severity | Count | Action |
|----------|-------|--------|
| **P1 — Critical** | 5 | **Block deployment until fixed** |
| **P2 — High** | 8 | Fix within current sprint |
| **P3 — Medium** | 13 | Plan for next 2–3 sprints |
| **P4 — Low** | 5 | Address opportunistically |

---

## Appendix: Quick Reference

### File Size Summary

| File | Lines | Complexity |
|------|-------|-----------|
| `router.py` | 1,549 | Very High — needs splitting |
| `db_handler.py` | 2,527 | Very High — god object |
| `mmd_data_handler.py` | 489 | Medium |
| `voluum_data_handler.py` | 245 | Medium |
| `sqs_handler.py` | 216 | Medium |
| `crone_job_conversion_clickers.py` | 177 | Low |
| `update_costs.py` | 142 | Low |
| `ongage_data_handler.py` | 91 | Low |
| `voluum_access_key_handler.py` | 55 | Low |
| `encryption_handler.py` | 35 | Low |
| `main.py` | 17 | Low |
