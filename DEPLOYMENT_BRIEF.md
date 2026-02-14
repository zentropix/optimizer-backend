# Optimizer Backend — Deployment Brief

**Prepared for:** Head of Back-end
**Date:** 2026-02-13
**Reviewer:** Senior DevOps Engineer

---

## 1. Stack Overview

| Component | Technology | Version |
|-----------|-----------|---------|
| Runtime | Python | 3.11 |
| Framework | FastAPI + Uvicorn | Latest |
| Database | PostgreSQL | psycopg2-binary (raw SQL) |
| Container | Docker + docker-compose | 3.8 spec |
| Queue | AWS SQS | boto3 (missing from deps) |
| External APIs | Voluum, Ongage, MessageWhiz, HLR Lookup | REST |
| Encryption | AES-256-GCM | cryptography lib |
| Cron | Python threads | Custom (no scheduler framework) |
| **Endpoints** | **57 total** | **Zero authentication** |
| **DB Tables** | **22 total** | **No migrations** |

---

## 2. Deployment Checklist

### Must-Fix Before Deployment (P1 Blockers)

- [ ] **Remove hardcoded API key** from `mmd_data_handler.py` lines 50–51 and 104 — replace with `os.getenv("MMD_API_KEY")`
- [ ] **Fix SQL injection** in `db_handler.py` line 358 — use parameterized queries
- [ ] **Add authentication** to all 57 API endpoints — at minimum, API key middleware
- [ ] **Fix SQS auto-execute** in `sqs_handler.py` line 216 — remove `SQSHandler().run()` from module level
- [ ] **Create `.env.example`** with all 22 environment variables documented
- [ ] **Add `boto3`** to `requirements.txt` (SQS handler dependency)
- [ ] **Remove `flask`** from `requirements.txt` (unused)

### Should-Fix Before Deployment (P2 High)

- [ ] Fix function name collisions in `router.py` (5+ overwritten functions)
- [ ] Fix `import_csv_into_blacklist()` — currently inserts into `monitor` table
- [ ] Align port configuration (main.py:8800 vs Dockerfile:8000)
- [ ] Add database connection pooling
- [ ] Uncomment SQS message deletion (lines 156–158)
- [ ] Fix module-level API call in `mmd_data_handler.py`

### Infrastructure Setup

- [ ] Provision RDS PostgreSQL (db.t3.medium, private subnet)
- [ ] Create ECR repository, push Docker image
- [ ] Create ECS Fargate service (or EC2)
- [ ] Configure ALB with HTTPS (ACM certificate)
- [ ] Set up security groups: ALB→ECS:8000, ECS→RDS:5432
- [ ] Store all secrets in AWS Secrets Manager / SSM Parameter Store
- [ ] Migrate file storage from local volumes to S3
- [ ] Configure CloudWatch log groups
- [ ] Set up ECS Scheduled Tasks for cron jobs (replace thread-based cron)
- [ ] Increase ALB idle timeout for SSE endpoint (`/get-live-broadcasts-stream`)

---

## 3. Environment Variables

**22 variables required — no `.env.example` exists in the project.**

| Variable | Service | Sensitivity |
|----------|---------|-------------|
| `DB_HOST` | PostgreSQL | Medium — use RDS endpoint |
| `DB_NAME` | PostgreSQL | Low |
| `DB_USER` | PostgreSQL | Medium |
| `DB_PASSWORD` | PostgreSQL | **HIGH — Secrets Manager** |
| `VOLUUM_ACCESS_ID` | Voluum API | **HIGH — Secrets Manager** |
| `VOLUUM_ACCESS_KEY` | Voluum API | **HIGH — Secrets Manager** |
| `LIST_ID` | Ongage | Medium |
| `X_USERNAME` | Ongage | **HIGH — Secrets Manager** |
| `X_PASSWORD` | Ongage | **HIGH — Secrets Manager** |
| `X_ACCOUNT_CODE` | Ongage | Medium |
| `MMD_API_KEY` | MessageWhiz | **HIGH — Secrets Manager** (currently partially hardcoded!) |
| `HLRLOOKUP_APIKEY` | HLR Lookup | **HIGH — Secrets Manager** |
| `HLRLOOKUP_SECRET` | HLR Lookup | **HIGH — Secrets Manager** |
| `AWS_ACCESS_KEY_ID` | SQS | **HIGH — use IAM Task Role instead** |
| `AWS_SECRET_ACCESS_KEY` | SQS | **HIGH — use IAM Task Role instead** |
| `AWS_REGION` | SQS | Low |
| `SQS_QUEUE_URL` | SQS | Low |
| `AES_KEY` | Encryption | **HIGH — Secrets Manager** |
| `UPLOAD_ROOT` | File storage | Low — default: `uploads` |
| `DB_ROOT` | File storage | Low — default: `db_files` |
| `STORAGE_DIR` | Docker volume | Low — set in compose |
| `BACKEND_BASE_URL` | Cron jobs | Low — set to ALB URL |

> **Recommendation:** 10 of 22 variables should be stored in AWS Secrets Manager. AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) should be replaced with an IAM Task Role attached to the ECS service.

---

## 4. Critical Path

```
Step 1: Fix P1 security issues (hardcoded key, SQL injection, auth)
   ↓
Step 2: Fix P1 architecture issues (SQS auto-execute, missing deps)
   ↓
Step 3: Create .env.example + document all 22 variables
   ↓
Step 4: Provision AWS infrastructure (RDS, ECS, ALB, ECR, S3)
   ↓
Step 5: Migrate file storage to S3 (replace Docker volumes)
   ↓
Step 6: Deploy to ECS Fargate behind ALB
   ↓
Step 7: Move cron jobs to ECS Scheduled Tasks
   ↓
Step 8: Configure monitoring (CloudWatch alerts, health checks)
   ↓
Step 9: Production traffic cutover
```

**Estimated Timeline:** 2–3 weeks (with one developer fixing code, one DevOps on infra)

---

## 5. Known Risks

| Risk | Impact | Mitigation |
|------|--------|-----------|
| **No authentication on 57 endpoints** | Anyone can read/write/delete all data | Add API key auth before exposing to internet |
| **Hardcoded API key in source code** | Credential exposure if repo leaks | Remove from code, rotate the compromised key |
| **SQL injection vulnerability** | Full database compromise possible | Fix parameterized query immediately |
| **No database migrations** | Schema changes require manual SQL | Adopt Alembic for migration management |
| **No tests** | Every change risks regressions | Add pytest coverage for critical paths |
| **Single DB connection (no pooling)** | Connection drops under load | Implement connection pooling |
| **Local file storage** | Data loss if container restarts | Migrate to S3 before production |
| **Function name collisions in router** | Some endpoints silently broken (overwritten by later definitions) | Rename or split into sub-routers |
| **SQS messages never deleted** | Infinite reprocessing, cost escalation | Uncomment deletion code |
| **SSE endpoint + ALB** | ALB default 60s timeout kills stream | Increase ALB idle timeout to 3600s |

---

## 6. Quick Wins (< 1 Hour Each)

| # | Fix | Time | Impact |
|---|-----|------|--------|
| 1 | Move hardcoded MMD API key to env var | 30 min | Eliminates P1 security risk |
| 2 | Fix SQL injection with parameterized query | 15 min | Eliminates P1 security risk |
| 3 | Add `boto3`, remove `flask` in requirements.txt | 5 min | Fixes broken import + clean deps |
| 4 | Remove `SQSHandler().run()` from module level | 30 min | Eliminates import side-effect |
| 5 | Fix port mismatch in main.py (8800 → 8000) | 5 min | Eliminates developer confusion |
| 6 | Fix `import_csv_into_blacklist` table name | 15 min | Fixes data going to wrong table |
| 7 | Create `.env.example` | 30 min | Enables developer onboarding |
| 8 | Fix typo `INAVLID_PHONE_NUMBER` | 5 min | Clean data quality |
| 9 | Uncomment SQS message deletion | 5 min | Stop infinite reprocessing |

> **Total Quick Wins time: ~2.5 hours for 9 fixes covering 3 P1 and 4 P2 issues.**

---

## Summary Verdict

**This project is NOT ready for production deployment in its current state.** The 5 P1 (Critical) issues — especially the lack of authentication, SQL injection, and hardcoded credentials — must be resolved before any internet-facing deployment. The recommended approach is:

1. **Week 1:** Fix all P1 + P2 issues (developer), provision AWS infrastructure (DevOps)
2. **Week 2:** Migrate storage to S3, deploy to staging, validate endpoints
3. **Week 3:** Add basic auth + monitoring, production cutover

The codebase is functional but was built with a "make it work" mindset. Before scaling, it needs security hardening, architectural decomposition (router.py + db_handler.py), and basic observability (logging + monitoring).
