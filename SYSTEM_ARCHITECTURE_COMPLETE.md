# SYSTEM ARCHITECTURE — COMPLETE GUIDE
### Mutual Fund Portfolio Analytics Platform
> Written for non-technical founders. Updated: March 2026.

---

## Table of Contents
1. [Project Overview](#1-project-overview)
2. [Full Technical Architecture](#2-full-technical-architecture)
3. [Complete Pipeline Flow](#3-complete-pipeline-flow)
4. [Folder Structure](#4-folder-structure)
5. [Environment Setup](#5-environment-setup)
6. [How To Run Everything](#6-how-to-run-everything)
7. [Database Design](#7-database-design)
8. [Error Handling & Failure Scenarios](#8-error-handling--failure-scenarios)
9. [Security Model](#9-security-model)
10. [Maintenance Guide](#10-maintenance-guide)

---

## 1. Project Overview

### What This System Does

This is a **private data analytics platform** for Mutual Funds. It automatically collects, processes, and displays portfolio holdings data from 40+ major Indian Asset Management Companies (AMCs) — think HDFC, ICICI, SBI, Nippon, ABSL, etc.

Every month, these AMCs publish their portfolio disclosures (what stocks they hold). This system:
- **Downloads** those Excel/PDF disclosures automatically
- **Extracts** the equity holdings data (which stocks, how much quantity, at what value)
- **Stores** everything in a structured database
- **Serves** that data through a web application your team can use

### Who Uses It

- **5–10 internal team members** via a private website
- No public access; this is an internal research/analytics tool
- Users browse the website to see stock-level holdings, scheme comparisons, ownership data, NAV trends, etc.

### What Problem It Solves

Manually checking 40+ AMC websites every month, downloading files, and trying to analyze who holds what stock is **extremely time-consuming**. This system eliminates that manual work entirely. The whole download-to-dashboard pipeline runs automatically on a schedule.

### High-Level Flow (Simple Version)

```
AMC Website (40+ sources)
        │
        ▼
  Downloader Scripts
  (download Excel/PDF files)
        │
        ▼
  Raw Files on Disk
  (data/raw/ folder)
        │
        ▼
  Extractor Scripts
  (read Excel → parse equity data)
        │
        ▼
  PostgreSQL Database
  (stores clean, structured data)
        │
        ▼
  FastAPI Backend
  (serves data as JSON via /api/...)
        │
        ▼
  React Frontend
  (website your team uses)
        │
        ▼
  Team Member's Browser
  (Chrome / Safari / Firefox)
```

---

## 2. Full Technical Architecture

### Components at a Glance

| Component | Technology | What It Does |
|-----------|-----------|--------------|
| Backend API | FastAPI (Python) | Serves data as JSON to the frontend |
| Frontend | React + Vite | The website users see in their browser |
| Database | PostgreSQL | Stores all mutual fund data |
| Downloaders | Python + Playwright | Automated browser to download AMC files |
| Extractors | Python + Pandas | Parse Excel files, extract equity data |
| Scheduler | APScheduler (Python) | Runs jobs automatically on a timetable |
| Alerts | Telegram Bot | Notifies team when jobs succeed/fail |
| NAV Ingestion | AMFI API | Pulls daily NAV (Net Asset Value) data |
| **Admin File Manager** | **FastAPI + React** | **View & delete raw/merged files from the UI to free up disk space (manual, admin-only)** |

---

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                        TEAM MEMBER                         │
│                   (Browser: Chrome/Firefox)                 │
└──────────────────────────┬──────────────────────────────────┘
                           │ HTTPS request
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    YOUR DOMAIN (e.g. app.yourdomain.com)    │
│                    Nginx (reverse proxy, optional)          │
└──────────────────────────┬──────────────────────────────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
              ▼                         ▼
┌─────────────────────┐    ┌──────────────────────┐
│  React Frontend     │    │  FastAPI Backend      │
│  (Vite dev server   │    │  port 8000            │
│   or static build)  │    │  run_api.py           │
└─────────────────────┘    └──────────┬───────────┘
                                      │ SQL queries
                                      ▼
                           ┌──────────────────────┐
                           │  PostgreSQL Database  │
                           │  port 5432            │
                           │  DB: mf_analytics      │
                           └──────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                   BACKGROUND PROCESSES                       │
│                                                             │
│  APScheduler ──► AMC Downloader Scripts (40+ AMCs)         │
│       │               │ downloads Excel/PDF                 │
│       │               ▼                                     │
│       │         data/raw/<amc>/<year>/<file.xlsx>           │
│       │               │                                     │
│       │         Extractor Scripts                           │
│       │               │ parses data                         │
│       │               ▼                                     │
│       │         data/output/merged excels/<amc>/...         │
│       │               │                                     │
│       │         Portfolio Loader                            │
│       │               │ INSERT into DB                      │
│       │               ▼                                     │
│       │         PostgreSQL Database                         │
│       │                                                     │
│       └──────────────────────► Telegram Bot (alerts)        │
└─────────────────────────────────────────────────────────────┘
```

### Backend (FastAPI)
- **Location**: `src/api/`
- **Entry point**: `run_api.py`
- **Port**: 8000
- **What it does**: Provides REST API endpoints that the frontend calls to get data. For example, `/api/schemes`, `/api/holdings`, `/api/stocks`
- **Routers** (categories of APIs): Located in `src/api/routers/`

### Frontend (React + Vite)
- **Location**: `frontend/`
- **Framework**: React (JavaScript)
- **Builder**: Vite (fast development server)
- **Port (dev)**: 5173
- **What it does**: The actual website. Makes API calls to the backend and renders tables, charts, and dashboards.

### Database (PostgreSQL)
- **Port**: 5432
- **Database name**: `mf_analytics`
- **User**: `mf_admin`
- **What it stores**: AMC records, fund schemes, monthly snapshots, stock holdings, ISIN master, NAV history

### Scheduler (APScheduler)
- **Location**: `src/scheduler/`
- **What it does**: Runs download + extraction jobs automatically on a monthly schedule (once per month per AMC, around the 10th–15th when AMCs publish disclosures)
- **One file per AMC**: e.g. `hdfc_scheduler.py`, `sbi_scheduler.py`, etc.

### Downloaders (Playwright)
- **Location**: `src/downloaders/`
- **What they do**: Act like a robot browser — visit AMC websites, click buttons, and download Excel/PDF files to disk
- **Technology**: Playwright (Python) — a browser automation library
- **One file per AMC**: e.g. `hdfc_downloader.py`, `icici_downloader.py`

### Extractors (Pandas)
- **Location**: `src/extractors/`
- **What they do**: Read the downloaded Excel files, identify the equity holdings table, parse ISIN codes, company names, quantities, and market values
- **Entry point**: `src/extractors/orchestrator.py`
- **Output**: Clean data rows that go into the database

### Alerts (Telegram)
- **Location**: `src/config/telegram_config.py`
- **What it does**: Sends Telegram messages to your team channel when a download job finishes, fails, or has warnings

---

## 3. Complete Pipeline Flow

The entire data journey has **6 stages**:

---

### Stage 1 — Trigger (Scheduler)

**What runs**: APScheduler job for a specific AMC+month  
**File**: `src/scheduler/<amc>_scheduler.py`  
**What happens**: At a scheduled time, the scheduler calls the downloader for that AMC  
**Command to test manually**:
```bash
.venv/Scripts/python src/scheduler/hdfc_scheduler.py
```
**What can break**: Job doesn't trigger (scheduler not running), wrong schedule time  
**How to test**: Check logs in `logs/` folder

---

### Stage 2 — Download (Downloader)

**What runs**: AMC-specific downloader  
**File**: `src/downloaders/<amc>_downloader.py`  
**What happens**: A headless browser visits the AMC website, locates the portfolio disclosure file for the target month, and downloads it to disk  
**Files touched**: `data/raw/<amc>/<year>/<month>/<file.xlsx>`  
**Command to test manually**:
```bash
.venv/Scripts/python -c "from src.downloaders.hdfc_downloader import HDFCDownloader; d=HDFCDownloader(); d.download(2025, 11)"
```
**What can break**:
- AMC website changes its layout → downloader breaks
- AMC hasn't published the file yet → download returns empty
- Internet is down → connection timeout
- Playwright browser not installed → crash

**How to test**: Check if file appears in `data/raw/` folder

---

### Stage 3 — Merge (Excel Consolidation)

**What runs**: Merge script that consolidates raw downloads into a single structured Excel  
**Output**: `data/output/merged excels/<amc>/<year>/CONSOLIDATED_<AMC>_<YYYY>_<MM>.xlsx`  
**What can break**: Mismatched sheet names, corrupt download file  
**How to test**: Open the merged Excel manually and verify the data looks correct

---

### Stage 4 — Extraction (Extractor)

**What runs**: `src/extractors/orchestrator.py` → selects appropriate extractor via `ExtractorFactory`  
**What happens**:
1. Opens the merged Excel
2. Scans each sheet for the portfolio data table
3. Identifies the header row (must contain `ISIN` + one of `INSTRUMENT`, `COMPANY`, `ISSUER`, etc.)
4. For each row, checks if ISIN is a valid equity (starts with `INE`, 12 chars, positions 8–10 are `10`)
5. Extracts: company name, ISIN, quantity, market value, % to NAV, sector
6. Normalizes rupee values (detects if values are in Lakhs/Crores and converts to base INR)

**Command (dry-run — no DB write)**:
```bash
.venv/Scripts/python -c "from src.extractors.orchestrator import ExtractionOrchestrator; o=ExtractionOrchestrator(); print(o.process_amc_month('hdfc',2025,11,dry_run=True,redo=True))"
```
**What can break**:
- Header row not recognized → `rows_read = 0`
- ISIN format different from expected → all rows filtered out
- Values in unexpected units

**How to test**: Run dry-run and check `rows_read` is reasonable (> 0)

---

### Stage 5 — Database Load (Portfolio Loader)

**What runs**: `src/loaders/portfolio_loader.py`  
**What happens**:
1. Groups rows by scheme (fund name + plan type + option type)
2. Upserts scheme record
3. Creates a "snapshot" record for this scheme+month
4. Inserts all holding rows
5. Handles duplicates (merges split ISINs, drops exact duplicates)

**Command (actual DB write)**:
```bash
.venv/Scripts/python -c "from src.extractors.orchestrator import ExtractionOrchestrator; o=ExtractionOrchestrator(); print(o.process_amc_month('hdfc',2025,11,dry_run=False,redo=False))"
```
**What can break**:
- DB not running → connection refused
- Duplicate snapshot → silently skipped (use `redo=True` to force)
- Wrong DB credentials → authentication error

**How to test**: Query `SELECT COUNT(*) FROM holdings WHERE ...` after loading

---

### Stage 6 — API + Frontend Display

**What runs**: FastAPI backend + React frontend  
**What happens**:
1. User opens the website in browser
2. React makes API calls like `GET /api/schemes?amc=hdfc`
3. FastAPI queries PostgreSQL and returns JSON
4. React renders the data as tables, charts, dashboards

**What can break**:
- Backend not running → frontend shows "API Error"
- CORS not configured → browser blocks request
- Frontend build not updated → old UI/data

**How to test**: Open browser, check all pages load, open browser DevTools → Network tab to see API responses

---

### Stage 7 — Admin File Cleanup (Manual, On-Demand)

**Who triggers this**: Admin user manually from the Admin Panel  
**Files involved**:
- `src/services/admin_file_service.py` — backend service
- `src/api/routers/admin.py` — API endpoints (`/api/admin/files/*`)
- `frontend/src/pages/admin/FileManagement.jsx` — admin UI

**What happens**:
1. Admin opens Admin Panel → **File Management**
2. UI shows a table of all AMCs × months with: Raw ✅/❌, Merged ✅/❌, DB Loaded 📥
3. Admin clicks **Delete Raw** or **Delete Merged** for any specific row
4. A **Bulk Wipe** option wipes an entire month across all AMCs at once
5. Every deletion is logged to `notification_logs` for audit

**Purpose**: After data is loaded into PostgreSQL, the original Excel files are no longer needed for the live dashboard. Deleting them frees disk space so the 20 GB server doesn't fill up.

**When to use**:
- After each successful monthly extraction + DB load (routine cleanup)
- When disk usage hits 70% — run a cleanup pass
- To reset a failed month — delete raw and re-trigger the downloader

> ⚠️ **Golden Rule**: Always check the **DB Loaded** column is ✅ *before* deleting any files. If DB is not loaded yet, deleting files means you'll need to re-download from the AMC website.

**What can break**: Path validation blocks any attempt to delete outside `data/` — this is a safety guard

**How to test**: Open Admin Panel → File Management → verify table matches actual `data/` folder contents

---

## 4. Folder Structure

```
NEW MUTUAL FUND MF AUTOMATION/
│
├── src/                          ← All Python application code
│   ├── api/                      ← FastAPI backend
│   │   ├── main.py               ← App entry point, CORS config
│   │   ├── routers/              ← API route handlers (one file per feature)
│   │   ├── models/               ← Pydantic data models (request/response shapes)
│   │   └── dependencies.py       ← Shared DB connection etc.
│   │
│   ├── downloaders/              ← One downloader per AMC (40+ files)
│   │   ├── base_downloader.py    ← Shared downloader logic
│   │   ├── hdfc_downloader.py    ← HDFC-specific download logic
│   │   └── ...                   ← (one file per AMC)
│   │
│   ├── extractors/               ← Excel parsing logic
│   │   ├── orchestrator.py       ← Main controller for extraction
│   │   ├── extractor_factory.py  ← Maps AMC slug → correct extractor
│   │   ├── base_extractor.py     ← Shared extraction utilities
│   │   ├── common_extractor_v1.py← Default extractor for most AMCs
│   │   └── hdfc_extractor.py     ← AMC-specific if different format
│   │
│   ├── loaders/                  ← Database loading logic
│   │   └── portfolio_loader.py   ← Writes extracted data to PostgreSQL
│   │
│   ├── services/                 ← Shared backend services
│   │   └── admin_file_service.py ← Admin file scanner & secure deletion logic [NEW]
│   │
│   ├── scheduler/                ← Scheduled job definitions
│   │   ├── hdfc_scheduler.py     ← HDFC monthly job
│   │   └── ...                   ← (one file per AMC)
│   │
│   ├── ingestion/                ← External data ingestion (NAV etc.)
│   │   └── amfi_nav_downloader.py← Pulls daily NAV from AMFI website
│   │
│   ├── config/                   ← Configuration & secrets loading
│   │   ├── settings.py           ← Loads .env file
│   │   └── telegram_config.py    ← Telegram bot config
│   │
│   ├── db/                       ← Database connection utilities
│   ├── alerts/                   ← Telegram notification logic
│   ├── utils/                    ← Shared utility functions
│   ├── scripts/                  ← One-off admin scripts
│   ├── validators/               ← Data validation helpers
│   └── tests/                    ← Automated test files
│
├── frontend/                     ← React web application
│   ├── src/                      ← React components and pages
│   ├── public/                   ← Static assets (favicon, images)
│   ├── index.html                ← HTML shell
│   ├── package.json              ← Node.js dependencies list
│   └── vite.config.js            ← Vite build configuration
│
├── database/                     ← Database schema and migrations
│   ├── schema_v1.0.sql           ← Full initial DB schema
│   └── migrations/               ← Incremental schema changes (001, 002...)
│
├── data/                         ← All data files (not in Git)
│   ├── raw/                      ← Downloaded Excel/PDF files from AMCs
│   │   └── <amc>/<year>/<month>/ ← Organized by AMC, year, month
│   │                               ⚠️ Safe to delete after DB load via Admin Panel
│   └── output/
│       └── merged excels/        ← Consolidated Excel files per AMC+month
│           └── <amc>/<year>/     ← CONSOLIDATED_<AMC>_<YYYY>_<MM>.xlsx
│                                   ⚠️ Safe to delete after DB load via Admin Panel
│
├── logs/                         ← Application log files
│   └── (auto-generated)
│
├── backups/                      ← Database backup files
│
├── run_api.py                    ← Start the backend server
├── requirements.txt              ← Python library dependencies
├── .env                          ← Your secrets (never commit to Git!)
├── .env.example                  ← Template showing required variables
└── .gitignore                    ← Files Git should ignore
```

---

## 5. Environment Setup

### Python Version
This project requires **Python 3.10 or higher**.

Check your version:
```bash
python --version
```

### Step-by-Step Setup (Fresh Machine)

**Step 1: Clone the repository**
```bash
git clone <your-repo-url>
cd "NEW MUTUAL FUND MF AUTOMATION"
```

**Step 2: Create virtual environment**
```bash
# Windows
python -m venv .venv

# Activate it (Windows PowerShell)
.venv\Scripts\Activate.ps1

# Activate it (Linux/Mac)
source .venv/bin/activate
```

> ⚡ You'll see `(.venv)` at the start of your terminal prompt when active. Always activate before running anything.

**Step 3: Install Python libraries**
```bash
pip install -r requirements.txt
```

**Step 4: Install Playwright browsers** (needed for downloaders)
```bash
playwright install chromium
```

**Step 5: Install Node.js** (for frontend only)
- Download from: https://nodejs.org (LTS version)

**Step 6: Install frontend dependencies**
```bash
cd frontend
npm install
cd ..
```

**Step 7: Set up PostgreSQL**
- Install PostgreSQL from https://www.postgresql.org/download/
- Create database:
```sql
CREATE DATABASE mf_analytics;
CREATE USER mf_admin WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE mf_analytics TO mf_admin;
```

**Step 8: Run database schema**
```bash
psql -U mf_admin -d mf_analytics -f database/schema_v1.0.sql
```

**Step 9: Create your `.env` file**
```bash
# Copy the template
copy .env.example .env
# Then edit .env with your actual values
```

### `.env` File Configuration

```env
# Environment
ENVIRONMENT=dev

# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mf_analytics
DB_USER=mf_admin
DB_PASSWORD=your_secure_password_here

# API
API_HOST=0.0.0.0
API_PORT=8000

# Frontend CORS (comma-separated origins)
CORS_ORIGINS=http://localhost:3000,http://localhost:5173

# Telegram Bot (optional, for alerts)
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id

# AngelOne (for live stock data, optional)
ANGEL_API_KEY=your_key
ANGEL_CLIENT_CODE=your_code
ANGEL_PASSWORD=your_4_digit_pin
ANGEL_TOTP_SECRET=your_totp_secret
```

### How Secrets Are Managed
- All secrets live in `.env` file
- `.env` is in `.gitignore` — it **never** gets pushed to GitHub
- The `.env.example` file shows what variables are needed (with fake values)
- On the server: upload `.env` manually via SSH, never through Git

---

## 6. How To Run Everything

### Start the Backend (FastAPI)

```bash
# Make sure .venv is activated
.venv\Scripts\Activate.ps1

# Start the backend
python run_api.py
```

What this does: Starts the API server on port 8000. You can visit `http://localhost:8000/docs` to see all API endpoints in a nice interface.

To test: Open browser → `http://localhost:8000/docs`

---

### Start the Frontend (React)

```bash
cd frontend
npm run dev
```

What this does: Starts the React development server on port 5173. Open your browser to see the website.

To test: Open browser → `http://localhost:5173`

---

### Run a Download (Manual)

```bash
# Example: Download HDFC's November 2025 portfolio
.venv\Scripts\python -c "from src.downloaders.hdfc_downloader import HDFCDownloader; d=HDFCDownloader(); d.download(2025, 11)"
```

What this does: Opens a headless browser, visits HDFC's website, downloads the monthly portfolio Excel.

---

### Run Extraction (Dry Run — No DB Write)

```bash
.venv\Scripts\python -c "
from src.extractors.orchestrator import ExtractionOrchestrator
o = ExtractionOrchestrator()
print(o.process_amc_month('hdfc', 2025, 11, dry_run=True, redo=True))
"
```

What this does: Tests the extraction without touching the database. Shows you how many rows would be loaded.

---

### Run Extraction (Load Mode — Writes to DB)

```bash
.venv\Scripts\python -c "
from src.extractors.orchestrator import ExtractionOrchestrator
o = ExtractionOrchestrator()
print(o.process_amc_month('hdfc', 2025, 11, dry_run=False, redo=False))
"
```

What this does: Does the actual database load. Only run after dry-run looks good.

---

### Run All Backfills (Bulk Historical Load)

```bash
# Windows PowerShell script
.\run_all_backfills.ps1
```

What this does: Processes multiple AMCs and months in batch. Used to initially populate the database.

---

### Check the Database

```bash
psql -U mf_admin -d mf_analytics

# Once inside, useful queries:
SELECT COUNT(*) FROM holdings;
SELECT DISTINCT amc_id FROM snapshots;
\q   # to exit
```

---

## 7. Database Design

### Where the Database Lives
- **Development (your laptop)**: PostgreSQL running locally at `localhost:5432`
- **Production (AWS)**: PostgreSQL running on the same EC2 server (or optionally on RDS)

### Database Name: `mf_analytics`

### Key Tables

| Table | What It Stores |
|-------|---------------|
| `amcs` | List of AMC companies (HDFC, SBI, ICICI, etc.) |
| `schemes` | Individual mutual fund schemes |
| `periods` | Month-year periods (e.g., Nov-2025) |
| `snapshots` | One record per scheme per month (summary stats) |
| `holdings` | Individual stock holdings (ISIN, quantity, value) |
| `companies` | Company master (name, ISIN, sector) |
| `isin_master` | ISIN → company mapping |
| `nav_history` | Daily NAV values per scheme |

### How It's Structured (Simplified)

```
amcs (HDFC, SBI...)
  └──► schemes (HDFC Flexi Cap - Direct Growth)
          └──► snapshots (scheme × month)
                   └──► holdings (ISIN × quantity × value per snapshot)
```

### Backup Strategy

**Daily automated backup (when on Linux server)**:
```bash
# Run this script daily via cron
pg_dump -U mf_admin mf_analytics > /backups/mf_analytics_$(date +%Y%m%d).sql
```

**Backup location**: `backups/` folder on the server  
**Also copy to**: An S3 bucket or external drive monthly  
**How to restore**:
```bash
psql -U mf_admin -d mf_analytics < /backups/mf_analytics_20251101.sql
```

### Database Size Expectations
- ~40 AMCs × ~50 schemes × 12 months × ~60 holdings = ~1.4 million holding rows per year
- Expected storage: 2–5 GB for 3 years of historical data
- A standard t3.micro with 20 GB EBS can handle this comfortably

### What Happens If DB Crashes
1. Backend API returns errors (frontend shows "loading failed")
2. Downloaders and extractors still run (they write to disk, not DB)
3. Restart PostgreSQL: `sudo systemctl restart postgresql`
4. Data is recoverable from the raw Excel files in `data/raw/`

### Future Scaling Path
- Right now: PostgreSQL on same server (cheap, simple, fine for this size)
- When you hit 50+ concurrent users or 50GB+ data: move to AWS RDS
- Estimated migration cost: +₹500–₹1500/month for RDS

---

## 8. Error Handling & Failure Scenarios

| Scenario | What Happens | Risk | Fix | Prevention |
|----------|-------------|------|-----|------------|
| **Server crash / reboot** | Website goes offline, all services stop | High | SSH in, restart services via systemd: `sudo systemctl restart mf-api` | Use systemd so services auto-restart on boot |
| **Database corruption** | API returns errors, data unreadable | High | Restore from latest backup | Run daily backups; never force-kill postgres |
| **Disk full** | Downloads fail, logs can't write, DB writes fail | High | Go to Admin Panel → File Management → delete old raw/merged files; or expand disk | Run monthly file cleanup after DB load |
| **Admin deletes files before DB load** | Source data lost, must re-download from AMC | High | Re-trigger the downloader for that AMC+month | Always verify "DB Loaded" ✅ indicator before deleting |
| **AMC website changed layout** | Downloader fails, no file downloaded | Medium | Update the specific `<amc>_downloader.py` | Monitor Telegram alerts for download failures |
| **Scheduler crash** | Monthly jobs don't run automatically | Medium | Restart scheduler service; run missed jobs manually | Use systemd for auto-restart |
| **Internet failure on server** | Downloads fail (can't reach AMC websites) | Medium | Wait for internet; re-run missed downloads | AWS VPC is very stable; rare issue |
| **API server crash** | Frontend shows blank/error | Medium | Restart: `sudo systemctl restart mf-api` | Systemd + Telegram alert on crash |
| **Wrong extraction (0 rows)** | Nothing loaded to DB | Low | Run dry-run to debug; fix extractor | Always dry-run before load mode |
| **Duplicate snapshot** | Data not re-loaded (silently skipped) | Low | Use `redo=True` to force reload | Expected behavior; not a bug |
| **Telegram bot stopped** | No alerts sent | Low | Restart Telegram service; check bot token | Set up backup email alert |
| **AWS billing issue** | EC2 stopped by AWS | Very High | Pay the bill; restart instance | Set billing alerts in AWS console |

---

## 9. Security Model

### Is It Public?
**Yes — the website is publicly accessible on the internet**, meaning anyone with the URL can reach the login page. However, only your 5–10 authorized team members can log in and use it. The data behind the login is not exposed to the public.

### Who Can Access?
- The **login page** is visible to anyone with the URL
- The **actual data, dashboards, and API** are only accessible after logging in with valid credentials
- Your 5–10 team members each have their own username + password

### Authentication (What You Need)

Since the site is on the public internet, **you must have a login system** — relying on obscurity is not safe.

**Recommended approach**: HTTP Basic Auth via Nginx (simplest option, no code changes needed)

Add this to your Nginx config block:
```nginx
location / {
    auth_basic "Team Access";
    auth_basic_user_file /etc/nginx/.htpasswd;
    ...
}
```

Create users:
```bash
# Install tool
sudo apt install apache2-utils

# Add first user (will prompt for password)
sudo htpasswd -c /etc/nginx/.htpasswd aditya

# Add more users
sudo htpasswd /etc/nginx/.htpasswd teammate2
```

This gives every team member a browser username/password prompt when they visit the site.

**Alternative (more polished)**: Add JWT login to FastAPI — a proper login page with token-based sessions. This requires a `users` table and uses `python-jose` + `passlib` libraries. Estimated effort: 1–2 days of development.

### Protecting the API Directly

Even with Nginx auth, add API key or CORS restriction to FastAPI:
```python
# In src/api/main.py — already configured
CORS_ORIGINS=https://app.yourdomain.com   # Only your domain can call the API
```
This ensures the API cannot be called directly from random websites.

### Secrets Management
- All passwords and API keys live in `.env` file on the server
- `.env` is **never** committed to Git
- Rotate passwords every 6 months
- DB user `mf_admin` should only have access to `mf_analytics` database
- Never share `.env` over email/WhatsApp — use a password manager

---

## 10. Maintenance Guide

### How to Restart the Backend

```bash
# If running as systemd service:
sudo systemctl restart mf-api

# If running manually (development):
# Kill existing process, then:
python run_api.py
```

### How to Restart the Frontend

```bash
# Development server:
cd frontend && npm run dev

# Production (static files served by Nginx):
# Re-deploy only if you changed frontend code
cd frontend && npm run build
# Then copy dist/ to your Nginx web root
```

### How to Monitor Logs

```bash
# Backend logs (systemd)
sudo journalctl -u mf-api -f

# Application logs
tail -f logs/app.log

# PostgreSQL logs
sudo journalctl -u postgresql -f

# Scheduler logs
tail -f logs/scheduler.log
```

### How to Check System Health

```bash
# Is the API responding?
curl http://localhost:8000/docs

# Is the database running?
sudo systemctl status postgresql

# Disk usage?
df -h

# Memory usage?
free -h

# CPU usage?
top
```

### How to Update Code

```bash
# Pull latest code from Git
git pull origin main

# If Python dependencies changed:
pip install -r requirements.txt

# If DB schema changed (new migration):
psql -U mf_admin -d mf_analytics -f database/migrations/009_new_migration.sql

# If frontend UI changed:
cd frontend && npm install && npm run build

# Restart backend:
sudo systemctl restart mf-api
```

### How to Upgrade Libraries Safely

```bash
# Check what's outdated
pip list --outdated

# Upgrade one library at a time (not all at once)
pip install --upgrade fastapi

# Test after each upgrade:
python run_api.py  # Check it starts without errors

# Freeze new versions:
pip freeze > requirements.txt
```

> ⚠️ **Never** run `pip install --upgrade` on everything at once. Libraries can break each other. Upgrade one at a time and test.

### Monthly Maintenance Checklist

- [ ] Check disk usage (`df -h`) — clean if > 70% full
- [ ] Verify last month's data loaded correctly (check DB counts)
- [ ] Review Telegram alerts for any failures
- [ ] Check API error logs for recurring issues
- [ ] Confirm backup files exist in `backups/`
- [ ] Test one end-to-end pipeline manually (download → load → verify in UI)

---

*Document maintained by the engineering team. Last updated: March 2026.*
