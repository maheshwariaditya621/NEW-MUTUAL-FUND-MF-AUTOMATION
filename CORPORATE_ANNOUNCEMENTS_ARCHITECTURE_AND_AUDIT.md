# Corporate Announcements Module: Complete Architecture, Technical Audit & Implementation Blueprint

> **Project**: Mutual Fund Portfolio Analytics Platform (`AV Fincorp`)  
> **Document Purpose**: Comprehensive technical audit of existing production architecture, database schema, infrastructure, and an end-to-end implementation specification for the new **Corporate Announcements** module. Optimized for review, ChatGPT integration, or direct implementation by a software engineering team.  
> **Date**: September 2026  
> **Environment**: AWS EC2 (`t3.micro`), Ubuntu 22.04 LTS, Python 3.10+ / FastAPI, PostgreSQL 14+, React 19 (Vite, Vanilla CSS)

---

## 1. Executive Summary & Key Owner Decisions

The platform is an existing, active production system providing institutional mutual fund analytics, scheme holdings tracking, and stock-level insights. We are adding an internal, office-only **Corporate Announcements** module that monitors NSE and BSE corporate filings with zero added infrastructure cost.

### Key Owner Decisions (Locked & Confirmed)
1. **Single Master Office Watchlist**: 
   - There will be a single, shared Master Office Watchlist for the announcement monitoring engine.
   - Office users add/manage companies in this shared list. The system does *not* monitor the entire 5,000+ Indian stock market; it monitors **only** the active companies present in this Master Watchlist.
2. **Document & PDF Lifecycle Management**:
   - **First 7 Days (Hot Tier)**: Original PDF is downloaded to local server storage and served directly for instant opening/previewing.
   - **Day 8 to Day 60 (Warm Tier)**: PDF remains locally stored on disk for quick retrieval.
   - **After 60 Days (Cold / Purge Tier)**: The local PDF file is automatically purged from the server disk to conserve storage, and the application seamlessly transitions to providing the direct external link to the NSE / BSE original source filing.
   - **Direct Exchange Link**: The canonical source URL (NSE/BSE document URL) is permanently stored in the database and never removed.
3. **Deployment Day 1 Startup**:
   - No historical multi-year backfill is required. Polling and indexing commence from **Day 1 of deployment** onwards.
4. **Target Operating Cost**:
   - **₹0 / month incremental expense**. Operates within the existing AWS EC2 Free Tier / standard `t3.micro` instance without any paid third-party APIs, paid proxies, or external cloud storage.

---

## 2. Existing System Architecture & Technology Stack Audit

### 2.1 Technology Stack Matrix
| Layer | Existing Technology | Details & Version | Source File / Evidence |
| :--- | :--- | :--- | :--- |
| **Frontend Framework** | React 19 | React `^19.2.0`, React DOM `^19.2.0` | `frontend/package.json` |
| **Frontend Routing** | React Router DOM | Version `^7.13.0` | `frontend/package.json` |
| **Frontend Build Tool** | Vite | Version `^7.3.1` with `@vitejs/plugin-react: ^5.1.1` | `frontend/vite.config.js` |
| **UI Design System** | Vanilla CSS + Theme Tokens | Hand-crafted CSS variables (`[data-theme="dark"]`), responsive layout | `frontend/src/index.css`, `frontend/src/App.css` |
| **Iconography** | Lucide React | Version `^0.564.0` | `frontend/package.json` |
| **Client Utilities** | jsPDF, XLSX, DOMPurify | Table exports to PDF/Excel, sanitization | `frontend/src/utils/exportUtils.js` |
| **Backend Framework** | FastAPI / Python 3.10+ | REST API, Pydantic validation, CORS middleware | `src/api/main.py`, `requirements.txt` |
| **Application Server** | Uvicorn | Production multi-worker (`workers=4`), dev hot-reload | `run_api.py`, `run_api_prod.py` |
| **Database Engine** | PostgreSQL 14+ | Relational SQL, `pg_trgm` extension for fuzzy search | `database/schema_v1.0.sql`, `database/migrations/` |
| **DB Access & Pooling** | `psycopg2-binary` | `ThreadedConnectionPool` (1-20 connections), TCP keepalives | `src/db/connection.py`, `src/db/repositories.py` |
| **Authentication** | JWT (HS256) + bcrypt | OAuth2 Password Bearer, token expiry (480 min), TOTP 2FA | `src/api/dependencies.py`, `src/api/utils/auth_utils.py` |
| **Authorization / RBAC** | Role & JSONB Permissions | Roles (`admin`, `user`) + `permissions` JSONB array on users table | `src/api/dependencies.py`, `database/migrations/028_secure_multi_user.sql` |
| **Background Jobs** | Linux systemd + Python loops | In-process scheduler loops, APScheduler for portfolio downloads | `src/corporate_actions/ca_scheduler.py`, `src/scheduler/` |
| **File Storage** | Local Linux Filesystem | Local paths under `data/raw/`, `data/output/` | `src/config/settings.py`, `.agent/rules/project-workflow.md` |
| **Alerting System** | Telegram Bot | Automated operational alert broadcasts | `src/alerts/telegram_notifier.py` |
| **Production Hosting** | AWS EC2 `t3.micro` | Ubuntu 22.04 LTS, Mumbai (`ap-south-1`), Nginx reverse proxy | `AWS_SIMPLE_DEPLOYMENT_PLAN.md` |

---

### 2.2 Relevant Existing Database Schema

The platform already has mature company and security master tables that cross-map NSE symbols, BSE scrip codes, and ISINs:

```sql
-- 1. COMPANIES TABLE (Master Equity Table)
CREATE TABLE companies (
    company_id      BIGSERIAL PRIMARY KEY,
    isin            CHAR(12) NOT NULL UNIQUE CHECK (isin ~ '^INE[A-Z0-9]{6}10[A-Z0-9]{1}$'),
    company_name    VARCHAR(255) NOT NULL,
    exchange_symbol VARCHAR(20),
    sector          VARCHAR(100),
    industry        VARCHAR(100),
    nse_symbol      VARCHAR(20),
    bse_code        VARCHAR(20),
    entity_id       INTEGER REFERENCES corporate_entities(entity_id),
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 2. ISIN MASTER TABLE (Resolution & Mapping Anchor)
CREATE TABLE isin_master (
    isin            VARCHAR(12) PRIMARY KEY,
    canonical_name  TEXT NOT NULL,
    nse_symbol      VARCHAR(20),
    bse_code        VARCHAR(20),
    sector          TEXT,
    industry        TEXT,
    entity_id       INTEGER REFERENCES corporate_entities(entity_id),
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. CORPORATE ENTITIES (Group Parent / Business Anchor)
CREATE TABLE corporate_entities (
    entity_id       SERIAL PRIMARY KEY,
    canonical_name  TEXT NOT NULL,
    group_symbol    VARCHAR(20) UNIQUE,
    sector          TEXT,
    industry        TEXT,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. USERS TABLE (RBAC & Auth)
CREATE TABLE users (
    id              SERIAL PRIMARY KEY,
    username        VARCHAR(50) UNIQUE NOT NULL,
    email           VARCHAR(100) UNIQUE NOT NULL,
    password_hash   VARCHAR(255) NOT NULL,
    role            VARCHAR(20) DEFAULT 'user',
    is_active       BOOLEAN DEFAULT TRUE,
    permissions     JSONB DEFAULT '[]'::jsonb, -- e.g. ["view_stocks", "view_announcements", "admin"]
    failed_login_attempts INTEGER DEFAULT 0,
    locked_until    TIMESTAMP WITH TIME ZONE,
    expires_at      TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 5. EXISTING USER WATCHLIST (Personal User Watchlists)
CREATE TABLE user_watchlist (
    watchlist_id    BIGSERIAL PRIMARY KEY,
    user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    asset_type      VARCHAR(10) NOT NULL CHECK (asset_type IN ('stock', 'scheme')),
    company_id      BIGINT REFERENCES companies(company_id) ON DELETE CASCADE,
    scheme_id       BIGINT REFERENCES schemes(scheme_id) ON DELETE CASCADE,
    added_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_watchlist_stock UNIQUE (user_id, company_id),
    CONSTRAINT uq_watchlist_scheme UNIQUE (user_id, scheme_id)
);
```

---

## 3. Recommended Corporate Announcements Architecture

```
                               ┌─────────────────────────────┐
                               │   Master Office Watchlist   │
                               │  (50 - 250 Active Stocks)   │
                               └──────────────┬──────────────┘
                                              │ In-memory hash set of
                                              │ (NSE Symbols & BSE Codes)
                                              ▼
┌──────────────────────────┐       ┌──────────────────────────────────────┐       ┌──────────────────────────┐
│      NSE India Feed      │       │     Master Announcement Worker       │       │      BSE India Feed      │
│ (equities announcements) │──────►│  (Daemon: mf-announcements.service)  │◄──────│   (AnnGetData/w Feed)    │
│    Polls every 60s       │       │    Lightweight HTTP Session (GET)    │       │     Polls every 60s      │
└──────────────────────────┘       └──────────────────┬───────────────────┘       └──────────────────────────┘
                                                      │
                                                      │ Fast In-Memory Watchlist Filter
                                                      │ (Discards non-watchlist market noise)
                                                      ▼
                                   ┌──────────────────────────────────────┐
                                   │  Normalization & Identity Resolution │
                                   │  - Symbol/Scrip -> canonical company │
                                   │  - Datetime -> UTC & IST             │
                                   │  - Extract exchange attachment URLs  │
                                   └──────────────────┬───────────────────┘
                                                      │
                                                      ▼
                                   ┌──────────────────────────────────────┐
                                   │  Deduplication & Cross-Exchange Link │
                                   │  - SHA256 Signature of filing        │
                                   │  - 4-Hour Time-Window & Fuzzy Match  │
                                   │  - Combines NSE & BSE into 1 entry   │
                                   └──────────────────┬───────────────────┘
                                                      │
                                                      ▼
                                   ┌──────────────────────────────────────┐
                                   │   Multi-Tag Rule Classification      │
                                   │   (Dividend, Bonus, M&A, Results,    │
                                   │    Board Meeting, Buyback, Debt, etc)│
                                   └──────────────────┬───────────────────┘
                                                      │
                                                      ▼
                                   ┌──────────────────────────────────────┐
                                   │         PostgreSQL Database          │
                                   │ - corporate_announcements            │
                                   │ - announcement_attachments           │
                                   │ - master_watchlist                   │
                                   └──────────────────┬───────────────────┘
                                                      │
                         ┌────────────────────────────┴────────────────────────────┐
                         ▼                                                         ▼
┌──────────────────────────────────────────────────┐      ┌──────────────────────────────────────────────────┐
│             FastAPI Backend Services             │      │            PDF Storage & Lifecycle Worker        │
│          (/api/v1/announcements)                │      │ - Days 0-7: Direct Local PDF Serving             │
│ - Feed listing (pagination, filters, search)     │      │ - Days 8-60: Stored locally on server disk       │
│ - Category aggregations                          │      │ - Day 61+: Auto-purge PDF, link-only fallback    │
│ - Secure PDF streaming / proxy fallback          │      │ - External NSE/BSE link permanently retained     │
└────────────────────────┬─────────────────────────┘      └──────────────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────┐
│              React 19 Frontend UI                │
│            (/announcements Route)                │
│ - Live announcement timeline & cards             │
│ - Category filter pills & keyword search         │
│ - Embedded PDF preview modal + Source Link button│
│ - Manage Master Watchlist modal for office team  │
└──────────────────────────────────────────────────┘
```

---

## 4. Polling Strategy & High-Efficiency Scraping Mechanics

### 4.1 The "Market Feed" Pattern vs. "Individual Stock Polling"
- **Do NOT** iterate over 200 stocks individually (making 200 requests/minute to NSE and 200 requests/minute to BSE). That would total 400 requests/min, resulting in rapid IP bans, 403 Forbidden responses, and excessive resource usage.
- **DO USE** the **Market-Wide Consolidated Announcements Feed**:
  1. **NSE Endpoint**: `https://www.nseindia.com/api/corporate-announcements?index=equities`
     - Returns the latest 50 to 100 announcements filed across all listed equities in reverse chronological order.
     - Rate: Exactly **1 HTTP request every 60 seconds**.
     - Session Priming: Hits `https://www.nseindia.com` once per session using `requests.Session()` to receive session cookies and spoof standard browser headers.
  2. **BSE Endpoint**: `https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w?pageno=1&strCat=-1&strPrevDate=&strScrip=&strSearch=P&strToDate=&strType=C`
     - Returns the real-time sliding window of corporate announcements across all BSE listed equities.
     - Rate: Exactly **1 HTTP request every 60 seconds**.
     - Header Requirements: Standard browser User-Agent and `Referer: https://www.bseindia.com/`.

### 4.2 Resource & Overhead Footprint
- Total requests: **2 requests per minute** (120 requests/hour across the entire server).
- Inbound bandwidth: ~50 KB per request = ~6 MB / hour = ~4.3 GB / month.
- Memory consumption: **< 45 MB RAM**. No Playwright or Chromium processes are involved.
- Zero risk to EC2 `t3.micro` stability.

---

## 5. Required New Database Schema (Migration 029)

Save as `database/migrations/029_corporate_announcements.sql`:

```sql
BEGIN;

-- ============================================================
-- 1. MASTER OFFICE WATCHLIST
-- Shared office list of companies monitored for announcements
-- ============================================================
CREATE TABLE IF NOT EXISTS master_watchlist (
    master_id       BIGSERIAL PRIMARY KEY,
    company_id      BIGINT NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    notes           TEXT,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    added_by        INTEGER REFERENCES users(id) ON DELETE SET NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_master_watchlist_company UNIQUE (company_id)
);

CREATE INDEX IF NOT EXISTS idx_master_watchlist_active ON master_watchlist(is_active);

-- ============================================================
-- 2. CORPORATE ANNOUNCEMENTS
-- Canonical unified repository of corporate announcements
-- ============================================================
CREATE TABLE IF NOT EXISTS corporate_announcements (
    announcement_id       BIGSERIAL PRIMARY KEY,
    company_id            BIGINT NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    isin                  VARCHAR(12) NOT NULL,
    company_name          VARCHAR(255) NOT NULL,
    broadcast_timestamp   TIMESTAMP WITH TIME ZONE NOT NULL,
    subject               TEXT NOT NULL,
    details               TEXT,
    
    -- Exchange Origin Flags
    is_nse                BOOLEAN NOT NULL DEFAULT FALSE,
    is_bse                BOOLEAN NOT NULL DEFAULT FALSE,
    nse_announcement_id   VARCHAR(100),
    bse_announcement_id   VARCHAR(100),
    
    -- Permanent Canonical URLs
    nse_source_url        TEXT,
    bse_source_url        TEXT,
    
    -- Multi-Tag Classification
    categories            TEXT[] NOT NULL DEFAULT '{Other}',
    
    -- Attachment Availability & Status
    has_attachment        BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- Deduplication Hash: SHA256(company_id + date + normalized_subject)
    dedup_signature       VARCHAR(64) NOT NULL UNIQUE,
    
    -- Raw Payload for Debug / Re-processing
    raw_payload           JSONB,
    created_at            TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_announcements_company ON corporate_announcements(company_id);
CREATE INDEX IF NOT EXISTS idx_announcements_broadcast ON corporate_announcements(broadcast_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_announcements_categories ON corporate_announcements USING GIN(categories);
CREATE INDEX IF NOT EXISTS idx_announcements_dedup ON corporate_announcements(dedup_signature);

-- ============================================================
-- 3. ANNOUNCEMENT ATTACHMENTS & DOCUMENT LIFECYCLE
-- Stores original PDF metadata, local disk path, and expiry state
-- ============================================================
CREATE TABLE IF NOT EXISTS announcement_attachments (
    attachment_id         BIGSERIAL PRIMARY KEY,
    announcement_id       BIGINT NOT NULL REFERENCES corporate_announcements(announcement_id) ON DELETE CASCADE,
    exchange              VARCHAR(10) NOT NULL, -- 'NSE' or 'BSE'
    original_file_name    VARCHAR(255) NOT NULL,
    source_file_url       TEXT NOT NULL,       -- Permanent direct exchange link
    local_file_path       TEXT,                -- Local path on disk while within 60 days
    file_size_bytes       BIGINT,
    mime_type             VARCHAR(100) DEFAULT 'application/pdf',
    
    -- Lifecycle Tracking: 'HOT' (<=7d), 'WARM' (8-60d), 'PURGED' (>60d)
    lifecycle_stage       VARCHAR(20) NOT NULL DEFAULT 'HOT',
    is_locally_available  BOOLEAN NOT NULL DEFAULT FALSE,
    downloaded_at         TIMESTAMP WITH TIME ZONE,
    purged_at             TIMESTAMP WITH TIME ZONE,
    
    created_at            TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_attachments_announcement ON announcement_attachments(announcement_id);
CREATE INDEX IF NOT EXISTS idx_attachments_lifecycle ON announcement_attachments(lifecycle_stage, is_locally_available);

COMMIT;
```

---

## 6. Document & PDF Lifecycle Architecture (7-Day Direct / 60-Day Purge)

Per the owner's exact requirements, server storage must not grow uncontrollably:

### 6.1 Lifecycle Progression Rules
1. **Tier 1: Hot Direct PDF (Day 0 – Day 7)**:
   - When the worker ingests an announcement with an attachment, it asynchronously downloads the PDF to disk:  
     `data/announcements/pdfs/{YYYY}/{MM}/{attachment_id}_{hash}.pdf`
   - In the database: `lifecycle_stage = 'HOT'`, `is_locally_available = TRUE`.
   - UI Behavior: Clicking "View PDF" instantly opens the local file in an embedded in-app viewer with zero latency and zero dependency on exchange servers.
2. **Tier 2: Warm PDF (Day 8 – Day 60)**:
   - The file remains on local disk.
   - `lifecycle_stage = 'WARM'`.
   - UI displays local preview, plus a button to "Open Original Exchange Source".
3. **Tier 3: Cold / Purged (Day 61+)**:
   - A daily lifecycle maintenance task identifies attachments where `created_at < NOW() - INTERVAL '60 days'` and `is_locally_available = TRUE`.
   - The task deletes the physical file from disk via `os.remove(local_file_path)`.
   - Updates record: `local_file_path = NULL`, `is_locally_available = FALSE`, `lifecycle_stage = 'PURGED'`, `purged_at = CURRENT_TIMESTAMP`.
   - UI Behavior: Clicking "View Document" smoothly opens the permanent `source_file_url` directly on the NSE/BSE archive portal in a new tab.

---

## 7. Deduplication, Normalization & Categorization Rules

### 7.1 Cross-Exchange Deduplication Algorithm
1. Normalize company identification: Both NSE Symbol and BSE Scrip Code map to canonical `company_id`.
2. Normalize subject text:
   - Lowercase string.
   - Strip punctuation, extra whitespaces, common prefixes (e.g. `Outcome of Board Meeting`, `Updates`, `Intimation under Regulation 30`).
   - Remove company name prefix if repeated.
3. Compute Dedup Signature:
   `signature = SHA256(f"{company_id}_{filing_date}_{normalized_subject[:60]}")`
4. Time-Window Merger:
   - If an incoming filing matches an existing record's `company_id` within a **±4 hour window** with a Levenshtein/Rapidfuzz similarity > 85%:
   - Merge into the existing record: set `is_bse = TRUE` (or `is_nse = TRUE`), record `bse_announcement_id`, and add any secondary attachment.

### 7.2 Multi-Tag Categorization Engine
A single announcement can match multiple tags based on rule priority:

| Category Tag | Trigger Keywords / Regex Patterns |
| :--- | :--- |
| **Dividend** | `dividend`, `interim div`, `final div`, `special div`, `record date for dividend` |
| **Bonus** | `bonus issue`, `bonus shares`, `allotment of bonus` |
| **Stock Split** | `sub-division`, `sub division`, `split`, `face value reduction` |
| **Rights Issue** | `rights issue`, `rights entitlement`, `draft letter of offer` |
| **Buyback** | `buyback`, `buy-back`, `tender offer`, `repurchase of shares` |
| **M&A** | `amalgamation`, `merger`, `demerger`, `acquisition`, `takeover`, `scheme of arrangement`, `joint venture`, `jv` |
| **Board Meeting** | `board meeting`, `meeting of board of directors`, `bm outcome`, `outcome of meeting` |
| **Financial Results** | `financial results`, `audited results`, `unaudited results`, `q1`, `q2`, `q3`, `q4`, `limited review` |
| **Fund Raising** | `fund raising`, `qip`, `preferential allotment`, `warrants`, `commercial paper`, `ncd`, `debentures` |
| **Credit Rating** | `credit rating`, `crisil`, `icra`, `care`, `ind-ra`, `downgrade`, `upgrade` |
| **Management Changes** | `resignation`, `appointment`, `cessation`, `kmp`, `chief executive`, `cfo`, `ceo`, `director` |
| **Investor/Analyst Meet**| `investor meet`, `analyst meet`, `earnings call`, `concall transcript`, `presentation` |
| **Orders/Contracts** | `bagged order`, `award of contract`, `work order`, `agreement signed`, `loi received` |
| **Regulatory/Legal** | `sebi`, `show cause`, `penalty`, `litigation`, `court order`, `nclt`, `cbi`, `enforcement` |
| **Business Updates** | `press release`, `operational update`, `capacity expansion`, `commissioning` |
| **Other** | Default fallback if no specific keywords match |

---

## 8. Backend API Specifications

Base path: `/api/v1/announcements`

### 8.1 Endpoint Catalog
1. `GET /api/v1/announcements`
   - **Access**: Authenticated (`view_announcements` permission)
   - **Query Params**:
     - `page` (int, default: 1)
     - `page_size` (int, default: 25, max: 100)
     - `company_id` (optional int)
     - `category` (optional string, e.g. `Dividend`)
     - `search` (optional string: searches subject, company name, symbol)
     - `from_date` / `to_date` (optional ISO dates)
     - `exchange` (optional: `NSE`, `BSE`, `ALL`)
   - **Response**:
     ```json
     {
       "total": 142,
       "page": 1,
       "page_size": 25,
       "items": [
         {
           "announcement_id": 892,
           "company_id": 45,
           "company_name": "TATA CONSULTANCY SERVICES LIMITED",
           "isin": "INE467B01029",
           "nse_symbol": "TCS",
           "bse_code": "532540",
           "broadcast_timestamp": "2026-09-12T14:32:00+05:30",
           "subject": "Outcome of the Board Meeting - Declaration of Interim Dividend",
           "categories": ["Board Meeting", "Dividend"],
           "is_nse": true,
           "is_bse": true,
           "has_attachment": true,
           "attachment": {
             "attachment_id": 1204,
             "is_locally_available": true,
             "lifecycle_stage": "HOT",
             "file_name": "TCS_Dividend_Outcome.pdf",
             "download_url": "/api/v1/announcements/attachments/1204/view",
             "source_url": "https://nsearchives.nseindia.com/corporate/TCS_Outcome_12092026.pdf"
           }
         }
       ]
     }
     ```
2. `GET /api/v1/announcements/categories`
   - Returns list of categories and counts for filter pills.
3. `GET /api/v1/announcements/attachments/{attachment_id}/view`
   - If `is_locally_available = TRUE`, streams the PDF via `FileResponse(media_type='application/pdf')`.
   - If purged, returns an HTTP 307 Redirect to `source_file_url`.
4. `GET /api/v1/announcements/master-watchlist`
   - Lists all companies currently active in the Master Office Watchlist.
5. `POST /api/v1/announcements/master-watchlist`
   - Adds a company by `company_id` or `isin` to the monitoring list.
6. `DELETE /api/v1/announcements/master-watchlist/{company_id}`
   - Deactivates a company from future polling (does NOT delete historical announcements).

---

## 9. Frontend User Interface Blueprint

### 9.1 Route & Layout Placement
- Route: `/announcements` in `frontend/src/App.jsx`.
- Header link added to `app-header`:
  ```jsx
  {hasPermission('view_announcements') && (
    <Link to="/announcements" className="nav-link">
      <span className="wl-icon">📢</span> Announcements
    </Link>
  )}
  ```

### 9.2 Page Component Architecture (`AnnouncementsPage.jsx`)
- **Top Header Bar**:
  - Page Title: "Corporate Announcements" with a live "Monitoring Active" green pulse indicator.
  - Action buttons: "Manage Master Watchlist" (modal) and `<ExportButton>` (Excel/CSV export).
- **Control Bar**:
  - `<SearchBox>`: Instant debounce search across company names, symbols, and filing headlines.
  - Exchange Filter: `All`, `NSE Only`, `BSE Only`.
  - Date Range Picker.
- **Category Filter Pills Bar**:
  - Horizontally scrollable category pills: `All`, `Financial Results`, `Dividend`, `Bonus/Split`, `Board Meeting`, `M&A`, `Orders`, `Credit Rating`.
- **Feed Timeline Cards**:
  - Company symbol pill (`TCS`), company name, and time ago (`"12 mins ago"`).
  - Exchange badge: `[NSE]` `[BSE]`.
  - Headline/Subject with highlight on search match.
  - Category badges: e.g. `[Board Meeting]` `[Dividend]`.
  - Document Button:
    - If within 7 days: Primary Button: `[📄 View Document]` (opens in clean modal previewer).
    - If past 60 days: Outline Button: `[↗ Open NSE/BSE Link]`.

---

## 10. Background Daemons & Deployment Configuration

### 10.1 Systemd Service Unit (`mf-announcements.service`)
Configured at `/etc/systemd/system/mf-announcements.service` on the EC2 server:

```ini
[Unit]
Description=Mutual Fund Corporate Announcements Worker
After=network.target postgresql.service mf-api.service

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/mf-app
Environment=PATH=/home/ubuntu/mf-app/.venv/bin
ExecStart=/home/ubuntu/mf-app/.venv/bin/python -m src.announcements.announcement_worker
Restart=always
RestartSec=15
StandardOutput=append:/home/ubuntu/mf-app/logs/announcements.log
StandardError=append:/home/ubuntu/mf-app/logs/announcements_error.log

[Install]
WantedBy=multi-user.target
```

### 10.2 Service Lifecycle Commands
```bash
sudo systemctl daemon-reload
sudo systemctl enable mf-announcements
sudo systemctl start mf-announcements
sudo systemctl status mf-announcements
```

---

## 11. Edge Cases & Resilience Engineering Matrix

| Scenario / Edge Case | Engineering Failure Risk | Implemented Architecture Solution |
| :--- | :--- | :--- |
| **Same filing on NSE and BSE** | Cluttered UI with duplicate cards | Deduplication engine computes SHA256 signature and merges filings within a 4-hour window, marking both `is_nse = true` and `is_bse = true`. |
| **Differing timestamps** | BSE filing appears at 10:15 AM; NSE filing appears at 10:45 AM | Sliding window deduplication identifies the same `company_id` and matching subject, merging into the first record. |
| **Corrigendum / Revision** | Overwrites original filing details | Stored as a new record linked via `is_revision = TRUE` and `parent_announcement_id`. |
| **Announcement without PDF** | Downloader throws null pointer | `has_attachment = FALSE`; UI renders headline with a "Text Only Announcement" badge. |
| **NSE Session Expiry / 403** | Worker blocked from polling | `NSECollector` catches 401/403 and automatically refreshes cookies by re-hitting the home page before retrying. |
| **BSE / NSE Server Downtime** | Daemon crashes on timeout | Network requests wrapped in exponential backoff try/except blocks. Failures log to Telegram alerts and retry in 60s. |
| **Company Removed from Watchlist** | Accidental data loss | Foreign key constraint on `master_watchlist` uses soft-deactivation (`is_active = FALSE`). Historical records in `corporate_announcements` are permanent. |
| **Large PDF Attachment (> 20 MB)** | Memory spike on 1 GB RAM EC2 | Stream file in 64 KB chunks directly from response stream to disk via `shutil.copyfileobj()` without loading file into Python memory. |
| **Corrupted PDF on Exchange** | PDF preview fails in browser | Worker checks HTTP status and validates `%PDF-` magic bytes. If invalid, falls back to direct exchange URL. |
| **Unmapped Category** | Filing dropped | Falls back to category tag `['Other']`. Never drops a valid announcement. |

---

## 12. Phased Implementation Roadmap

1. **Phase 1: Database Migration**:
   - Execute Migration `029_corporate_announcements.sql` creating `master_watchlist`, `corporate_announcements`, and `announcement_attachments`.
   - Seed `master_watchlist` with existing distinct stocks from `user_watchlist`.
2. **Phase 2: Ingestion & Classification Services**:
   - Implement `src/announcements/nse_collector.py` and `src/announcements/bse_collector.py`.
   - Implement `src/announcements/deduplicator.py` and `src/announcements/classifier.py`.
3. **Phase 3: Worker & PDF Lifecycle Manager**:
   - Implement `src/announcements/announcement_worker.py` (polling loop + 60-day auto-purge job).
   - Test locally with dry-run mode.
4. **Phase 4: API Endpoints & RBAC**:
   - Implement `src/api/routers/announcements.py`.
   - Add `"view_announcements"` permission to user permission scopes in `src/api/dependencies.py`.
5. **Phase 5: React Frontend UI**:
   - Build `frontend/src/pages/AnnouncementsPage.jsx` and `AnnouncementsPage.css`.
   - Add navigation link to `frontend/src/App.jsx`.
6. **Phase 6: Deployment**:
   - Deploy `mf-announcements.service` to Ubuntu EC2 and verify background logs.

---
*End of Specification — Prepared for AV Fincorp Internal Engineering Review*
