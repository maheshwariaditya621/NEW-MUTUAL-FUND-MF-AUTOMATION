# Automated Indian Company Earnings Calendar & Live Results Tracking System
## Operational & Architecture Documentation
**Platform:** AV Fincorp Institutional / Rupeevest Research Platform  
**Version:** 1.0.0 (Production Release)  
**Date:** October 14, 2026  

---

## 1. Architecture Overview

The Earnings Calendar & Live Results Tracking subsystem is an automated, real-time intelligence engine that tracks upcoming corporate board meetings (mandated by SEBI LODR Regulation 29), forward analyst consensus estimates, and official financial-results filings disseminated by the Bombay Stock Exchange (BSE) and National Stock Exchange (NSE).

### 1.1 Core Components
1. **Source Connectors (`src/earnings/sources/`):**
   - **`BSESource`**: Statutory disclosures from BSE India API (`AnnSubCategoryGetData/w`) for board meetings (`strCat=Board Meeting`) and actual financial results (`strCat=Result`).
   - **`NSESource`**: Exchange feed from NSE India (`/api/corporate-board-meetings` and `/api/corporate-announcements`) with automatic session-cookie priming and an independent provider toggle (`ENABLE_NSE_COLLECTOR`).
   - **`TigZigYfinSource`**: Supplementary consensus EPS and Revenue estimates and expected earnings calendar dates via TigZig YFIN API (`/v1/get-calendar/`).
2. **Deterministic Classifier (`src/earnings/classifier.py`):**
   - Natural language and regex extraction for Fiscal Year (`FY2026`, `FY2027`) and Fiscal Quarter (`Q1`, `Q2`, `Q3`, `Q4`, `FY`).
   - Standalone vs. Consolidated classification (`STANDALONE`, `CONSOLIDATED`, `BOTH`).
   - Audit status classification (`AUDITED`, `UNAUDITED`, `LIMITED_REVIEW`).
   - Meeting status detection (`SCHEDULED`, `RESCHEDULED`, `POSTPONED`, `CANCELLED`).
   - Revision and corrigendum detection (`CORRIGENDUM`, `RESUBMISSION`, `REVISION`).
3. **Reconciliation & Historical Audit Engine (`src/earnings/reconciliation.py`):**
   - Automatically links unlinked published filings in `earnings_results_filings` to their parent meeting in `earnings_calendar`.
   - Transitions calendar status from `SCHEDULED` to `RESULTS_PUBLISHED`.
   - Records an immutable, append-only history trail in `earnings_calendar_history` for any date reschedule or status transition.
   - Auto-concludes meetings whose dates passed >3 days ago without filings.
4. **Master Ingestion Engine (`src/earnings/engine.py`):**
   - Coordinates multi-source ingestion, idempotent database upserts, sync state logging, and optional Telegram alerts.
5. **FastAPI Endpoints (`src/api/routers/earnings.py`):**
   - REST endpoints for calendar, today's desk, results ledger, company dossier, event inspector, and health status.
6. **React 18 Frontend (`frontend/src/pages/EarningsPage.jsx`):**
   - Dedicated `/earnings` view with KPI metrics, Today's Results Desk (Awaiting vs Published), Calendar Grid & Agenda views, Results Ledger, and Feed Health.
   - Integrated with Stock Holdings (`StockHoldingsPage.jsx`) via interactive earnings badges.

---

## 2. Database Schema & Migration Notes

Migration file: `database/migrations/033_earnings_calendar.sql`

### 2.1 `earnings_calendar`
Stores forward-looking board meetings and expected results dates.

| Column | Type | Description |
| :--- | :--- | :--- |
| `event_id` | `BIGSERIAL PRIMARY KEY` | Unique synthetic identifier |
| `company_id` | `BIGINT NOT NULL REFERENCES companies` | Canonical company link |
| `isin` | `VARCHAR(12) NOT NULL` | 12-char ISIN |
| `exchange_symbol` | `VARCHAR(50)` | NSE Symbol |
| `bse_code` | `VARCHAR(20)` | 6-digit BSE Scrip Code |
| `financial_year` | `VARCHAR(10)` | e.g. 'FY2026', 'FY2027' |
| `fiscal_period` | `VARCHAR(10)` | e.g. 'Q1', 'Q2', 'Q3', 'Q4', 'FY' |
| `meeting_date` | `DATE NOT NULL` | Scheduled board meeting date |
| `meeting_time` | `VARCHAR(50)` | Optional meeting time if disclosed |
| `expected_results_date` | `DATE` | Expected publication date |
| `raw_purpose` | `TEXT` | Raw statutory disclosure agenda |
| `normalized_event_type` | `VARCHAR(50)` | Default `'BOARD_MEETING_RESULTS'` |
| `event_status` | `VARCHAR(50)` | `'SCHEDULED'`, `'RESCHEDULED'`, `'RESULTS_PUBLISHED'`, `'CONCLUDED'`, `'POSTPONED'`, `'CANCELLED'` |
| `source_name` | `VARCHAR(50)` | `'BSE'`, `'NSE'`, `'TIGZIG'`, `'COMPANY_IR'` |
| `source_record_id` | `VARCHAR(100)` | Unique record ID from provider |
| `verification_status` | `VARCHAR(50)` | `'VERIFIED_EXCHANGE'`, `'THIRD_PARTY_CALENDAR'` |
| `consensus_eps` | `NUMERIC(10, 2)` | Analyst consensus EPS |
| `consensus_revenue_inr` | `NUMERIC(18, 2)` | Analyst consensus Revenue (INR) |
| `first_detected_at` | `TIMESTAMPTZ` | Internal first detection timestamp |
| `last_verified_at` | `TIMESTAMPTZ` | Last verification timestamp |

### 2.2 `earnings_calendar_history`
Append-only audit ledger for change tracking.

| Column | Type | Description |
| :--- | :--- | :--- |
| `history_id` | `BIGSERIAL PRIMARY KEY` | Unique history record ID |
| `event_id` | `BIGINT REFERENCES earnings_calendar` | Target calendar event |
| `company_id` | `BIGINT REFERENCES companies` | Company ID |
| `old_meeting_date` | `DATE` | Previous meeting date |
| `new_meeting_date` | `DATE` | Newly rescheduled date |
| `old_status` | `VARCHAR(50)` | Previous status |
| `new_status` | `VARCHAR(50)` | New status |
| `change_reason` | `TEXT` | Explanation / reason |
| `detected_at` | `TIMESTAMPTZ` | Detection instant |
| `source_evidence` | `TEXT` | Source filing ID or URL |

### 2.3 `earnings_results_filings`
Actual published financial results filings from exchanges.

| Column | Type | Description |
| :--- | :--- | :--- |
| `filing_id` | `BIGSERIAL PRIMARY KEY` | Unique filing ID |
| `company_id` | `BIGINT REFERENCES companies` | Company link |
| `calendar_event_id` | `BIGINT REFERENCES earnings_calendar` | Linked scheduled meeting event |
| `exchange` | `VARCHAR(10)` | `'BSE'`, `'NSE'`, `'BOTH'` |
| `source_filing_id` | `VARCHAR(100)` | Exchange sequence ID |
| `financial_year` | `VARCHAR(10)` | FY (e.g. 'FY2027') |
| `fiscal_period` | `VARCHAR(10)` | Period (e.g. 'Q2') |
| `result_type` | `VARCHAR(50)` | `'STANDALONE'`, `'CONSOLIDATED'`, `'BOTH'`, `'UNKNOWN'` |
| `audit_status` | `VARCHAR(50)` | `'AUDITED'`, `'UNAUDITED'`, `'LIMITED_REVIEW'`, `'UNKNOWN'` |
| `submission_timestamp` | `TIMESTAMPTZ` | Company submission timestamp |
| `dissemination_timestamp` | `TIMESTAMPTZ` | Exchange public publication timestamp |
| `detected_at` | `TIMESTAMPTZ` | System detection timestamp |
| `filing_title` | `TEXT` | Disclosure headline & subject |
| `attachment_url` | `TEXT` | Direct link to exchange PDF |
| `is_revision` | `BOOLEAN` | Revision / Corrigendum flag |
| `revision_type` | `VARCHAR(50)` | `'CORRIGENDUM'`, `'RESUBMISSION'`, `'REVISION'` |

### 2.4 `earnings_sync_state`
Operational observability and sync checkpoints.

---

## 3. API Endpoint Reference

All endpoints are mounted under `/api/v1/earnings` and require a valid JWT Bearer token.

### 3.1 `GET /api/v1/earnings/calendar`
Returns paginated scheduled events.
- **Query Parameters:**
  - `page` (default: 1)
  - `limit` (default: 50, max: 100)
  - `from_date` (YYYY-MM-DD)
  - `to_date` (YYYY-MM-DD)
  - `company_id` (int)
  - `event_status` (`SCHEDULED`, `RESULTS_PUBLISHED`, `RESCHEDULED`, `CONCLUDED`, `POSTPONED`, `CANCELLED`)
  - `financial_year` (`FY2026`, `FY2027`)
  - `fiscal_period` (`Q1`, `Q2`, `Q3`, `Q4`, `FY`)
  - `watchlist_only` (`true` / `false`)
  - `search` (company name, ticker symbol, BSE code, ISIN)

### 3.2 `GET /api/v1/earnings/today`
Returns Today's Results Desk:
- `summary`: `scheduled_today`, `published_today`, `upcoming_this_week`, `total_scheduled_active`.
- `awaiting_results`: List of companies scheduled for today awaiting results.
- `results_published`: List of results filings published today with exact dissemination timestamps.

### 3.3 `GET /api/v1/earnings/results`
Returns paginated published financial results filings with direct PDF links.
- **Query Parameters:** `page`, `limit`, `from_date`, `to_date`, `exchange`, `fiscal_period`, `result_type`, `audit_status`, `search`.

### 3.4 `GET /api/v1/earnings/company/{company_id}`
Returns complete earnings dossier for a single company:
- `company`: Master metadata.
- `upcoming_events`: Future scheduled meetings.
- `past_events`: Historical meetings.
- `filings`: All historical financial results filings.
- `history`: Full audit trail of date reschedules.

### 3.5 `GET /api/v1/earnings/event/{event_id}`
Returns single event inspector with linked filings and date change history.

### 3.6 `GET /api/v1/earnings/sync-status`
Returns diagnostic health of all sources (`BSE_BOARD_MEETINGS`, `BSE_RESULTS_FEED`, `NSE_BOARD_MEETINGS`, `NSE_RESULTS_FEED`, `TIGZIG_CONSENSUS`, `RECONCILIATION_ENGINE`).

### 3.7 `POST /api/v1/earnings/sync-now`
Admin-only endpoint to trigger an immediate multi-source sync cycle.

---

## 4. Deployment & Systemd Configuration

### 4.1 Systemd Service Files
- Service: `deployment/mf-earnings.service`
- Timer: `deployment/mf-earnings.timer` (runs every 15 minutes during market and post-market hours, 08:00 to 23:45 IST, Monday through Saturday).

### 4.2 Installing on Oracle Cloud VM
```bash
sudo cp deployment/mf-earnings.service /etc/systemd/system/
sudo cp deployment/mf-earnings.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now mf-earnings.timer
sudo systemctl status mf-earnings.timer
```

### 4.3 Triggering a Manual Run on Server
```bash
cd /home/ubuntu/mf-app
.venv/bin/python -m src.earnings.engine
```

---

## 5. Environment Variables & Configuration

| Variable | Default | Description |
| :--- | :--- | :--- |
| `ENABLE_NSE_COLLECTOR` | `true` | Set to `false` to disable NSE polling without breaking BSE/TigZig |
| `ENABLE_EARNINGS_TELEGRAM_ALERTS` | `false` | Set to `true` to dispatch real-time Telegram alerts for newly detected filings |
| `DATABASE_URL` | Configured | PostgreSQL connection string |

---

## 6. Source Rate Limits & Access Rules

1. **BSE India API:**
   - Permitted rate: ~1–2 requests per second.
   - Pacing: Built into `BSESource`.
2. **NSE India API:**
   - Protected by session cookies.
   - Pacing: `NSESource._prime_session()` handles session initialization.
   - If blocked or degraded, the system logs a warning and proceeds with BSE.
3. **TigZig YFIN API:**
   - Limit: 100 requests / minute.
   - Pacing: Batches of 10 tickers with 0.5s pause between requests.

---

## 7. Troubleshooting & Recovery

### 7.1 "No results published today yet"
- Normal before market hours (09:15 IST) or on trading holidays.
- Verify status via `GET /api/v1/earnings/sync-status`.

### 7.2 NSE Returns 401 or 403
- NSE session cookies expire periodically. `NSESource` automatically attempts a re-prime. If persistent, `ENABLE_NSE_COLLECTOR=false` can be toggled in environment variables.

### 7.3 Backfilling Past Events
To run a backfill over the past 30 days:
```bash
.venv/bin/python -c "from src.earnings.engine import EarningsEngine; e=EarningsEngine(); e.bse.fetch_results_filings(lookback_days=30); e.bse.fetch_scheduled_events(lookahead_days=60); e.run_cycle()"
```
