# Bulk & Block Deals Intelligence Module — Architecture & Deployment Guide

## 1. System Overview

The **Bulk & Block Deals** module provides end-to-end tracking, historical backfilling, automated synchronization, institutional categorization, and mutual fund cross-referencing for all major volume and block-window transactions executed on the **National Stock Exchange (NSE)** and **Bombay Stock Exchange (BSE)**.

```
                           ┌───────────────────────────────┐
                           │      Exchange Sources         │
                           │  • NSE JSON / Daily Archives  │
                           │  • BSE API Endpoints          │
                           │  • Fallback CSV Uploader      │
                           └──────────────┬────────────────┘
                                          │
                                          ▼
                           ┌───────────────────────────────┐
                           │       Normalizer Engine       │
                           │  • Parse Dates & Decimals     │
                           │  • Institutional Classification│
                           │    (MF, FII, DII, Promoter)   │
                           │  • Company Master Resolution  │
                           │  • % Equity Share Calculation │
                           │  • SHA256 Deterministic Hash  │
                           └──────────────┬────────────────┘
                                          │
                                          ▼
                           ┌───────────────────────────────┐
                           │    PostgreSQL Database        │
                           │  • market_deals               │
                           │  • market_deal_ingestion_runs │
                           │  • deal_sync_state            │
                           └──────────────┬────────────────┘
                                          │
                     ┌────────────────────┴────────────────────┐
                     ▼                                         ▼
        ┌─────────────────────────┐               ┌─────────────────────────┐
        │     FastAPI Router      │               │   Scheduled Automation  │
        │  /api/v1/deals          │               │  • Systemd Service      │
        │  /api/v1/deals/summary  │               │  • Systemd Timer        │
        │  /api/v1/deals/status   │               │  • CLI Backfill Tool    │
        │  /api/v1/deals/sync-now │               └─────────────────────────┘
        │  /api/v1/deals/upload   │
        └────────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │   Vite + React UI       │
        │  • Summary Stat Cards   │
        │  • Filter & Search Bar  │
        │  • Responsive Data Grid │
        │  • CSV Upload Modal     │
        │  • Health Status Modal  │
        └─────────────────────────┘
```

---

## 2. Exchange Sources & Regulation Timings

| Deal Type | Definition / Threshold | Window / Trading Hours | Publication Window on NSE/BSE |
| :--- | :--- | :--- | :--- |
| **Block Deals** | Minimum ₹10 Crore value or 5,000,000 shares through designated separate order window | **Morning:** 08:45 – 09:00 AM<br>**Afternoon:** 02:05 – 02:20 PM | Morning: ~09:15–09:30 AM<br>Afternoon: ~02:30–02:45 PM<br>Consolidated: ~05:00 PM |
| **Bulk Deals** | Single trade/client exceeding **0.5%** of company's total equity during normal market hours | Normal Trading Hours<br>(09:15 AM – 03:30 PM) | Disclosed post-market from **05:30 PM to 08:00 PM** |

---

## 3. Database Schema (`034_bulk_block_deals.sql`)

### A. `market_deals`
Stores normalized, deduplicated deals:
* `deal_id`: BigSerial Primary Key
* `deal_type`: `BULK` or `BLOCK`
* `exchange`: `NSE` or `BSE`
* `trade_date`: Date
* `company_id`: References `companies(company_id)` (Null if unmapped)
* `isin`: Standard 12-char ISIN
* `symbol`: Ticker / BSE Code
* `security_name`: Published Company Name
* `client_name`: Buyer or Seller Entity Name
* `investor_category`: `MUTUAL_FUND`, `FII_FPI`, `DII`, `PROMOTER`, `AIF`, `OTHER`
* `action`: `BUY` or `SELL`
* `quantity`: Traded shares count
* `trade_price`: Execution price (Numeric 14,4)
* `trade_value_inr`: `quantity * price` (Numeric 18,2)
* `pct_equity_traded`: Traded % of total shares outstanding
* `source_fingerprint`: Deterministic SHA256 unique index `(source_fingerprint)` ensuring **zero duplicates**

### B. `market_deal_ingestion_runs`
Audit and ledger for all collection runs (automated, backfill, or manual CSV).

### C. `deal_sync_state`
Tracks operational health, checkpoints, last attempted and successful timestamps, and date coverage ranges.

---

## 4. CLI Execution & Commands

All commands run using the application's virtual environment:

### A. Check Current System Status
```bash
python -m src.deals.engine --mode status
```

### B. Daily Synchronization (Recent 5 Days)
```bash
python -m src.deals.engine --mode sync --days 5 --type ALL
```

### C. Dry-Run Verification (Preview without writing to DB)
```bash
python -m src.deals.engine --mode sync --days 5 --dry-run
```

### D. Historical 12-Month Backfill
```bash
python -m src.deals.engine --mode backfill --months 12 --type ALL
```

---

## 5. API Endpoints

All endpoints are mounted under `/api/v1/deals` and protected by authentication:

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/api/v1/deals` | Filtered, sorted, and paginated deals list |
| `GET` | `/api/v1/deals/summary` | Aggregate statistics (turnover, buy vs sell, institutional breakdown) |
| `GET` | `/api/v1/deals/status` | Operational health and sync checkpoints |
| `POST` | `/api/v1/deals/sync-now` | Immediate background sync cycle (Admin only) |
| `POST` | `/api/v1/deals/upload-csv` | Manual CSV upload with validation and dry-run preview |

---

## 6. Frontend Route & Navigation

* **Route:** `/deals` (Protected, requires `view_deals` or `view_stocks` permission)
* **Components:**
  * Summary Cards: Total Deals, Turnover (₹ Cr), Buy Action, Sell Action
  * Tab Selector: All Deals, Bulk Deals (>0.5%), Block Window Deals
  * Filters: Exchange (NSE/BSE), Action (Buy/Sell), Investor Category, Date Range
  * Search Bar: Company name, symbol, ISIN, client name
  * Sortable Table: Trade Date, Type, Exch, Company, Client, Class, Action, Qty, Price, Total Value, % Equity
  * Modal: Operational Health Diagnostics & Ingestion Ledger
  * Modal: Manual CSV Import with Dry-Run Preview

---

## 7. Production Deployment Checklist (Ubuntu Server / OCI)

Follow these steps when ready to deploy to the live server:

1. **Pull Latest Code:**
   ```bash
   cd /home/ubuntu/mf-app
   git pull origin main
   ```

2. **Apply Database Migration:**
   ```bash
   .venv/bin/python -m src.scripts.apply_sql_file database/migrations/034_bulk_block_deals.sql
   ```

3. **Install Systemd Units for Background Automation:**
   ```bash
   sudo cp deployment/mf-deals.service /etc/systemd/system/
   sudo cp deployment/mf-deals.timer /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now mf-deals.timer
   ```

4. **Verify Timer Status:**
   ```bash
   systemctl list-timers | grep mf-deals
   ```

5. **Build and Deploy Frontend:**
   ```bash
   cd frontend
   npm run build
   ```

6. **Rollback Guidance (If Required):**
   * Schema changes are additive. To disable or rollback:
     ```bash
     sudo systemctl stop mf-deals.timer
     sudo systemctl disable mf-deals.timer
     ```
   * The database tables can be left in place or dropped cleanly via:
     ```sql
     DROP TABLE IF EXISTS market_deals, market_deal_ingestion_runs, deal_sync_state CASCADE;
     ```
