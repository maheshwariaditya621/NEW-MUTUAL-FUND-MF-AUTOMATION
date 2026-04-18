# Mutual Fund Automation: Core Components Guide

This guide explains the two critical background systems: the **Corporate Action Finder** and the **Outstanding Share Finder**.

---

## 1. Corporate Action Finder (Auto-Adjustment System)

### Overview
This component ensures historical consistency of mutual fund holdings. When a stock undergoes a corporate action (like a **Stock Split** or **Bonus Issue**), the number of shares increases but the total value remains stable. Without this system, your historical portfolio data would show incorrect quantity drops or "accidental" growth.

### How it Works
1.  **Fetcher (`NSEFetcher` / `BSEFetcher`)**: Crawls exchange APIs to find announcements of splits, bonuses, and name changes.
2.  **Classification**: Parses announcement text using regex to extract **Ratio Factors** (e.g., 10:1 split = factor of 10).
3.  **Reprocessing Queue**: Quantity-affecting actions are stored in a `reprocessing_queue`.
4.  **Adjustment Engine**: Iterates through historical data and calculates `adj_quantity = raw_quantity * factor`. This ensures that 1 share held 5 years ago is shown as 10 shares today if a 10:1 split occurred.

### How to Run
#### **A. Start Daily Scheduler**
This starts a long-running process that checks for new corporate actions twice daily (08:15 and 21:00 IST) and applies them.
```bash
.venv\Scripts\python -m src.corporate_actions.ca_scheduler
```

#### **B. Manual Fetch (One-off)**
Run this to fetch corporate actions right now without waiting for the scheduler.
```bash
.venv\Scripts\python -c "from src.corporate_actions.ca_scheduler import run_ca_fetch; run_ca_fetch()"
```

#### **C. Drain Reprocessing Queue**
If you have pending actions in the database and want to force-apply them to your history:
```bash
.venv\Scripts\python -c "from src.corporate_actions.reprocessing_worker import ReprocessingWorker; w=ReprocessingWorker(); print(w.drain_queue())"
```

---

## 2. Outstanding Share Finder (Fundamental Sync)

### Overview
Crucial for calculating **Market Capitalization** and **Ownership Percentage** (how much of a company's total shares are held by a specific mutual fund scheme).

### How it Works
1.  **Shares Service (`SharesService`)**: Connects to the `yfinance` API.
2.  **Ticker Resolution**: Maps internal symbols (e.g., `RELIANCE`) to exchange-compatible tickers (`RELIANCE.NS`).
3.  **Sync Logic**: Iterates through the `companies` table and updates `shares_outstanding` for everyone.
4.  **Rate Limiting**: Built-in protection against 429 errors (Too Many Requests) with exponential backoff and localized concurrency.

### How to Run
#### **A. Full Update Sync**
This will sync all companies that haven't been updated in the last 24 hours.
```bash
.venv\Scripts\python -c "from src.services.shares_service import shares_service; shares_service.sync_all_shares()"
```

#### **B. Check Sync Status (SQL)**
You can monitor the sync progress directly in your database:
```sql
SELECT count(*) FROM companies WHERE shares_outstanding IS NOT NULL;
SELECT company_name, shares_outstanding, shares_last_updated_at FROM companies ORDER BY shares_last_updated_at DESC LIMIT 10;
```

---

## Technical Details

### Key Database Tables
| Table | Description |
| :--- | :--- |
| `corporate_actions` | Stores raw split/bonus data, ratios, and apply status. |
| `reprocessing_queue` | Tasks for the worker to recalculate ISIN histories. |
| `adjustment_factors` | Cached cumulative factors for every ISIN. |
| `companies` | Contains `shares_outstanding` and sync timestamps. |

### Logs
All activities are logged to the console and your configured log files. Look for `[NSEFetcher]`, `[AdjustmentEngine]`, or `SharesService` in the logs to debug issues.
