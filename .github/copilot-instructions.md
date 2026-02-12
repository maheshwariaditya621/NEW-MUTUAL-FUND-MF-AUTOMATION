# Copilot Instructions

## Big Picture
- This repo is a scheduler-driven ingestion pipeline: schedulers/backfills trigger per-AMC downloaders, which save raw files into a canonical folder layout and emit Telegram alerts. See [PROJECT_CONTEXT.md](PROJECT_CONTEXT.md) and [src/scheduler](src/scheduler).
- Downloaders only fetch files and write completion markers; no parsing/validation/loading belongs here. See [src/downloaders/README.md](src/downloaders/README.md).
- Raw data contract is strict: data goes to data/raw/{amc}/{YYYY_MM}/ with a _SUCCESS.json marker; missing markers mean incomplete and should be quarantined to _corrupt. See [PROJECT_CONTEXT.md](PROJECT_CONTEXT.md) and [src/downloaders/hdfc_downloader.py](src/downloaders/hdfc_downloader.py).
- Consolidation is run after successful downloads (or skipped months) via BaseDownloader.consolidate_downloads. See [src/downloaders/base_downloader.py](src/downloaders/base_downloader.py).

## Adding a New AMC (Critical)
- Any mutual fund getting added should follow the same steps as SBI or HDFC mutual fund. This is very important. Use [NEW_AMC_IMPLEMENTATION_GUIDE.md](NEW_AMC_IMPLEMENTATION_GUIDE.md) and mirror [src/downloaders/sbi_downloader.py](src/downloaders/sbi_downloader.py) or [src/downloaders/hdfc_downloader.py](src/downloaders/hdfc_downloader.py).
- Always inherit from BaseDownloader and implement idempotency checks, _SUCCESS marker creation, and corruption recovery. See [src/downloaders/base_downloader.py](src/downloaders/base_downloader.py).
- Prefer API-based downloads (HDFC pattern) and use Playwright only when unavoidable (SBI pattern). See [src/downloaders/hdfc_downloader.py](src/downloaders/hdfc_downloader.py) and [src/downloaders/sbi_downloader.py](src/downloaders/sbi_downloader.py).
- Use Telegram notifier for success/warning/error/not-published events. See [alerts](alerts) and [src/downloaders/sbi_downloader.py](src/downloaders/sbi_downloader.py).

## Developer Workflows
- Install deps: `pip install -r requirements.txt`. See [README.md](README.md).
- Single-month HDFC download: `python -m src.cli.run_hdfc_downloader --year 2025 --month 1`. See [src/cli/run_hdfc_downloader.py](src/cli/run_hdfc_downloader.py).
- HDFC backfill range or auto mode: `python -m src.cli.run_hdfc_bulk_downloader --start-year ... --start-month ... --end-year ... --end-month ...`. See [src/cli/run_hdfc_bulk_downloader.py](src/cli/run_hdfc_bulk_downloader.py).
- Run schedulers directly (example SBI): `python -m src.scheduler.sbi_scheduler`. See [src/scheduler/sbi_scheduler.py](src/scheduler/sbi_scheduler.py).
- Tests use pytest; see [tests/README.md](tests/README.md).

## Config and Integrations
- Environment config is loaded from .env; never hardcode secrets. See [config/README.md](config/README.md) and [config/settings.py](config/settings.py).
- Downloader behavior is controlled by downloader_config (DRY_RUN, retries, headless). See [src/downloaders/hdfc_downloader.py](src/downloaders/hdfc_downloader.py).
- Database schema is defined in [database/schema_v1.0.sql](database/schema_v1.0.sql) and migrations in [database/migrations](database/migrations); avoid editing the locked schema directly.
