"""
Weekly Fund Manager Synchronization Scheduler Job.

Executes once weekly (or on demand) to update mutual fund manager identities,
tenures, and assignments from official AMFI Scheme Summary Document (SSD) feeds.
Prevents overlapping runs, applies bounded retries with backoff, and alerts via Telegram.
"""

import sys
import time
import argparse
from datetime import datetime, timezone

from src.config import logger
from src.services.fund_manager_sync import FundManagerSyncService
from src.alerts.telegram_notifier import get_notifier

MAX_RETRIES = 3
INITIAL_BACKOFF_SECONDS = 5


def run_weekly_sync(dry_run: bool = False) -> bool:
    """
    Executes the synchronization job with retry handling and alerting.
    """
    notifier = get_notifier()
    logger.info("⏰ Starting scheduled Fund Manager synchronization job...")
    service = FundManagerSyncService()

    success = False
    result = None
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"Sync attempt {attempt}/{MAX_RETRIES} (dry_run={dry_run})...")
            result = service.sync(dry_run=dry_run)
            success = True
            break
        except Exception as e:
            last_error = e
            logger.warning(f"Sync attempt {attempt} failed: {e}")
            if attempt < MAX_RETRIES:
                sleep_sec = INITIAL_BACKOFF_SECONDS * (2 ** (attempt - 1))
                logger.info(f"Retrying in {sleep_sec}s...")
                time.sleep(sleep_sec)

    if success and result:
        matched = result.get("matched_schemes", 0)
        total = result.get("total_local_schemes", 0)
        pct = result.get("coverage_pct", 0)
        dur = result.get("duration_seconds", 0)
        additions = result.get("manager_additions", 0)

        msg = (
            f"✅ <b>Mutual Fund Manager Sync Success</b>\n"
            f"Coverage: <b>{matched}/{total}</b> ({pct}%)\n"
            f"New Assignments: {additions}\n"
            f"Duration: {dur:.1f}s\n"
            f"Mode: {'Dry Run' if dry_run else 'Production'}"
        )
        logger.info(msg.replace("<b>", "").replace("</b>", ""))
        try:
            notifier.alert(msg)
        except Exception:
            pass
        return True
    else:
        err_msg = (
            f"🚨 <b>Mutual Fund Manager Sync FAILED</b>\n"
            f"Attempts: {MAX_RETRIES}\n"
            f"Error: <code>{str(last_error)[:200]}</code>\n"
            f"Timestamp: {datetime.now(timezone.utc).isoformat()}"
        )
        logger.error(f"Sync job failed completely: {last_error}")
        try:
            notifier.alert(err_msg)
        except Exception:
            pass
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weekly Mutual Fund Manager Synchronization")
    parser.add_argument("--dry-run", action="store_true", help="Execute in dry-run mode without committing to database")
    args = parser.parse_args()

    ok = run_weekly_sync(dry_run=args.dry_run)
    sys.exit(0 if ok else 1)
