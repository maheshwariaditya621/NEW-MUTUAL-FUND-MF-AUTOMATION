"""
Corporate Announcements Background Ingestion Worker.

Runs an automated, drift-free monotonic 60-second polling loop for NSE and BSE feeds,
matching against the Master Office Watchlist, merging duplicates, and downloading PDFs.
"""

from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional
import os
import sys
import time
import signal
import argparse

# Ensure workspace root is in sys.path
sys.path.insert(0, os.path.abspath("."))

from src.config import logger
from src.announcements.watchlist_service import WatchlistService
from src.announcements.nse_collector import NSECollector
from src.announcements.bse_collector import BSECollector
from src.announcements.pdf_manager import PDFManager
from src.alerts.telegram_client import TelegramClient


class AnnouncementWorker:
    """Production worker for continuous corporate announcement ingestion."""

    def __init__(self, cycle_interval_seconds: int = 60):
        self.interval = cycle_interval_seconds
        self.running = False
        self.nse_collector = NSECollector()
        self.bse_collector = BSECollector(max_workers=4)
        self.telegram = TelegramClient()
        self.consecutive_errors = 0
        self.last_purge_date = None

        # Health metrics
        self.health_state = {
            "status": "STOPPED",
            "last_cycle_started_at": None,
            "last_cycle_completed_at": None,
            "last_cycle_duration_seconds": 0.0,
            "total_cycles_run": 0,
            "consecutive_failures": 0,
            "last_error": None,
            "watchlist_size": 0
        }

    def poll_once(self, recovery: bool = False) -> Dict[str, Any]:
        """
        Execute a single unified polling cycle:
        1. Load active Master Office Watchlist
        2. Poll NSE equities feed
        3. Poll BSE scrip feeds
        4. Trigger batch download of pending PDFs
        5. Check and run daily 60-day purge
        """
        cycle_start = time.time()
        start_dt = datetime.now(timezone.utc)
        self.health_state["last_cycle_started_at"] = start_dt.isoformat()

        # 1. Fetch Master Watchlist
        watchlist = WatchlistService.get_active_watchlist()
        self.health_state["watchlist_size"] = len(watchlist)

        if not watchlist:
            logger.info("Master Office Watchlist is empty. Skipping exchange polling.")
            return {
                "status": "SKIPPED",
                "reason": "Empty watchlist",
                "watchlist_count": 0,
                "duration_seconds": time.time() - cycle_start
            }

        nse_result = {}
        bse_result = {}
        pdf_downloaded = 0
        purge_result = {}
        cycle_errors = []

        # 2. Poll NSE
        try:
            nse_result = self.nse_collector.poll(watchlist, recovery=recovery)
        except Exception as e:
            msg = f"NSE polling error: {e}"
            logger.error(msg)
            cycle_errors.append(msg)

        # 3. Poll BSE
        try:
            bse_result = self.bse_collector.poll(watchlist, recovery=recovery)
        except Exception as e:
            msg = f"BSE polling error: {e}"
            logger.error(msg)
            cycle_errors.append(msg)

        # 4. Process pending PDF downloads in small batch
        try:
            pdf_downloaded = PDFManager.process_pending_downloads(batch_size=10)
        except Exception as e:
            logger.warning(f"Error processing pending PDFs: {e}")

        # 5. Run daily purge check (runs once per calendar day UTC)
        today_date = start_dt.date()
        if self.last_purge_date != today_date:
            try:
                purge_result = PDFManager.run_daily_purge()
                self.last_purge_date = today_date
            except Exception as e:
                logger.warning(f"Daily purge error: {e}")

        cycle_duration = time.time() - cycle_start
        self.health_state["last_cycle_completed_at"] = datetime.now(timezone.utc).isoformat()
        self.health_state["last_cycle_duration_seconds"] = round(cycle_duration, 2)
        self.health_state["total_cycles_run"] += 1

        if cycle_errors:
            self.consecutive_errors += 1
            self.health_state["consecutive_failures"] = self.consecutive_errors
            self.health_state["last_error"] = "; ".join(cycle_errors)

            if self.consecutive_errors >= 3:
                self.send_failure_alert("; ".join(cycle_errors))
        else:
            self.consecutive_errors = 0
            self.health_state["consecutive_failures"] = 0
            self.health_state["last_error"] = None

        return {
            "status": "PARTIAL_ERROR" if cycle_errors else "SUCCESS",
            "errors": cycle_errors,
            "watchlist_count": len(watchlist),
            "nse": nse_result,
            "bse": bse_result,
            "pdfs_downloaded": pdf_downloaded,
            "purge": purge_result,
            "duration_seconds": round(cycle_duration, 2)
        }

    def send_failure_alert(self, error_message: str):
        """Send Telegram alert if repeated failures occur."""
        try:
            text = (
                f"🚨 *Corporate Announcements Worker Alert*\n\n"
                f"Consecutive failures: `{self.consecutive_errors}`\n"
                f"Error: {error_message[:400]}\n"
                f"Time: `{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}`"
            )
            self.telegram.send_message(text)
        except Exception as e:
            logger.error(f"Failed to dispatch Telegram alert: {e}")

    def run(self):
        """Run continuous drift-free 60-second loop."""
        self.running = True
        self.health_state["status"] = "RUNNING"
        logger.info(f"Starting Corporate Announcements Worker (interval: {self.interval}s)")

        # Initial startup recovery run
        logger.info("Executing initial startup recovery check...")
        try:
            recovery_summary = self.poll_once(recovery=True)
            logger.info(f"Startup recovery completed in {recovery_summary.get('duration_seconds')}s")
        except Exception as e:
            logger.error(f"Error during startup recovery run: {e}")

        while self.running:
            start_mono = time.monotonic()

            try:
                summary = self.poll_once(recovery=False)
                logger.info(
                    f"Poll cycle finished in {summary.get('duration_seconds')}s: "
                    f"NSE Matched={summary.get('nse', {}).get('matched', 0)}, "
                    f"BSE Matched={summary.get('bse', {}).get('matched', 0)}, "
                    f"PDFs={summary.get('pdfs_downloaded', 0)}"
                )
            except Exception as e:
                logger.error(f"Unhandled exception in poll cycle: {e}")

            if not self.running:
                break

            elapsed = time.monotonic() - start_mono
            sleep_duration = max(0.0, float(self.interval) - elapsed)
            time.sleep(sleep_duration)

        self.health_state["status"] = "STOPPED"
        logger.info("Corporate Announcements Worker stopped cleanly.")

    def stop(self):
        """Signal worker to stop gracefully."""
        self.running = False


def main():
    parser = argparse.ArgumentParser(description="Corporate Announcements Polling Worker")
    parser.add_argument("--once", action="store_true", help="Run a single poll cycle and exit")
    parser.add_argument("--recovery", action="store_true", help="Run with extended recovery window")
    parser.add_argument("--interval", type=int, default=60, help="Polling cycle interval in seconds (default: 60)")
    args = parser.parse_args()

    worker = AnnouncementWorker(cycle_interval_seconds=args.interval)

    if args.once:
        logger.info(f"Executing single cycle (recovery={args.recovery})...")
        res = worker.poll_once(recovery=args.recovery)
        print("Cycle Result:", res)
        sys.exit(0)

    # Signal handling for clean shutdown
    def handle_signal(sig, frame):
        logger.info("Shutdown signal received, stopping worker...")
        worker.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    worker.run()


if __name__ == "__main__":
    main()
