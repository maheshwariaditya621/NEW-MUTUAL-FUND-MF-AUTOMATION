"""
ICICI Scheduler Module.

Runs ICICI downloader in AUTO MODE ONLY at fixed times.
AUTO MODE = previous calendar month only.

Pure Python, cross-platform, no external schedulers.
"""

import time
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Optional, Tuple
from src.downloaders.icici_downloader import ICICIDownloader
from src.config import logger
from src.alerts.telegram_notifier import get_notifier


# ============================================================================
# SCHEDULE TIMES
# ============================================================================

SCHEDULE_TIMES = [
    dt_time(7, 0),   # 07:00 AM
    dt_time(15, 0),  # 03:00 PM
    dt_time(23, 45)  # 11:45 PM
]

# Sleep interval (seconds)
SLEEP_INTERVAL = 60  # Check every 60 seconds


class SchedulerState:
    """Track last execution to prevent duplicates."""
    
    def __init__(self):
        self.last_execution: Optional[Tuple[str, int]] = None  # (date, hour)
    
    def should_run(self, current_time: datetime, scheduled_hour: int) -> bool:
        """
        Check if job should run.
        
        Args:
            current_time: Current datetime
            scheduled_hour: Scheduled hour (0-23)
            
        Returns:
            True if should run, False if already ran in this window
        """
        current_date = current_time.strftime("%Y-%m-%d")
        current_hour = current_time.hour
        
        # Check if we're in the scheduled hour
        if current_hour != scheduled_hour:
            return False
        
        # Check if already ran in this window
        if self.last_execution is not None:
            last_date, last_hour = self.last_execution
            if last_date == current_date and last_hour == scheduled_hour:
                return False
        
        return True
    
    def mark_executed(self, execution_time: datetime, scheduled_hour: int):
        """
        Mark job as executed.
        
        Args:
            execution_time: Execution datetime
            scheduled_hour: Scheduled hour that was executed
        """
        execution_date = execution_time.strftime("%Y-%m-%d")
        self.last_execution = (execution_date, scheduled_hour)


def get_previous_month() -> Tuple[int, int]:
    """
    Get previous calendar month.
    
    Returns:
        Tuple of (year, month)
    """
    now = datetime.now()
    
    # Calculate previous month
    if now.month == 1:
        prev_year = now.year - 1
        prev_month = 12
    else:
        prev_year = now.year
        prev_month = now.month - 1
    
    return (prev_year, prev_month)


def is_month_complete(year: int, month: int) -> bool:
    """
    Check if month is complete (has _SUCCESS.json marker).
    
    Args:
        year: Year
        month: Month (1-12)
        
    Returns:
        True if _SUCCESS.json exists, False otherwise
    """
    folder_path = Path(f"data/raw/icici/{year}_{month:02d}")
    success_marker = folder_path / "_SUCCESS.json"
    
    return success_marker.exists()


def get_next_scheduled_time(current_time: datetime) -> dt_time:
    """
    Get the next scheduled time after current time.
    
    Args:
        current_time: Current datetime
        
    Returns:
        Next scheduled time
    """
    current_time_only = current_time.time()
    
    for schedule_time in SCHEDULE_TIMES:
        if current_time_only < schedule_time:
            return schedule_time
    
    # If past all times today, return first time tomorrow
    return SCHEDULE_TIMES[0]


def wait_for_scheduled_time(state: SchedulerState):
    """
    Wait until the next scheduled time.
    
    This ensures scheduler doesn't execute immediately on startup.
    
    Args:
        state: Scheduler state
    """
    now = datetime.now()
    next_time = get_next_scheduled_time(now)
    
    # Check if we can run at the next scheduled time
    next_datetime = now.replace(hour=next_time.hour, minute=next_time.minute, second=0, microsecond=0)
    
    # If next time is earlier than now, it's tomorrow
    if next_datetime <= now:
        next_datetime = next_datetime + timedelta(days=1)
    
    wait_seconds = (next_datetime - now).total_seconds()
    
    if wait_seconds > 0:
        logger.info(f"⏰ Waiting for next scheduled time: {next_time.strftime('%H:%M')}")
        logger.info(f"⏰ Will execute in {wait_seconds/60:.1f} minutes")


def run_auto_mode() -> dict:
    """
    Run AUTO MODE: Download previous calendar month only.
    
    Returns:
        Summary dictionary with result
    """
    start_time = time.time()
    
    logger.info("=" * 70)
    logger.info("📥 ICICI AUTO MODE EXECUTION")
    logger.info("=" * 70)
    
    # Get previous month
    year, month = get_previous_month()
    month_name = ICICIDownloader.MONTH_NAMES[month]
    
    logger.info(f"Target: {year}-{month:02d} ({month_name})")
    
    # Check if already complete
    if is_month_complete(year, month):
        duration = time.time() - start_time
        logger.info(f"⏭️  {year}-{month:02d} already downloaded — SKIPPING")
        logger.info(f"🕒 Duration: {duration:.2f}s")
        logger.info("=" * 70)
        
        return {
            "year": year,
            "month": month,
            "status": "skipped",
            "duration": duration
        }
    
    # Attempt download
    logger.info(f"📥 Attempting download for {year}-{month:02d}...")
    
    try:
        downloader = ICICIDownloader()
        result = downloader.download(year=year, month=month)
        
        duration = time.time() - start_time
        
        if result["status"] == "success":
            logger.success(f"✅ SUCCESS | ICICI {year}-{month:02d}")
            logger.success(f"📄 Files downloaded: {result['files_downloaded']}")
        elif result["status"] == "not_published":
            logger.info(f"🚫 NOT PUBLISHED | ICICI {year}-{month:02d}")
            logger.info(f"ℹ️  Data not available from AMC yet (expected)")
        else:
            logger.warning(f"❌ FAILED | ICICI {year}-{month:02d}")
            logger.warning(f"Reason: {result.get('reason', 'Unknown error')}")
        
        logger.info(f"⏱️  Duration: {duration:.2f}s")
        logger.info("=" * 70)
        
        return {
            "year": year,
            "month": month,
            "status": result["status"],
            "files_downloaded": result.get("files_downloaded", 0),
            "duration": duration
        }
    
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"❌ EXCEPTION | ICICI {year}-{month:02d}")
        logger.error(f"Error: {str(e)}")
        logger.info(f"⏱️  Duration: {duration:.2f}s")
        logger.info("=" * 70)
        
        return {
            "year": year,
            "month": month,
            "status": "failed",
            "error": str(e),
            "duration": duration
        }


def run_scheduler():
    """
    Run ICICI scheduler.
    
    Infinite loop that runs AUTO MODE at scheduled times.
    Waits for exact scheduled time before executing.
    """
    logger.info("=" * 70)
    logger.info("📥 ICICI SCHEDULER STARTED")
    logger.info("=" * 70)
    logger.info("Mode: AUTO ONLY (previous calendar month)")
    logger.info("")
    logger.info("Schedule:")
    for schedule_time in SCHEDULE_TIMES:
        logger.info(f"  - {schedule_time.strftime('%H:%M')}")
    logger.info(f"Check interval: {SLEEP_INTERVAL} seconds")
    logger.info("=" * 70)
    
    state = SchedulerState()
    
    # STARTUP GUARD: Wait for next scheduled time
    wait_for_scheduled_time(state)
    
    while True:
        try:
            now = datetime.now()
            current_hour = now.hour
            current_minute = now.minute
            
            # Check each scheduled time
            for schedule_time in SCHEDULE_TIMES:
                scheduled_hour = schedule_time.hour
                scheduled_minute = schedule_time.minute
                
                # Check if we're in the scheduled time window
                if current_hour == scheduled_hour and current_minute == scheduled_minute:
                    if state.should_run(now, scheduled_hour):
                        logger.info("=" * 70)
                        logger.info(f"⏰ SCHEDULED RUN TRIGGERED - {now.strftime('%Y-%m-%d %H:%M:%S')}")
                        logger.info("=" * 70)
                        
                        try:
                            # Run AUTO MODE
                            result = run_auto_mode()
                            
                            # Mark as executed
                            state.mark_executed(now, scheduled_hour)
                            
                            # Emit Telegram notification (if enabled)
                            try:
                                notifier = get_notifier()
                                
                                status_emoji = {
                                    "success": "✅",
                                    "skipped": "⏭️",
                                    "not_published": "🚫",
                                    "failed": "❌"
                                }.get(result["status"], "ℹ️")
                                
                                status_text = result["status"].upper().replace("_", " ")
                                
                                message = (
                                    f"{status_emoji} ICICI {result['year']}-{result['month']:02d}\n"
                                    f"Status: {status_text}\n"
                                    f"Duration: {result['duration']:.2f}s"
                                )
                                
                                if result["status"] == "success":
                                    message += f"\nFiles: {result['files_downloaded']}"
                                
                                notifier.notify_scheduler(
                                    event="ICICI Scheduled Run",
                                    message=message,
                                    stats=result
                                )
                            except Exception as notify_error:
                                # Telegram failures must never crash scheduler
                                logger.warning(f"Telegram notification failed: {str(notify_error)}")
                        
                        except Exception as e:
                            logger.error(f"❌ Scheduled run failed: {str(e)}")
                            
                            # Emit error event
                            try:
                                notifier = get_notifier()
                                notifier.notify_scheduler(
                                    event="ICICI Scheduled Run Failed",
                                    message=f"Error: {str(e)[:100]}",
                                    stats={}
                                )
                            except:
                                pass  # Ignore notification errors
                            
                            # Still mark as executed to prevent retry spam
                            state.mark_executed(now, scheduled_hour)
                        
                        logger.info("=" * 70)
                        next_idx = (SCHEDULE_TIMES.index(schedule_time) + 1) % len(SCHEDULE_TIMES)
                        logger.info(f"⏰ Next scheduled run at: {SCHEDULE_TIMES[next_idx].strftime('%H:%M')}")
                        logger.info("=" * 70)
            
            # Sleep
            time.sleep(SLEEP_INTERVAL)
        
        except KeyboardInterrupt:
            logger.info("=" * 70)
            logger.info("SCHEDULER STOPPED BY USER")
            logger.info("=" * 70)
            break
        
        except Exception as e:
            logger.error(f"Scheduler error: {str(e)}")
            logger.info(f"Retrying in {SLEEP_INTERVAL} seconds...")
            time.sleep(SLEEP_INTERVAL)


if __name__ == "__main__":
    run_scheduler()
