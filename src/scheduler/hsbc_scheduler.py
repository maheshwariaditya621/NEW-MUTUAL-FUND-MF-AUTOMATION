"""
HSBC Scheduler Module.

Runs HSBC backfill at fixed times:
- 07:00 AM
- 03:00 PM (15:00)
- 11:45 PM (23:45)

AUTO MODE ONLY: Downloads latest eligible month only.
Pure Python, cross-platform, no external schedulers.
"""

import time
from datetime import datetime, time as dt_time
from typing import Optional, Tuple
from src.scheduler.hsbc_backfill import run_hsbc_backfill
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
        # Add one day
        from datetime import timedelta
        next_datetime = next_datetime + timedelta(days=1)
    
    wait_seconds = (next_datetime - now).total_seconds()
    
    if wait_seconds > 0:
        logger.info(f"Waiting for next scheduled time: {next_time.strftime('%H:%M')}")
        logger.info(f"Will execute in {wait_seconds/60:.1f} minutes")


def run_scheduler():
    """
    Run HSBC scheduler.
    
    Infinite loop that runs HSBC backfill (auto mode) at scheduled times.
    Waits for exact scheduled time before executing.
    """
    logger.info("=" * 70)
    logger.info("HSBC SCHEDULER STARTED")
    logger.info("=" * 70)
    logger.info("Schedule:")
    for schedule_time in SCHEDULE_TIMES:
        logger.info(f"  - {schedule_time.strftime('%H:%M')}")
    logger.info(f"Check interval: {SLEEP_INTERVAL} seconds")
    logger.info("")
    logger.info("Mode: AUTO (latest eligible month only)")
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
                            # Run backfill in AUTO MODE (no arguments)
                            result = run_hsbc_backfill()
                            
                            # Mark as executed
                            state.mark_executed(now, scheduled_hour)
                            
                            # Log final summary
                            logger.info("=" * 70)
                            logger.info("SCHEDULED RUN SUMMARY")
                            logger.info("=" * 70)
                            
                            if result["downloaded"] > 0:
                                logger.success(f"✅ Downloaded {result['downloaded']} month(s)")
                                
                                # Send Telegram notification only if downloaded
                                notifier = get_notifier()
                                notifier.notify_scheduler(
                                    event="HSBC Scheduled Run Complete",
                                    message=f"AMC: HSBC | Time: {now.strftime('%H:%M')}",
                                    stats={
                                        "downloaded": result["downloaded"],
                                        "skipped": result["skipped"],
                                        "failed": result["failed"]
                                    }
                                )
                            
                            elif result["skipped"] > 0:
                                logger.info("ℹ️  Latest month already complete")
                            
                            else:
                                # No downloads, no skips (likely not_published)
                                logger.info("ℹ️  No new data available")
                        
                        except Exception as e:
                            logger.error(f"❌ Scheduled run failed: {str(e)}")
                            
                            # Emit error event
                            notifier = get_notifier()
                            notifier.notify_scheduler(
                                event="HSBC Scheduled Run Failed",
                                message=f"Error: {str(e)[:100]}",
                                stats={}
                            )
                            
                            # Still mark as executed to prevent retry spam
                            state.mark_executed(now, scheduled_hour)
                        
                        logger.info("=" * 70)
                        next_idx = (SCHEDULE_TIMES.index(schedule_time) + 1) % len(SCHEDULE_TIMES)
                        logger.info(f"Next scheduled run at: {SCHEDULE_TIMES[next_idx].strftime('%H:%M')}")
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
