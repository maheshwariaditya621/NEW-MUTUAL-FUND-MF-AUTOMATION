"""
Axis Scheduler Module.

Runs Axis backfill in TWO STAGES at fixed times:
- 07:00 AM
- 03:00 PM (15:00)
- 11:45 PM (23:45)

STAGE 1: Scheduled range backfill (optional, if configured)
STAGE 2: Auto eligible month check (always)

Pure Python, cross-platform, no external schedulers.
"""

import time
from datetime import datetime, time as dt_time
from typing import Optional, Tuple, Dict
from src.scheduler.axis_backfill import run_axis_backfill
from src.config import logger
from src.alerts.telegram_notifier import get_notifier


# ============================================================================
# SCHEDULED RANGE CONFIGURATION (OPTIONAL)
# ============================================================================
# Format: {"start": (year, month), "end": (year, month)}
# If None or incomplete, scheduled range backfill is skipped
# ============================================================================

SCHEDULE_RANGE: Optional[Dict] = None

# To disable scheduled range, set to None:
# SCHEDULE_RANGE = None

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


def has_scheduled_range_config() -> bool:
    """
    Check if scheduled range is configured.
    
    Returns:
        True if SCHEDULE_RANGE is properly configured, False otherwise
    """
    if SCHEDULE_RANGE is None:
        return False
    
    if not isinstance(SCHEDULE_RANGE, dict):
        return False
    
    if "start" not in SCHEDULE_RANGE or "end" not in SCHEDULE_RANGE:
        return False
    
    start = SCHEDULE_RANGE["start"]
    end = SCHEDULE_RANGE["end"]
    
    if not (isinstance(start, tuple) and len(start) == 2):
        return False
    
    if not (isinstance(end, tuple) and len(end) == 2):
        return False
    
    return True


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
        time.sleep(wait_seconds)


def run_two_stage_backfill() -> dict:
    """
    Run two-stage backfill.
    
    STAGE 1 (Optional): Scheduled range backfill
    STAGE 2 (Always): Auto eligible month check
    
    Returns:
        Summary dictionary with results from both stages
    """
    logger.info("=" * 70)
    logger.info("TWO-STAGE BACKFILL EXECUTION")
    logger.info("=" * 70)
    
    stage1_result = None
    stage2_result = None
    
    # STAGE 1: Scheduled range backfill (if configured)
    if has_scheduled_range_config():
        start_year, start_month = SCHEDULE_RANGE["start"]
        end_year, end_month = SCHEDULE_RANGE["end"]
        
        logger.info("🔁 STAGE 1: SCHEDULED RANGE BACKFILL")
        logger.info(f"Range: {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
        
        try:
            stage1_result = run_axis_backfill(
                start_year=start_year,
                start_month=start_month,
                end_year=end_year,
                end_month=end_month
            )
            
            if stage1_result["downloaded"] > 0:
                logger.success(f"Stage 1: Downloaded {stage1_result['downloaded']} month(s)")
            elif stage1_result["skipped"] > 0:
                logger.info(f"Stage 1: All {stage1_result['skipped']} month(s) already complete")
            
        except Exception as e:
            logger.error(f"Stage 1 failed: {str(e)}")
    else:
        logger.info("🔁 STAGE 1: SKIPPED (no scheduled range configured)")
    
    logger.info("-" * 70)
    
    # STAGE 2: Auto eligible month check (always)
    logger.info("🔄 STAGE 2: AUTO ELIGIBLE MONTH CHECK")
    
    try:
        stage2_result = run_axis_backfill()  # No arguments = auto mode
        
        if stage2_result["downloaded"] > 0:
            logger.success(f"Stage 2: Downloaded latest month")
        elif stage2_result["skipped"] > 0:
            logger.info(f"Stage 2: Latest month already complete")
        elif stage2_result["failed"] > 0:
            logger.warning(f"Stage 2: No data available yet for latest month")
        
    except Exception as e:
        logger.error(f"Stage 2 failed: {str(e)}")
    
    logger.info("=" * 70)
    
    # Combined summary
    total_downloaded = 0
    total_failed = 0
    
    if stage1_result:
        total_downloaded += stage1_result["downloaded"]
        total_failed += stage1_result["failed"]
    
    if stage2_result:
        total_downloaded += stage2_result["downloaded"]
        total_failed += stage2_result["failed"]
    
    return {
        "stage1": stage1_result,
        "stage2": stage2_result,
        "total_downloaded": total_downloaded,
        "total_failed": total_failed
    }


def run_scheduler():
    """
    Run Axis scheduler.
    
    Infinite loop that runs two-stage backfill at scheduled times.
    Waits for exact scheduled time before executing.
    """
    logger.info("=" * 70)
    logger.info("AXIS TWO-STAGE SCHEDULER STARTED")
    logger.info("=" * 70)
    logger.info("Schedule:")
    for schedule_time in SCHEDULE_TIMES:
        logger.info(f"  - {schedule_time.strftime('%H:%M')}")
    logger.info(f"Check interval: {SLEEP_INTERVAL} seconds")
    logger.info("")
    logger.info("Execution stages:")
    logger.info("  Stage 1: Scheduled range backfill (if configured)")
    logger.info("  Stage 2: Auto eligible month check (always)")
    logger.info("")
    
    if has_scheduled_range_config():
        start_year, start_month = SCHEDULE_RANGE["start"]
        end_year, end_month = SCHEDULE_RANGE["end"]
        logger.info(f"Scheduled range: {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
    else:
        logger.info("Scheduled range: NOT CONFIGURED (Stage 1 will be skipped)")
    
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
                            # Run two-stage backfill
                            result = run_two_stage_backfill()
                            
                            # Mark as executed
                            state.mark_executed(now, scheduled_hour)
                            
                            # Log final summary
                            logger.info("=" * 70)
                            logger.info("SCHEDULED RUN SUMMARY")
                            logger.info("=" * 70)
                            
                            if result["total_downloaded"] > 0:
                                logger.success(f"✅ Downloaded {result['total_downloaded']} month(s) total")
                            
                            if result["total_failed"] > 0:
                                logger.warning(f"⚠️  {result['total_failed']} month(s) failed (data may not be available yet)")
                            
                            if result["total_downloaded"] == 0 and result["total_failed"] == 0:
                                logger.info("ℹ️  All months up to date")
                            
                            # Emit scheduler summary alert
                            notifier = get_notifier()
                            if result["total_downloaded"] > 0 or result["total_failed"] > 0:
                                notifier.notify_scheduler(
                                    event="Axis Scheduled Run Complete",
                                    message=f"AMC: Axis | Mode: AUTO | Time: {now.strftime('%H:%M')}",
                                    stats={
                                        "downloaded": result["total_downloaded"],
                                        "failed": result["total_failed"]
                                    }
                                )
                        
                        except Exception as e:
                            logger.error(f"❌ Scheduled run failed: {str(e)}")
                            
                            # Emit error event
                            notifier = get_notifier()
                            notifier.notify_scheduler(
                                event="Scheduled Run Failed",
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
