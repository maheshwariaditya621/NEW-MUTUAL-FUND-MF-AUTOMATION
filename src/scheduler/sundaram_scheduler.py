# src/scheduler/sundaram_scheduler.py

import time
from datetime import datetime, time as dt_time
from typing import Optional, Tuple, Dict
from src.scheduler.sundaram_backfill import run_sundaram_backfill
from src.config import logger
from src.alerts.telegram_notifier import get_notifier


# ============================================================================
# SCHEDULED RANGE CONFIGURATION (OPTIONAL)
# ============================================================================
SCHEDULE_RANGE = None

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
        current_date = current_time.strftime("%Y-%m-%d")
        current_hour = current_time.hour
        
        if current_hour != scheduled_hour:
            return False
        
        if self.last_execution is not None:
            last_date, last_hour = self.last_execution
            if last_date == current_date and last_hour == scheduled_hour:
                return False
        
        return True
    
    def mark_executed(self, execution_time: datetime, scheduled_hour: int):
        execution_date = execution_time.strftime("%Y-%m-%d")
        self.last_execution = (execution_date, scheduled_hour)


def has_scheduled_range_config() -> bool:
    if SCHEDULE_RANGE is None: return False
    if not isinstance(SCHEDULE_RANGE, dict): return False
    if "start" not in SCHEDULE_RANGE or "end" not in SCHEDULE_RANGE: return False
    return True


def get_next_scheduled_time(current_time: datetime) -> dt_time:
    current_time_only = current_time.time()
    for schedule_time in SCHEDULE_TIMES:
        if current_time_only < schedule_time:
            return schedule_time
    return SCHEDULE_TIMES[0]


def wait_for_scheduled_time(state: SchedulerState):
    now = datetime.now()
    next_time = get_next_scheduled_time(now)
    
    next_datetime = now.replace(hour=next_time.hour, minute=next_time.minute, second=0, microsecond=0)
    
    if next_datetime <= now:
        from datetime import timedelta
        next_datetime = next_datetime + timedelta(days=1)
    
    wait_seconds = (next_datetime - now).total_seconds()
    
    if wait_seconds > 0:
        logger.info(f"Waiting for next scheduled time: {next_time.strftime('%H:%M')}")
        logger.info(f"Will execute in {wait_seconds/60:.1f} minutes")


def run_two_stage_backfill() -> dict:
    logger.info("=" * 70)
    logger.info("SUNDARAM TWO-STAGE BACKFILL EXECUTION")
    logger.info("=" * 70)
    
    stage1_result = None
    stage2_result = None
    
    # STAGE 1: Scheduled range backfill
    if has_scheduled_range_config():
        start_year, start_month = SCHEDULE_RANGE["start"]
        end_year, end_month = SCHEDULE_RANGE["end"]
        logger.info("🔁 STAGE 1: SCHEDULED RANGE BACKFILL")
        try:
            stage1_result = run_sundaram_backfill(
                start_year=start_year,
                start_month=start_month,
                end_year=end_year,
                end_month=end_month
            )
        except Exception as e:
            logger.error(f"Stage 1 failed: {str(e)}")
    
    logger.info("-" * 70)
    
    # STAGE 2: Auto eligible month check (always)
    logger.info("🔄 STAGE 2: AUTO ELIGIBLE MONTH CHECK")
    try:
        stage2_result = run_sundaram_backfill()
    except Exception as e:
        logger.error(f"Stage 2 failed: {str(e)}")
    
    logger.info("=" * 70)
    
    total_downloaded = (stage1_result["downloaded"] if stage1_result else 0) + (stage2_result["downloaded"] if stage2_result else 0)
    total_failed = (stage1_result["failed"] if stage1_result else 0) + (stage2_result["failed"] if stage2_result else 0)
    
    return {
        "total_downloaded": total_downloaded,
        "total_failed": total_failed
    }


def run_scheduler():
    logger.info("=" * 70)
    logger.info("SUNDARAM MF SCHEDULER STARTED")
    logger.info("=" * 70)
    for schedule_time in SCHEDULE_TIMES:
        logger.info(f"  - {schedule_time.strftime('%H:%M')}")
    logger.info("=" * 70)
    
    state = SchedulerState()
    wait_for_scheduled_time(state)
    
    while True:
        try:
            now = datetime.now()
            current_hour = now.hour
            current_minute = now.minute
            
            for schedule_time in SCHEDULE_TIMES:
                if current_hour == schedule_time.hour and current_minute == schedule_time.minute:
                    if state.should_run(now, schedule_time.hour):
                        logger.info("=" * 70)
                        logger.info(f"⏰ SCHEDULED RUN TRIGGERED - {now.strftime('%Y-%m-%d %H:%M:%S')}")
                        logger.info("=" * 70)
                        
                        try:
                            result = run_two_stage_backfill()
                            state.mark_executed(now, schedule_time.hour)
                            
                            logger.info("=" * 70)
                            logger.info("SCHEDULED RUN SUMMARY")
                            logger.info("=" * 70)
                            
                            if result["total_downloaded"] > 0:
                                logger.success(f"✅ Downloaded {result['total_downloaded']} month(s) total")
                            if result["total_failed"] > 0:
                                logger.warning(f"⚠️  {result['total_failed']} month(s) failed")
                            
                            notifier = get_notifier()
                            if result["total_downloaded"] > 0 or result["total_failed"] > 0:
                                notifier.notify_scheduler(
                                    event="Scheduled Run Complete",
                                    message=f"Sundaram completed at {now.strftime('%H:%M')}",
                                    stats={
                                        "downloaded": result["total_downloaded"],
                                        "failed": result["total_failed"]
                                    }
                                )
                        except Exception as e:
                            logger.error(f"❌ Scheduled run failed: {str(e)}")
                            notifier = get_notifier()
                            notifier.notify_scheduler(
                                event="Scheduled Run Failed",
                                message=f"Sundaram Error: {str(e)[:100]}",
                                stats={}
                            )
                            state.mark_executed(now, schedule_time.hour)
            
            time.sleep(SLEEP_INTERVAL)
        except KeyboardInterrupt:
            logger.info("SCHEDULER STOPPED BY USER")
            break
        except Exception as e:
            logger.error(f"Scheduler error: {str(e)}")
            time.sleep(SLEEP_INTERVAL)


if __name__ == "__main__":
    run_scheduler()
