import time
from datetime import datetime, time as dt_time, timedelta
from typing import Optional, Tuple, Dict
from src.scheduler.ppfas_backfill import run_ppfas_backfill
from src.config import logger
from src.alerts.telegram_notifier import get_notifier

SCHEDULE_RANGE: Optional[Dict] = {
    "start": (2025, 1),
    "end": (2026, 1)
}

SCHEDULE_TIMES = [
    dt_time(7, 30),  # Slightly offset from HDFC
    dt_time(15, 30),
    dt_time(23, 50)
]

SLEEP_INTERVAL = 60

class SchedulerState:
    def __init__(self):
        self.last_execution: Optional[Tuple[str, int]] = None
    
    def should_run(self, current_time: datetime, scheduled_hour: int) -> bool:
        current_date = current_time.strftime("%Y-%m-%d")
        if current_time.hour != scheduled_hour:
            return False
        if self.last_execution is not None:
            last_date, last_hour = self.last_execution
            if last_date == current_date and last_hour == scheduled_hour:
                return False
        return True
    
    def mark_executed(self, execution_time: datetime, scheduled_hour: int):
        self.last_execution = (execution_time.strftime("%Y-%m-%d"), scheduled_hour)

def run_ppfas_scheduler():
    logger.info("=" * 70)
    logger.info("PPFAS SCHEDULER STARTED")
    logger.info("=" * 70)
    
    state = SchedulerState()
    notifier = get_notifier()
    
    while True:
        try:
            now = datetime.now()
            for schedule_time in SCHEDULE_TIMES:
                if now.hour == schedule_time.hour and now.minute == schedule_time.minute:
                    if state.should_run(now, now.hour):
                        logger.info(f"⏰ PPFAS RUN TRIGGERED - {now}")
                        
                        # Stage 1: Range (if configured)
                        if SCHEDULE_RANGE:
                            run_ppfas_backfill(
                                start_year=SCHEDULE_RANGE["start"][0],
                                start_month=SCHEDULE_RANGE["start"][1],
                                end_year=SCHEDULE_RANGE["end"][0],
                                end_month=SCHEDULE_RANGE["end"][1]
                            )
                        
                        # Stage 2: Auto
                        result = run_ppfas_backfill()
                        state.mark_executed(now, now.hour)
                        
                        if result["downloaded"] > 0:
                            notifier.notify_scheduler("PPFAS Run Complete", f"Downloaded {result['downloaded']} month(s)", result)
            
            time.sleep(SLEEP_INTERVAL)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"PPFAS Scheduler error: {e}")
            time.sleep(SLEEP_INTERVAL)

if __name__ == "__main__":
    run_ppfas_scheduler()
