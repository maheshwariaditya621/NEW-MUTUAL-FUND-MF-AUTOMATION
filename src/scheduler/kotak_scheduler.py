# src/scheduler/kotak_scheduler.py

import time
from datetime import datetime, time as dt_time
from src.scheduler.kotak_backfill import run_kotak_backfill
from src.config import logger
from src.alerts.telegram_notifier import get_notifier

SCHEDULE_TIMES = [
    dt_time(8, 30),
    dt_time(16, 30),
    dt_time(0, 10)
]

SLEEP_INTERVAL = 60

class SchedulerState:
    def __init__(self):
        self.last_execution = None
    
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

def run_kotak_scheduler():
    logger.info("=" * 70)
    logger.info("KOTAK SCHEDULER STARTED")
    logger.info("=" * 70)
    
    state = SchedulerState()
    notifier = get_notifier()
    
    while True:
        try:
            now = datetime.now()
            for schedule_time in SCHEDULE_TIMES:
                if now.hour == schedule_time.hour and now.minute == schedule_time.minute:
                    if state.should_run(now, now.hour):
                        logger.info(f"⏰ KOTAK RUN TRIGGERED - {now}")
                        
                        result = run_kotak_backfill()
                        state.mark_executed(now, now.hour)
                        
                        if result["downloaded"] > 0:
                            notifier.notify_scheduler("Kotak Run Complete", f"Downloaded {result['downloaded']} month(s)", result)
            
            time.sleep(SLEEP_INTERVAL)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Kotak Scheduler error: {e}")
            time.sleep(SLEEP_INTERVAL)

if __name__ == "__main__":
    run_kotak_scheduler()
