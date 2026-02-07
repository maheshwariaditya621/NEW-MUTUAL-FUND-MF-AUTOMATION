# src/scheduler/bandhan_scheduler.py

import time
from datetime import datetime
from src.scheduler.bandhan_backfill import run_bandhan_backfill
from src.config import logger
from src.alerts.telegram_notifier import get_notifier

SCHEDULE_TIMES = ["09:15", "17:30", "01:30"]

def run_scheduler():
    logger.info("Bandhan Scheduler Service Started")
    logger.info(f"Targeting times: {SCHEDULE_TIMES}")
    
    while True:
        now = datetime.now()
        current_time = now.strftime("%H:%M")
        
        if current_time in SCHEDULE_TIMES:
            logger.info(f"Scheduled time reached: {current_time}. Triggering backfill...")
            try:
                run_bandhan_backfill()
            except Exception as e:
                logger.error(f"Scheduler backfill failed: {e}")
                get_notifier().notify_error("BANDHAN", now.year, now.month, "Scheduler Failure", str(e)[:100])
            
            logger.info("Sleeping for 61 seconds to avoid double triggers...")
            time.sleep(61)
            
        time.sleep(30)

if __name__ == "__main__":
    run_scheduler()
