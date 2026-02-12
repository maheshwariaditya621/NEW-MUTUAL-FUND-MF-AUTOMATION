from src.utils.db_backup import backup_database
from src.config import logger

if __name__ == "__main__":
    logger.info("Triggering manual backup...")
    backup_file = backup_database()
    if backup_file:
        logger.info(f"Backup created successfully at: {backup_file}")
    else:
        logger.error("Backup failed.")
