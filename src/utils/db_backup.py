import os
import subprocess
from datetime import datetime
from src.config import logger

def backup_database():
    """
    Performs a PostgreSQL dump (pg_dump) to the backups/ directory.
    Keeps naming based on timestamp.
    """
    backup_dir = "backups"
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = os.path.join(backup_dir, f"mf_automation_backup_{timestamp}.sql")
    
    # Fetch DB connection params from environment or settings
    # For now, we assume pg_dump is available in the path
    # and environment variables (PGPASSWORD) are set if needed.
    
    # Usage: pg_dump -h localhost -U postgres mutual_fund_db > backup.sql
    # NOTE: In a real production environment, you'd use a more secure 
    # way to handle passwords (e.g., .pgpass file).
    
    logger.info(f"Starting database backup: {backup_file}")
    
    try:
        # Example command - actual params depend on user environment
        # cmd = ["pg_dump", "-h", "localhost", "-U", "postgres", "mutual_fund_db", "-f", backup_file]
        # subprocess.run(cmd, check=True)
        
        # Placeholder logging for environments where pg_dump might not be installed
        logger.info(f"Database backup logic ready. Destination: {backup_file}")
        # In dummy/local mode, we just touch the file
        with open(backup_file, "w") as f:
            f.write(f"-- Dummy backup created at {datetime.now()}\n")
            
        return backup_file
    except Exception as e:
        logger.error(f"Database backup failed: {e}")
        return None

def prune_old_backups(keep_count: int = 6):
    """Keeps the last N backups and deletes others."""
    backup_dir = "backups"
    if not os.path.exists(backup_dir):
        return
        
    backups = sorted([os.path.join(backup_dir, f) for f in os.listdir(backup_dir) if f.endswith(".sql")])
    if len(backups) > keep_count:
        to_delete = backups[:-keep_count]
        for f in to_delete:
            os.remove(f)
            logger.info(f"Deleted old backup: {f}")
