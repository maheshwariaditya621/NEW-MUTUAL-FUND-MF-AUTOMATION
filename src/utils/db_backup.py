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
    
    # Path to pg_dump - Make it platform aware
    import platform
    if platform.system() == "Windows":
        # Check standard installation paths or assume it's in PATH
        pg_dump_path = os.getenv("PG_DUMP_PATH", r"C:\Program Files\PostgreSQL\18\bin\pg_dump.exe")
        if not os.path.exists(pg_dump_path):
            pg_dump_path = "pg_dump.exe" # Fallback to PATH
    else:
        pg_dump_path = "pg_dump" # Standard Linux behavior
    
    # DB Credentials
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_user = os.getenv("DB_USER", "postgres")
    db_name = os.getenv("DB_NAME", "mf_analytics")
    db_password = os.getenv("DB_PASSWORD")

    # Set PGPASSWORD environment variable for this process
    env = os.environ.copy()
    if db_password:
        env["PGPASSWORD"] = db_password
    
    try:
        cmd = [
            pg_dump_path,
            "-h", db_host,
            "-p", str(db_port),
            "-U", db_user,
            "-F", "c", # Custom format (compressed)
            "-f", backup_file,
            db_name
        ]
        
        logger.info(f"Executing: {' '.join(cmd)}")
        subprocess.run(cmd, check=True, env=env)
        
        logger.success(f"Database backup completed: {backup_file}")
        return backup_file
    except subprocess.CalledProcessError as e:
        logger.error(f"pg_dump failed with exit code {e.returncode}")
        return None
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
