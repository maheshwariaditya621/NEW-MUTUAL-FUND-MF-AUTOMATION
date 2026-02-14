
import os
import subprocess
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

def backup_db():
    load_dotenv()
    
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "mf_analytics")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "").strip('"')
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = Path("backups")
    backup_dir.mkdir(exist_ok=True)
    
    backup_file = backup_dir / f"{db_name}_backup_{timestamp}.sql"
    
    print(f"Starting backup for {db_name} to {backup_file}...")
    
    # Set PGPASSWORD environment variable for pg_dump
    env = os.environ.copy()
    env["PGPASSWORD"] = db_password
    
    try:
        # Use the full path for pg_dump if it's not in the PATH
        pg_dump_path = r"C:\Program Files\PostgreSQL\18\bin\pg_dump.exe"
        if not os.path.exists(pg_dump_path):
            pg_dump_path = "pg_dump" # Fallback to PATH
            
        # Construct the pg_dump command
        cmd = [
            pg_dump_path,
            "-h", db_host,
            "-p", db_port,
            "-U", db_user,
            "-f", str(backup_file),
            db_name
        ]
        
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"Backup successful! File saved to: {backup_file}")
            print(f"Backup size: {os.path.getsize(backup_file)} bytes")
        else:
            print("Backup failed!")
            print(f"Error: {result.stderr}")
            
    except FileNotFoundError:
        print("Error: pg_dump not found. Please ensure PostgreSQL client tools are installed and in your PATH.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    backup_db()
