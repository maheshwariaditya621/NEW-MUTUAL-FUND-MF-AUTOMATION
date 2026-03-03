"""
test_db_connection.py

Simple script to verify that the local machine can connect to the target database.
Use this to confirm your AWS Security Group allows access from your current IP.
"""

import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.connection import get_connection
from src.config import DB_HOST, DB_NAME, logger

def test_connection():
    logger.info(f"Attempting to connect to {DB_NAME} at {DB_HOST}...")
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()
            logger.success(f"✅ Successfully connected to {DB_HOST}!")
            logger.info(f"DB Version: {version[0]}")
            
            cur.execute("SELECT count(*) FROM amcs;")
            amc_count = cur.fetchone()[0]
            logger.info(f"Connection verified. Found {amc_count} AMCs in database.")
            
    except Exception as e:
        logger.error(f"❌ Connection failed to {DB_HOST}: {e}")
        logger.info("\nTIP: If this is an AWS instance, check if:")
        logger.info("1. Port 5432 is open in the AWS Security Group for your current IP.")
        logger.info("2. The 'pg_hba.conf' on the server allows remote connections.")
        logger.info("3. Your .env file has the correct AWS Public IP.")
        sys.exit(1)

if __name__ == "__main__":
    test_connection()
