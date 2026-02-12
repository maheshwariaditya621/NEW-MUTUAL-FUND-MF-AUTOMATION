"""
Verify HSBC Data in Database.
"""
import sys
import os
import pandas as pd
from pathlib import Path
from sqlalchemy import text

# Add project root to path
sys.path.append(os.getcwd())

from src.db import SessionLocal

def verify_db():
    session = SessionLocal()
    try:
        # 1. Check AMC
        print("\nChecking AMC:")
        amc = session.execute(text("SELECT * FROM amcs WHERE amc_name = 'HSBC Mutual Fund'")).fetchone()
        if amc:
            print(f"✅ Found AMC: {amc}")
            amc_id = amc[0]
        else:
            print("❌ AMC 'HSBC Mutual Fund' NOT FOUND!")
            return

        # 2. Check Schemes
        print("\nChecking Schemes:")
        schemes = session.execute(text(f"SELECT scheme_name, scheme_category FROM schemes WHERE amc_id = {amc_id} ORDER BY scheme_name")).fetchall()
        print(f"Found {len(schemes)} schemes.")
        for s in schemes:
            print(f"  - {s[0]} ({s[1] or 'Unknown'})")
            
        # 3. Check Holdings Count
        print("\nChecking Holdings:")
        count = session.execute(text(f"SELECT COUNT(*) FROM equity_holdings h JOIN schemes s ON h.scheme_id = s.id WHERE s.amc_id = {amc_id}")).scalar()
        print(f"Total Holdings in DB: {count}")
        
        if count == 1501:
            print("✅ Count Matches Extraction (1501)")
        else:
            print(f"❌ Count Mismatch! Expected 1501, got {count}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    verify_db()
