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

from src.db.connection import SessionLocal

def verify_db():
    session = SessionLocal()
    try:
        # 1. Check AMC
        print("\nChecking AMC:")
        amc = session.execute(text("SELECT * FROM amcs WHERE amc_name = 'HSBC Mutual Fund'")).fetchone()
        if amc:
            print(f"✅ Found AMC: {amc[1]} (ID: {amc[0]})")
            amc_id = amc[0]
        else:
            print("❌ AMC 'HSBC Mutual Fund' NOT FOUND!")
            return

        # 2. Check Schemes
        print("\nChecking Schemes:")
        schemes = session.execute(text(f"SELECT scheme_name, scheme_category, id FROM schemes WHERE amc_id = {amc_id} ORDER BY scheme_name")).fetchall()
        print(f"Found {len(schemes)} schemes.")
        
        # Check for clean names (no 'HSBC' prefix if possible, though HSBC schemes might legally contain it)
        # HDFC schemes are "HDFC Equity Fund", so "HSBC Equity Fund" is standard.
        # But we want to avoid "HSBC MUTUAL FUND HSBC EQUITY FUND" or similar double prefixes.
        for s in schemes:
            name = s[0]
            print(f"  - {name} (ID: {s[2]})")
            
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
        import traceback
        traceback.print_exc()
    finally:
        session.close()

if __name__ == "__main__":
    verify_db()
