import sys
import os
import time
from datetime import datetime
import pandas as pd
import yfinance as yf
import psycopg2
from typing import List, Dict, Any

# Ensure we can import from src
sys.path.append(os.getcwd())
from src.config.settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

def update_market_caps():
    """
    Fetches market cap for all companies using yfinance and updates DB.
    Categorizes into Large, Mid, and Small Cap based on rank.
    """
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()
    
    try:
        # 1. Fetch all companies with symbols
        print("Fetching companies from database...")
        cur.execute("SELECT company_id, company_name, nse_symbol, bse_code FROM companies")
        companies = cur.fetchall()
        print(f"Total companies in DB: {len(companies)}")
        
        mcap_data = [] # List of {company_id, market_cap, shares}
        
        # 2. Fetch Market Cap from yfinance
        print("\nFetching market caps from Yahoo Finance...")
        for comp_id, name, nse, bse in companies:
            ticker_symbol = None
            if nse:
                ticker_symbol = f"{nse}.NS"
            elif bse:
                ticker_symbol = f"{bse}.BO"
            
            if not ticker_symbol:
                continue
                
            try:
                ticker = yf.Ticker(ticker_symbol)
                # We use fast_info if available or info
                mcap = ticker.info.get("marketCap")
                shares = ticker.info.get("sharesOutstanding")
                
                if mcap:
                    mcap_data.append({
                        "id": comp_id,
                        "name": name,
                        "mcap": mcap,
                        "shares": shares
                    })
                    print(f"  [OK] {name}: ₹{mcap/10000000:,.2f} Cr")
                    
                    # Update DB immediately for mcap and shares
                    update_timestamp = datetime.now()
                    cur.execute(
                        """
                        UPDATE companies 
                        SET market_cap = %s, 
                            mcap_updated_at = %s,
                            shares_outstanding = %s,
                            shares_last_updated_at = %s
                        WHERE company_id = %s
                        """,
                        (
                            int(mcap), 
                            update_timestamp, 
                            int(shares) if shares else None,
                            update_timestamp if shares else None,
                            comp_id
                        )
                    )
                    conn.commit()
                else:
                    print(f"  [MISSING] {name} ({ticker_symbol})")
            except Exception as e:
                print(f"  [ERROR] {name} ({ticker_symbol}): {e}")
            
            # Simple rate limiting
            time.sleep(0.1)

        if not mcap_data:
            print("No market cap data fetched. Exiting.")
            return

        # 3. Categorize by Rank
        # Sort by mcap descending
        df = pd.DataFrame(mcap_data)
        df = df.sort_values(by="mcap", ascending=False).reset_index(drop=True)
        
        # Large Cap (1-100), Mid Cap (101-250), Small Cap (251+)
        def get_type(rank):
            if rank <= 100: return "Large Cap"
            if rank <= 250: return "Mid Cap"
            return "Small Cap"
            
        df['mcap_type'] = (df.index + 1).map(get_type)
        
        print(f"\nUpdating {len(df)} companies in database...")
        
        # 4. Final Update for MCAP Types
        print(f"\nUpdating cap types for {len(df)} companies...")
        for _, row in df.iterrows():
            cur.execute(
                "UPDATE companies SET mcap_type = %s WHERE company_id = %s",
                (row['mcap_type'], row['id'])
            )
        
        conn.commit()
        print("\nSuccessfully updated all data.")
        
    except Exception as e:
        conn.rollback()
        print(f"Critical error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    update_market_caps()
