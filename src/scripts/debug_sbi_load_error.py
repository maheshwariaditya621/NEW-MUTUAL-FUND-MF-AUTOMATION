from src.extractors.extractor_factory import ExtractorFactory
from src.loaders.portfolio_loader import PortfolioLoader
from src.db.connection import get_cursor, close_connection, get_connection
from src.db.repositories import upsert_amc, upsert_period, delete_extraction_run_and_holdings, upsert_isin_master, upsert_company
from src.config import logger
import traceback

def debug_load():
    try:
        # 1. Setup
        amc = "sbi"
        year = 2025
        month = 12
        file_path = f"data/output/merged excels/{amc}/{year}/CONSOLIDATED_{amc.upper()}_{year}_{month}.xlsx"
        
        print(f"DEBUG: Extracting {file_path}")
        extractor = ExtractorFactory.get_extractor(amc, year, month)
        holdings = extractor.extract(file_path)
        print(f"DEBUG: Extracted {len(holdings)} holdings.")
        
        # 2. Manual DB Ops (No Context Manager to see error)
        conn = get_connection()
        conn.autocommit = False # Manual align
        
        try:
            # Re-create helpers since imported ones use get_cursor() which makes new conn
            # We need to share ONE connection/cursor for transaction
            cursor = conn.cursor()
            
            # Upsert AMC
            cursor.execute("INSERT INTO amcs (amc_name) VALUES (%s) ON CONFLICT (amc_name) DO UPDATE SET amc_name = EXCLUDED.amc_name RETURNING amc_id", (extractor.amc_name,))
            amc_id = cursor.fetchone()[0]
            print(f"AMC ID: {amc_id}")
            
            # Upsert Period
            from datetime import date, timedelta
            next_month = date(year, month, 1) + timedelta(days=32)
            period_end = next_month.replace(day=1) - timedelta(days=1)
            cursor.execute("INSERT INTO periods (year, month, period_end_date) VALUES (%s, %s, %s) ON CONFLICT (year, month) DO UPDATE SET period_end_date = EXCLUDED.period_end_date RETURNING period_id", (year, month, period_end))
            period_id = cursor.fetchone()[0]
            print(f"Period ID: {period_id}")
            
            print("grouping holdings by scheme...")
            schemes_data = {}
            for h in holdings:
                s_key = (h['scheme_name'], h['plan_type'], h['option_type'])
                if s_key not in schemes_data:
                    schemes_data[s_key] = []
                schemes_data[s_key].append(h)
                
            print(f"Loading {len(schemes_data)} schemes...")
            
            # Start Global Transaction for all schemes
            for s_key, items in schemes_data.items():
                print(f"Loading Scheme: {s_key}")
                try:
                    # 1. Scheme
                    h_sample = items[0]
                    cursor.execute("""
                        INSERT INTO schemes (amc_id, scheme_name, plan_type, option_type, is_reinvest)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (amc_id, scheme_name, plan_type, option_type, is_reinvest) 
                        DO UPDATE SET updated_at = CURRENT_TIMESTAMP
                        RETURNING scheme_id
                    """, (amc_id, h_sample['scheme_name'], h_sample['plan_type'], h_sample['option_type'], h_sample.get('is_reinvest', False)))
                    scheme_id = cursor.fetchone()[0]
                    
                    # 2. Companies & ISINs
                    db_holdings = []
                    for h in items:
                        upsert_isin_master(
                            isin=h['isin'], 
                            canonical_name=h['company_name'], 
                            sector=h.get('sector'), 
                            industry=h.get('industry')
                        )
                        try:
                            cid = upsert_company(
                                isin=h['isin'], 
                                company_name=h['company_name'], 
                                sector=h.get('sector'), 
                                industry=h.get('industry')
                            )
                        except Exception as ce:
                            print(f"[ERROR] Company insert failed for {h}: {ce}")
                            raise ce
                            
                        db_holdings.append({
                            "company_id": cid,
                            "quantity": h['quantity'],
                            "market_value_inr": h['market_value_inr'],
                            "percent_of_nav": h['percent_to_nav']
                        })
                        
                    # 3. Snapshot
                    # Check exists first to avoid dupes in this debug run
                    cursor.execute("SELECT snapshot_id FROM scheme_snapshots WHERE scheme_id=%s AND period_id=%s", (scheme_id, period_id))
                    existing = cursor.fetchone()
                    if existing:
                        snapshot_id = existing[0]
                        # Clean holdings to reload
                        cursor.execute("DELETE FROM equity_holdings WHERE snapshot_id=%s", (snapshot_id,))
                    else:
                        tv = sum(x['market_value_inr'] for x in db_holdings)
                        cursor.execute("""
                            INSERT INTO scheme_snapshots (scheme_id, period_id, total_holdings, total_value_inr, holdings_count)
                            VALUES (%s, %s, %s, %s, %s) RETURNING snapshot_id
                        """, (scheme_id, period_id, len(db_holdings), tv, len(db_holdings)))
                        snapshot_id = cursor.fetchone()[0]
                        
                    # 4. Insert Holdings (Bulk)
                    # We do it one by one to find the killer if bulk fails
                    for dh in db_holdings:
                        try:
                            cursor.execute("""
                                INSERT INTO equity_holdings (snapshot_id, company_id, quantity, market_value_inr, percent_of_nav)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (snapshot_id, dh['company_id'], dh['quantity'], dh['market_value_inr'], dh['percent_of_nav']))
                        except Exception as he:
                            print(f"[ERROR] Holding insert failed for {dh}: {he}")
                            raise he
                            
                    print(f"Success: {s_key}")
                    # conn.commit() REMOVED to simulate single transaction
                    
                except Exception as e:
                    print(f"CRASH loading scheme {s_key}")
                    print(f"Error: {e}")
                    conn.rollback() 
                    raise e # Fail fast to see the error

            print("All schemes loaded. Committing transaction...")
            conn.commit()
            print("Transaction Committed.")
            
        except Exception as e:
            print(f"Outer Error: {e}")
            traceback.print_exc()
            conn.rollback()
        finally:
            conn.close()
            
    except Exception as e:
        print(f"Fatal: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    debug_load()
