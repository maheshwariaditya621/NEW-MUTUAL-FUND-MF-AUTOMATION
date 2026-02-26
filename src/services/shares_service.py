import time
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor
from src.config import logger
from src.db.connection import get_connection

class SharesService:
    def __init__(self):
        pass

    def _get_ticker_symbol(self, nse_symbol: str, bse_code: str) -> str:
        """Determines the correct yfinance ticker format"""
        if nse_symbol:
            return f"{nse_symbol}.NS"
        if bse_code:
            return f"{bse_code}.BO"
        return None

    def fetch_shares(self, ticker: str, retries: int = 4) -> int:
        """Fetches the shares outstanding from yfinance with backoff retries"""
        import random
        for attempt in range(retries):
            try:
                tkr = yf.Ticker(ticker)
                shares = tkr.info.get("sharesOutstanding")
                if shares:
                    return int(shares)
                break # If no exception but no shares, just break
            except Exception as e:
                err_str = str(e).lower()
                if "too many requests" in err_str or "429" in err_str:
                    sleep_time = (2 ** attempt) + random.random()
                    logger.debug(f"Rate limited for {ticker}. Retrying {attempt+1}/{retries} in {sleep_time:.2f}s...")
                    time.sleep(sleep_time)
                else:
                    logger.debug(f"Failed to fetch shares for {ticker}: {e}")
                    break
                    
        return None

    def sync_all_shares(self) -> int:
        """
        Iterates over all unique companies in the database, fetches their shares,
        and safely updates the DB in bulk.
        Returns the number of companies successfully updated.
        """
        logger.info("Starting automated Shares Outstanding sync from yfinance...")
        start_time = time.time()
        updates = []
        
        try:
            conn = get_connection()
            cur = conn.cursor()
            
            # 1. Get all eligible companies (Skip recently updated ones to allow resuming interrupted jobs)
            cur.execute('''
                SELECT company_id, nse_symbol, bse_code, company_name 
                FROM companies
                WHERE (nse_symbol IS NOT NULL OR bse_code IS NOT NULL)
                AND (shares_last_updated_at IS NULL OR shares_last_updated_at < NOW() - INTERVAL '1 day')
            ''')
            companies = cur.fetchall()
            
            if not companies:
                logger.info("All companies have shares synced recently. Nothing to update.")
                cur.close()
                conn.close()
                return 0
            
            def _process_company(row):
                cid, nse, bse, name = row
                ticker = self._get_ticker_symbol(nse, bse)
                if not ticker:
                    return None
                    
                # Rate limit safety sleep between requests
                time.sleep(0.5)
                
                shares = self.fetch_shares(ticker)
                if shares:
                    return (shares, cid)
                return None
                
            # 2. Fetch in parallel (Reduced concurrency to 2 to minimize 429 blocks)
            logger.info(f"Fetching shares for {len(companies)} companies concurrently...")
            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(_process_company, companies))
                
            updates = [r for r in results if r]
            
            # 3. Bulk Update DB
            if updates:
                cur.executemany('''
                    UPDATE companies 
                    SET shares_outstanding = %s, 
                        shares_last_updated_at = NOW() 
                    WHERE company_id = %s
                ''', updates)
                conn.commit()
                
            cur.close()
            conn.close()
            
            duration = time.time() - start_time
            logger.info(f"Shares sync complete. Updated {len(updates)} records in {duration:.2f}s")
            return len(updates)
            
        except Exception as e:
            logger.error(f"Error during shares sync: {e}")
            return 0

shares_service = SharesService()
