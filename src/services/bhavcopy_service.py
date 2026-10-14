"""
NSE Bhavcopy EOD Price Service.

Downloads official daily NSE Bhavcopy files directly from NSE Archives,
extracts closing prices, previous closes, and trade dates by ISIN,
and updates the PostgreSQL companies table in high-performance batches.
"""

import io
import csv
import zipfile
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from decimal import Decimal
import requests
from zoneinfo import ZoneInfo

from src.db.connection import get_pool_connection, release_pool_connection
from src.config import logger

IST = ZoneInfo("Asia/Kolkata")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com",
}


class BhavcopyService:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def get_latest_trading_date(self) -> datetime:
        """
        Returns the most recent probable trading date.
        If current IST time is before 18:45, looks at the previous day.
        Skips weekends (Saturday/Sunday).
        """
        now = datetime.now(IST)
        # Bhavcopy is typically released after 18:30 IST on trading days
        target = now if now.hour >= 19 else now - timedelta(days=1)

        # Skip backward if weekend
        while target.weekday() >= 5:  # 5=Sat, 6=Sun
            target -= timedelta(days=1)

        return target

    def download_cm_bhavcopy(self, target_date: Optional[datetime] = None) -> Tuple[Optional[bytes], datetime]:
        """
        Attempt to download consolidated CM Bhavcopy for the target date,
        falling back up to 5 days if markets were closed (holidays/weekends).
        """
        if not target_date:
            target_date = self.get_latest_trading_date()

        for attempt in range(6):
            check_date = target_date - timedelta(days=attempt)
            if check_date.weekday() >= 5:
                continue

            date_str = check_date.strftime("%Y%m%d")
            url = f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv.zip"

            try:
                logger.info(f"Checking NSE Bhavcopy for date: {check_date.strftime('%Y-%m-%d')}...")
                resp = self.session.get(url, timeout=10)
                if resp.status_code == 200 and len(resp.content) > 1000:
                    logger.info(f"Found NSE Bhavcopy for {check_date.strftime('%Y-%m-%d')} ({len(resp.content)} bytes)")
                    return resp.content, check_date
            except Exception as e:
                logger.warning(f"Error fetching Bhavcopy for {date_str}: {e}")

        return None, target_date

    def parse_bhavcopy_zip(self, zip_bytes: bytes) -> List[Dict[str, Any]]:
        """
        Extract and parse closing prices from Bhavcopy zip file.
        Returns a list of dicts with:
        isin, symbol, close_price, prev_close, price_date, price_change_pct
        """
        results = []
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
                csv_filename = z.namelist()[0]
                with z.open(csv_filename) as f:
                    reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
                    headers = [c.strip() for c in next(reader)]

                    isin_idx = headers.index("ISIN")
                    cls_idx = headers.index("ClsPric")
                    prev_idx = headers.index("PrvsClsgPric")
                    dt_idx = headers.index("TradDt")
                    sym_idx = headers.index("TckrSymb")
                    srs_idx = headers.index("SctySrs")

                    for row in reader:
                        if not row or len(row) <= max(isin_idx, cls_idx, prev_idx, dt_idx, srs_idx):
                            continue

                        isin = row[isin_idx].strip()
                        series = row[srs_idx].strip()
                        symbol = row[sym_idx].strip()

                        # Filter for equity series & valid equity ISINs (starts with INE)
                        if series in ("EQ", "BE", "SM") and isin.startswith("INE"):
                            try:
                                close_p = float(row[cls_idx]) if row[cls_idx] else None
                                prev_p = float(row[prev_idx]) if row[prev_idx] else None
                                dt_str = row[dt_idx].strip()
                                
                                chg_pct = None
                                if close_p and prev_p and prev_p > 0:
                                    chg_pct = round(((close_p - prev_p) / prev_p) * 100, 2)

                                if close_p is not None:
                                    results.append({
                                        "isin": isin,
                                        "symbol": symbol,
                                        "close_price": close_p,
                                        "prev_close": prev_p,
                                        "price_date": dt_str,
                                        "price_change_pct": chg_pct
                                    })
                            except (ValueError, TypeError):
                                continue

            logger.info(f"Parsed {len(results)} equity closing prices from Bhavcopy.")
            return results

        except Exception as e:
            logger.error(f"Failed to parse Bhavcopy zip: {e}")
            return []

    def sync_prices_to_db(self, prices: List[Dict[str, Any]]) -> int:
        """
        High performance staging batch update into PostgreSQL companies table.
        Updates close_price, prev_close, price_date, price_change_pct, and market_cap.
        """
        if not prices:
            return 0

        conn = get_pool_connection()
        try:
            cur = conn.cursor()

            # 1. Create temporary staging table
            cur.execute("""
                CREATE TEMP TABLE staging_bhavcopy (
                    isin VARCHAR(12) PRIMARY KEY,
                    close_price NUMERIC(12, 2),
                    prev_close NUMERIC(12, 2),
                    price_date DATE,
                    price_change_pct NUMERIC(6, 2)
                ) ON COMMIT DROP;
            """)

            # 2. Batch insert into staging table
            from psycopg2.extras import execute_values
            data_tuples = [
                (
                    p["isin"],
                    p["close_price"],
                    p["prev_close"],
                    p["price_date"],
                    p["price_change_pct"]
                )
                for p in prices
            ]

            execute_values(
                cur,
                """
                INSERT INTO staging_bhavcopy (isin, close_price, prev_close, price_date, price_change_pct)
                VALUES %s
                ON CONFLICT (isin) DO UPDATE SET
                    close_price = EXCLUDED.close_price,
                    prev_close = EXCLUDED.prev_close,
                    price_date = EXCLUDED.price_date,
                    price_change_pct = EXCLUDED.price_change_pct
                """,
                data_tuples
            )

            # 3. Update companies table from staging table
            cur.execute("""
                UPDATE companies c
                SET 
                    close_price = s.close_price,
                    prev_close = s.prev_close,
                    price_date = s.price_date,
                    price_change_pct = s.price_change_pct,
                    market_cap = CASE 
                        WHEN c.shares_outstanding IS NOT NULL AND s.close_price > 0 
                        THEN (s.close_price * c.shares_outstanding)::BIGINT 
                        ELSE c.market_cap 
                    END,
                    mcap_updated_at = CURRENT_TIMESTAMP
                FROM staging_bhavcopy s
                WHERE c.isin = s.isin;
            """)

            updated_count = cur.rowcount
            conn.commit()
            logger.info(f"Successfully updated EOD closing prices for {updated_count} companies in database.")
            return updated_count

        except Exception as e:
            conn.rollback()
            logger.error(f"Error syncing Bhavcopy to database: {e}")
            raise
        finally:
            release_pool_connection(conn)

    def run_sync(self, target_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Orchestrates full download, parse, and DB sync.
        """
        zip_bytes, trade_date = self.download_cm_bhavcopy(target_date)
        if not zip_bytes:
            return {
                "success": False,
                "message": "Could not download Bhavcopy from NSE archives."
            }

        prices = self.parse_bhavcopy_zip(zip_bytes)
        if not prices:
            return {
                "success": False,
                "message": "No valid equity prices found in Bhavcopy."
            }

        updated_count = self.sync_prices_to_db(prices)
        date_str = trade_date.strftime("%Y-%m-%d")

        return {
            "success": True,
            "trade_date": date_str,
            "total_bhavcopy_records": len(prices),
            "updated_companies_count": updated_count,
            "message": f"Successfully synced {updated_count} stock EOD prices for trade date {date_str}."
        }


bhavcopy_service = BhavcopyService()

if __name__ == "__main__":
    result = bhavcopy_service.run_sync()
    print(result)
