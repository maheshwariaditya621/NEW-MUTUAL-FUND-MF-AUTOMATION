import requests
import time
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Dict, Any
from src.config import logger
from src.ingestion.amfi_nav_parser import AMFINavParser
from src.ingestion.amfi_amc_mapping import AMFI_AMC_MAPPING
from src.db import upsert_nav_entries, close_connection

class AMFINavBackfiller:
    """
    Crawls and fills historical NAV data from AMFI in 90-day chunks.
    """
    # Using the portal URL directly
    BASE_URL = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Referer": "https://www.amfiindia.com/net-asset-value/nav-history"
    }

    def __init__(self, download_dir: str = "data/raw/amfi/historical"):
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.parser = AMFINavParser()

    def _fetch_chunk(self, amc_code: int, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """
        Fetches a 90-day chunk for a specific AMC.
        """
        params = {
            "mf": amc_code,
            "frmdt": start_date.strftime("%d-%b-%Y"),
            "todt": end_date.strftime("%d-%b-%Y")
        }
        
        logger.info(f"Fetching historical NAV for AMC {amc_code} ({start_date} to {end_date})...")
        
        try:
            response = requests.get(self.BASE_URL, params=params, headers=self.HEADERS, timeout=60)
            response.raise_for_status()
            
            # Save to temporary file for parsing
            temp_path = self.download_dir / f"amc_{amc_code}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.txt"
            with open(temp_path, "w", encoding="utf-8") as f:
                f.write(response.text)
            
            # Parse
            data = self.parser.parse_file(temp_path)
            return data
            
        except Exception as e:
            logger.error(f"Failed to fetch chunk for AMC {amc_code}: {e}")
            return []

    def backfill_amc(self, amc_slug: str, start_date: date):
        """
        Runs the backfill loop for a single AMC.
        """
        amc_code = AMFI_AMC_MAPPING.get(amc_slug)
        if not amc_code:
            logger.error(f"AMC slug {amc_slug} not found in AMFI mapping.")
            return

        current_start = start_date
        today = date.today()
        
        while current_start < today:
            current_end = min(current_start + timedelta(days=90), today)
            
            data = self._fetch_chunk(amc_code, current_start, current_end)
            if data:
                upsert_nav_entries(data)
                logger.info(f"Ingested {len(data)} rows for {amc_slug} ({current_start} to {current_end})")
            
            # Rate limiting
            logger.info("Rate limiting: sleeping for 3 seconds...")
            time.sleep(3)
            
            current_start = current_end + timedelta(days=1)

    def run_all(self, start_date: date):
        """
        Runs backfill for all mapped AMCs.
        """
        for slug in AMFI_AMC_MAPPING.keys():
            self.backfill_amc(slug, start_date)

if __name__ == "__main__":
    backfiller = AMFINavBackfiller()
    # Test for HDFC for the last 6 months
    test_start = date.today() - timedelta(days=180)
    backfiller.backfill_amc("hdfc", test_start)
    close_connection()
