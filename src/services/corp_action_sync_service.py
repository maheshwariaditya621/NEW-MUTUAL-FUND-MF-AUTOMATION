import requests
import re
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from src.config import logger
from src.db import get_cursor, get_isin_details, log_resolution_audit

class CorpActionSyncService:
    """
    Authoritative service to sync corporate actions from NSE.
    Reconciles official entries with internally detected anomalies.
    """
    
    NSE_URL = "https://www.nseindia.com/api/corporates-corporateActions?index=equities"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-actions",
        "X-Requested-With": "XMLHttpRequest"
    }

    @staticmethod
    def fetch_from_nse() -> List[Dict[str, Any]]:
        """Fetch raw JSON from NSE API."""
        try:
            # Note: NSE often requires hitting the home page first to get cookies
            session = requests.Session()
            session.get("https://www.nseindia.com", headers=CorpActionSyncService.HEADERS, timeout=10)
            
            response = session.get(CorpActionSyncService.NSE_URL, headers=CorpActionSyncService.HEADERS, timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to fetch corporate actions from NSE: {e}")
            return []

    @staticmethod
    def parse_purpose(purpose: str) -> Optional[Dict[str, Any]]:
        """
        Parses the 'Purpose' string for Split or Bonus ratios.
        """
        purpose = purpose.upper()
        
        # 1. SPLIT: "Face Value Split From Rs 10/- To Rs 2/-"
        split_match = re.search(r"FROM RS (\d+)/- TO RS (\d+)/-", purpose)
        if split_match:
            old_fv = float(split_match.group(1))
            new_fv = float(split_match.group(2))
            return {"type": "SPLIT/BONUS", "factor": old_fv / new_fv}

        # 2. BONUS: "Bonus 1:1"
        bonus_match = re.search(r"BONUS (\d+):(\d+)", purpose)
        if bonus_match:
            added = float(bonus_match.group(1))
            base = float(bonus_match.group(2))
            return {"type": "SPLIT/BONUS", "factor": (added + base) / base}

        return None

    @classmethod
    def sync(cls):
        """Main execution loop for syncing actions."""
        logger.info("Starting Corporate Action Sync from NSE...")
        data = cls.fetch_from_nse()
        if not data:
            return

        cursor = get_cursor()
        count = 0

        for entry in data:
            isin = entry.get('isin')
            symbol = entry.get('symbol')
            purpose_raw = entry.get('purpose', '')
            ex_date_str = entry.get('exDate') # e.g., "16-Feb-2026"
            
            if not isin or not ex_date_str:
                continue

            parsed = cls.parse_purpose(purpose_raw)
            if not parsed:
                continue

            # Resolve Entity
            isin_meta = get_isin_details(isin)
            if not isin_meta or not isin_meta.get('entity_id'):
                logger.warning(f"Could not resolve entity for ISIN {isin} ({symbol}). Skipping sync.")
                continue
            
            entity_id = isin_meta['entity_id']
            try:
                ex_date = datetime.strptime(ex_date_str, "%d-%b-%Y").date()
            except ValueError:
                logger.error(f"Invalid date format: {ex_date_str}")
                continue

            # Upsert logic with Reconciliation
            # Priority: Official NSE entry overrides or confirms PROPOSED ones
            cursor.execute(
                """
                INSERT INTO corporate_actions (
                    entity_id, action_type, ratio_factor, effective_date, 
                    status, confidence_score, source
                )
                VALUES (%s, %s, %s, %s, 'CONFIRMED', 1.0, 'NSE_API')
                ON CONFLICT (entity_id, effective_date, action_type) DO UPDATE SET
                    status = 'CONFIRMED',
                    confidence_score = 1.0,
                    source = 'NSE_API',
                    ratio_factor = EXCLUDED.ratio_factor,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (entity_id, parsed['type'], parsed['factor'], ex_date)
            )
            count += 1
        
        cursor.connection.commit()
        logger.success(f"Successfully synced {count} corporate actions from NSE.")

if __name__ == "__main__":
    CorpActionSyncService.sync()
