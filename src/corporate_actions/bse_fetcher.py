"""
BSE Corporate Actions Fetcher
=============================
Fetches corporate actions from BSE India API, parses them, 
and upserts them into the database. Resolves BSE scrip codes to ISINs.
"""

import re
import logging
from datetime import datetime, timedelta, date
from typing import Optional, Dict, List, Tuple, Any

import requests

from src.db.connection import get_connection
from src.config import logger

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

BSE_CA_URL = "https://api.bseindia.com/BseIndiaAPI/api/DefaultData/w"

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.bseindia.com/corporates/corporate_act.aspx",
    "Connection": "keep-alive",
}

# Reusing patterns from NSEFetcher but adding BSE-specific variations if needed
SPLIT_RATIO_PATTERNS = [
    r"from\s+(?:rs|re)\.?\s*(\d+(?:\.\d+)?)/?-?\s*(?:per\s+share)?\s+to\s+(?:rs|re)\.?\s*(\d+(?:\.\d+)?)/?-?",
    r"(?:face\s*value|fv)\s*(?:from)?\s*(?:rs\.?\s*|inr\s*)?(\d+(?:\.\d+)?)\s*(?:to|-)\s*(?:rs\.?\s*|inr\s*)?(\d+(?:\.\d+)?)",
    r"split\s+(?:of\s+)?(?:rs\.?\s*)?(\d+(?:\.\d+)?)\s*/?\s*-?\s*(?:to|into|/)\s*(?:rs\.?\s*)?(\d+(?:\.\d+)?)",
]

BONUS_RATIO_PATTERNS = [
    r"bonus\s+(\d+)\s*:\s*(\d+)",
    r"(\d+)\s*:\s*(\d+)\s*bonus",
    r"(\d+)\s+equity\s+shares?\s+for\s+(?:every\s+)?(\d+)",
    r"bonus.*?(\d+)\s*(?:share|eq).*?(?:every|for|per)\s*(\d+)",
]

ACTION_KEYWORDS = {
    "SPLIT":    ["face value", "fv", "sub-division", "subdivision", "sub division",
                 "split", "reduced from", "face val"],
    "BONUS":    ["bonus"],
    "DIVIDEND": ["dividend", "interim div", "final div", "special div"],
    "RIGHTS":   ["rights issue", "rights entitlement", "rights"],
    "MERGER":   ["amalgamation", "merger", "demerger", "scheme of arrangement",
                 "acquisition", "takeover"],
}

# ─────────────────────────────────────────────────────────────────────────────
# BSEFetcher
# ─────────────────────────────────────────────────────────────────────────────

class BSEFetcher:
    """
    Fetches corporate actions from BSE India and upserts them into the DB.
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(REQUEST_HEADERS)

    def run(self) -> Dict[str, Any]:
        """
        Main entry point. Fetches recent corporate actions from BSE.
        """
        logger.info("[BSEFetcher] Fetching corporate actions from BSE")

        # BSE DefaultData/w returns a sliding window of recent actions
        params = {
            "pageType": "corp_action",
            "scrp_cd": "",
            "exDate": "",
            "category": ""
        }

        try:
            resp = self.session.get(BSE_CA_URL, params=params, timeout=20)
            resp.raise_for_status()
            records = resp.json()
        except Exception as e:
            logger.error(f"[BSEFetcher] BSE fetch failed: {e}")
            return {"inserted": 0, "updated": 0, "errors": 1}

        if not isinstance(records, list):
            logger.warning(f"[BSEFetcher] Unexpected BSE response format: {type(records)}")
            return {"inserted": 0, "updated": 0, "errors": 1}

        logger.info(f"[BSEFetcher] Received {len(records)} raw records from BSE.")

        # Parse + classify each record
        parsed = []
        for r in records:
            p = self._parse_bse_record(r)
            if p:
                parsed.append(p)

        logger.info(f"[BSEFetcher] Parsed {len(parsed)} valid records with ISINs.")

        # Upsert into DB
        stats = self._upsert_records(parsed)

        # Enqueue SPLIT/BONUS ISINs for reprocessing
        enqueued = self._enqueue_quantity_affecting_isins(parsed)
        stats["enqueued_for_reprocessing"] = enqueued

        logger.info(f"[BSEFetcher] Done. inserted={stats['inserted']}, updated={stats['updated']}, enqueued={enqueued}")
        return stats

    def _parse_bse_record(self, record: Dict) -> Optional[Dict[str, Any]]:
        """
        Parses a BSE raw record.
        BSE fields: scrip_code, short_name, Ex_date, Purpose, long_name, exdate
        """
        scrip_code = str(record.get("scrip_code") or "").strip()
        comp_name  = (record.get("long_name") or record.get("short_name") or "").strip()
        purpose    = (record.get("Purpose") or "").strip()
        ex_date_str = (record.get("exdate") or "").strip() # Format: 20260309

        if not scrip_code or not purpose or not ex_date_str:
            return None

        # Resolve ISIN and Entity ID
        isin, entity_id = self._resolve_isin_and_entity(scrip_code)
        if not isin or not entity_id:
            logger.debug(f"[BSEFetcher] Could not resolve ISIN/Entity for BSE code {scrip_code} - skipping.")
            return None

        # Parse date
        try:
            ex_date = datetime.strptime(ex_date_str, "%Y%m%d").date()
        except ValueError:
            return None

        # Classify
        action_type = self._classify_action(purpose)
        if not action_type:
            action_type = "OTHER"

        # Extract Ratio
        numerator, denominator = self._extract_ratio(purpose, action_type)
        
        # Calculate ratio_factor
        if numerator and denominator and denominator > 0:
            if action_type == "SPLIT":
                ratio_factor = numerator / denominator
            elif action_type == "BONUS":
                ratio_factor = (denominator + numerator) / denominator
            else:
                ratio_factor = 1.0
        else:
            ratio_factor = 1.0

        confidence = 0.95 if (action_type in ("SPLIT", "BONUS") and numerator) else 0.80

        return {
            "isin": isin,
            "entity_id": entity_id,
            "company_name": comp_name,
            "action_type": action_type,
            "ex_date": ex_date,
            "numerator": numerator,
            "denominator": denominator,
            "ratio_factor": ratio_factor,
            "description": purpose,
            "source": "BSE",
            "confidence": confidence
        }

    def _resolve_isin_and_entity(self, scrip_code: str) -> Tuple[Optional[str], Optional[int]]:
        """Resolves ISIN and Entity ID from BSE Scrip Code."""
        conn = get_connection()
        cur = conn.cursor()
        
        # Try isin_master first
        cur.execute("SELECT isin, entity_id FROM isin_master WHERE bse_code = %s LIMIT 1", (scrip_code,))
        row = cur.fetchone()
        if row:
            return row[0], row[1]
            
        # Try companies table
        cur.execute("SELECT isin, entity_id FROM companies WHERE bse_code = %s LIMIT 1", (scrip_code,))
        row = cur.fetchone()
        if row:
            return row[0], row[1]
            
        return None, None

    def _classify_action(self, subject: str) -> Optional[str]:
        s = subject.lower()
        for action_type, keywords in ACTION_KEYWORDS.items():
            for kw in keywords:
                if kw in s:
                    return action_type
        return None

    def _extract_ratio(self, subject: str, action_type: str) -> Tuple[Optional[float], Optional[float]]:
        s = subject.lower()
        if action_type == "SPLIT":
            for pattern in SPLIT_RATIO_PATTERNS:
                m = re.search(pattern, s)
                if m:
                    try:
                        old_fv = float(m.group(1)); new_fv = float(m.group(2))
                        if new_fv > 0: return (old_fv / new_fv, 1.0)
                    except: pass
        elif action_type == "BONUS":
            for pattern in BONUS_RATIO_PATTERNS:
                m = re.search(pattern, s)
                if m:
                    try: return (float(m.group(1)), float(m.group(2)))
                    except: pass
        return None, None

    def _upsert_records(self, parsed: List[Dict]) -> Dict[str, int]:
        conn = get_connection()
        cur = conn.cursor()
        inserted = updated = errors = 0

        for rec in parsed:
            try:
                # Determine status
                if rec["action_type"] in ("SPLIT", "BONUS") and rec["confidence"] >= 0.90:
                    status = "CONFIRMED" if rec["numerator"] else "PROPOSED"
                elif rec["action_type"] in ("DIVIDEND", "RIGHTS"):
                    status = "CONFIRMED"
                else:
                    status = "PROPOSED"

                cur.execute("""
                    INSERT INTO corporate_actions (
                        entity_id, effective_date, action_type,
                        ratio_factor, numerator, denominator,
                        description, source, status, confidence_score,
                        is_applied
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE)
                    ON CONFLICT (entity_id, effective_date, action_type)
                    DO UPDATE SET
                        ratio_factor = EXCLUDED.ratio_factor,
                        numerator = EXCLUDED.numerator,
                        denominator = EXCLUDED.denominator,
                        description = EXCLUDED.description,
                        source = EXCLUDED.source,
                        confidence_score = EXCLUDED.confidence_score,
                        status = CASE
                            WHEN corporate_actions.status = 'CONFIRMED' THEN corporate_actions.status
                            ELSE EXCLUDED.status
                        END
                """, (
                    rec["entity_id"], rec["ex_date"], rec["action_type"],
                    rec["ratio_factor"], rec["numerator"], rec["denominator"],
                    rec["description"], rec["source"], status, rec["confidence"]
                ))
                if cur.rowcount: inserted += 1
                else: updated += 1
            except Exception as e:
                logger.warning(f"[BSEFetcher] Upsert failed for {rec['isin']}: {e}")
                conn.rollback()
                errors += 1
        
        conn.commit()
        return {"inserted": inserted, "updated": updated, "errors": errors}

    def _enqueue_quantity_affecting_isins(self, parsed: List[Dict]) -> int:
        affecting = [r["isin"] for r in parsed if r["action_type"] in ("SPLIT", "BONUS")]
        if not affecting: return 0
        from src.corporate_actions.reprocessing_worker import ReprocessingWorker
        return ReprocessingWorker().enqueue_batch(affecting, reason="NEW_BSE_ACTION")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    stats = BSEFetcher().run()
    print(stats)
