"""
NSE/BSE Corporate Actions Fetcher
===================================
Fetches corporate actions from NSE India and BSE India APIs,
parses and classifies each action, and upserts them into the
`corporate_actions` table in the database.

Sources:
  Primary:  NSE India — has ISIN directly, reliable, no auth required
  Secondary: BSE India — used for cross-validation / completeness

Action types handled:
  SPLIT     — stock split (face value reduction)
  BONUS     — bonus share issue
  DIVIDEND  — cash dividend (interim / final)
  RIGHTS    — rights issue
  MERGER    — amalgamation / demerger

Flow:
  fetch_nse() → parse_nse_records() → classify_action()
              → upsert_corporate_actions()
              → enqueue_reprocessing() for SPLIT/BONUS actions

Usage:
    from src.corporate_actions.nse_fetcher import NSEFetcher
    fetcher = NSEFetcher()
    result = fetcher.run(lookback_days=30)
"""

import re
import time
import logging
from datetime import datetime, timedelta, date
from typing import Optional, Dict, List, Tuple, Any

import requests

from src.db.connection import get_connection, get_cursor
from src.config import logger


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

NSE_CA_URL   = "https://www.nseindia.com/api/corporates-corporateActions"
NSE_BASE_URL = "https://www.nseindia.com"
BSE_CA_URL   = "https://api.bseindia.com/BseIndiaAPI/api/DefaultData/w"

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}

# Known split ratios as regex patterns — used to parse subject strings
# NSE splits are described as face value changes:
#   "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Rs 2/- Per Share"
#   "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Re 1/- Per Share"
SPLIT_RATIO_PATTERNS = [
    # NSE standard: "From Rs 10/- Per Share To Rs 2/- Per Share"  or  "To Re 1/-"
    r"from\s+(?:rs|re)\.?\s*(\d+(?:\.\d+)?)/?-?\s*(?:per\s+share)?\s+to\s+(?:rs|re)\.?\s*(\d+(?:\.\d+)?)/?-?",
    # Generic: "face value from 10 to 2" or "fv 10 to 2"
    r"(?:face\s*value|fv)\s*(?:from)?\s*(?:rs\.?\s*|inr\s*)?(\d+(?:\.\d+)?)\s*(?:to|-)\s*(?:rs\.?\s*|inr\s*)?(\d+(?:\.\d+)?)",
    # "split 10/2" or "split of Rs 10 to Rs 2"
    r"split\s+(?:of\s+)?(?:rs\.?\s*)?(\d+(?:\.\d+)?)\s*/?\s*-?\s*(?:to|into|/)\s*(?:rs\.?\s*)?(\d+(?:\.\d+)?)",
]

BONUS_RATIO_PATTERNS = [
    # "Bonus 1:1" or "Bonus 3:1" — most common NSE format
    r"bonus\s+(\d+)\s*:\s*(\d+)",
    # "1:1 Bonus"
    r"(\d+)\s*:\s*(\d+)\s*bonus",
    # "1 equity share for every 2"
    r"(\d+)\s+equity\s+shares?\s+for\s+(?:every\s+)?(\d+)",
    # "bonus of 1 share per 2 shares"
    r"bonus.*?(\d+)\s*(?:share|eq).*?(?:every|for|per)\s*(\d+)",
]

# Classification keyword map (subject string → action_type)
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
# NSEFetcher
# ─────────────────────────────────────────────────────────────────────────────

class NSEFetcher:
    """
    Fetches corporate actions from NSE India and upserts them into the DB.
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(REQUEST_HEADERS)
        self._session_primed = False

    # ─────────────────────────────────────
    # Public API
    # ─────────────────────────────────────

    def run(self, lookback_days: int = 60) -> Dict[str, Any]:
        """
        Main entry point. Fetches last `lookback_days` of corporate actions
        from NSE, parses them, upserts into DB, and enqueues SPLIT/BONUS ISINs
        for reprocessing.

        Returns:
            {
                "fetched_nse": int,
                "inserted": int,
                "updated": int,
                "enqueued_for_reprocessing": int,
                "errors": int
            }
        """
        today    = datetime.now()
        from_dt  = today - timedelta(days=lookback_days)
        to_date  = today.strftime("%d-%m-%Y")
        from_date = from_dt.strftime("%d-%m-%Y")

        logger.info(f"[NSEFetcher] Fetching corporate actions from {from_date} to {to_date}")

        # Fetch raw data
        nse_records = self._fetch_nse(from_date, to_date)
        logger.info(f"[NSEFetcher] NSE returned {len(nse_records)} raw records.")

        # Parse + classify each record
        parsed = [self._parse_nse_record(r) for r in nse_records]
        parsed = [p for p in parsed if p is not None]  # drop unparseable rows
        logger.info(f"[NSEFetcher] Parsed {len(parsed)} valid records.")

        # Upsert into DB
        stats = self._upsert_records(parsed)

        # Enqueue SPLIT/BONUS ISINs for reprocessing
        enqueued = self._enqueue_quantity_affecting_isins(parsed)
        stats["enqueued_for_reprocessing"] = enqueued

        logger.info(
            f"[NSEFetcher] Done. inserted={stats['inserted']}, "
            f"updated={stats['updated']}, enqueued={enqueued}, "
            f"errors={stats['errors']}"
        )
        return stats

    # ─────────────────────────────────────
    # Internal: HTTP fetch
    # ─────────────────────────────────────

    def _prime_session(self):
        """Hit NSE main page to acquire session cookies. Retries once."""
        if self._session_primed:
            return
        try:
            self.session.get(NSE_BASE_URL, timeout=15)
            time.sleep(1)
            self._session_primed = True
        except Exception as e:
            logger.warning(f"[NSEFetcher] Failed to prime NSE session: {e}. Proceeding anyway.")

    def _fetch_nse(self, from_date: str, to_date: str) -> List[Dict]:
        """Fetches corporate actions from NSE equities endpoint."""
        self._prime_session()
        params = {
            "index":      "equities",
            "from_date":  from_date,
            "to_date":    to_date,
        }
        try:
            resp = self.session.get(NSE_CA_URL, params=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            logger.warning(f"[NSEFetcher] Unexpected NSE response format: {type(data)}")
            return []
        except requests.RequestException as e:
            logger.error(f"[NSEFetcher] NSE fetch failed: {e}")
            return []
        except ValueError as e:
            logger.error(f"[NSEFetcher] NSE JSON parse error: {e}")
            return []

    # ─────────────────────────────────────
    # Internal: Parse + classify
    # ─────────────────────────────────────

    def _parse_nse_record(self, record: Dict) -> Optional[Dict[str, Any]]:
        """
        Parses a single NSE raw record into our canonical corporate_action schema.

        NSE fields:
            symbol, series, isin, comp, subject, exDate, recDate, faceVal

        Returns None if the record should be skipped (debt series, no ISIN, etc.)
        """
        isin   = (record.get("isin") or "").strip()
        symbol = (record.get("symbol") or "").strip()
        comp   = (record.get("comp") or "").strip()
        subject = (record.get("subject") or "").strip()
        series  = (record.get("series") or "EQ").strip().upper()
        ex_date_str = (record.get("exDate") or "").strip()
        rec_date_str = (record.get("recDate") or "").strip()
        face_val_str = (record.get("faceVal") or "").strip()

        # Skip non-equity series
        if series not in ("EQ", "BE", "BL", "SM", "ST", "N1", "N2", "N3", "N4", "N5", "N6", "N7", "N8", "N9"):
            if series not in ("EQ", "BE", "BL"):
                return None

        # Must have ISIN and ex_date
        if not isin or not ex_date_str or ex_date_str == "-":
            return None

        # Validate ISIN format (equity: INE...10...)
        if len(isin) != 12 or not isin.startswith("IN"):
            return None

        # Parse dates
        ex_date  = self._parse_date(ex_date_str)
        rec_date = self._parse_date(rec_date_str) if rec_date_str and rec_date_str != "-" else None
        if ex_date is None:
            return None

        # Classify action type from subject string
        action_type = self._classify_action(subject)
        if action_type is None:
            # Unknown — store as-is for review, but don't auto-confirm
            action_type = "OTHER"

        # Extract ratio for SPLIT and BONUS
        numerator, denominator = self._extract_ratio(subject, action_type, face_val_str)

        # Build ratio_factor (backward compat)
        if numerator and denominator and denominator > 0:
            if action_type == "SPLIT":
                ratio_factor = numerator / denominator
            elif action_type == "BONUS":
                ratio_factor = (denominator + numerator) / denominator
            else:
                ratio_factor = 1.0
        else:
            ratio_factor = 1.0

        # Confidence: HIGH if we parsed a clean ratio, MEDIUM otherwise
        if action_type in ("SPLIT", "BONUS") and numerator and denominator:
            confidence = 0.95
        elif action_type in ("DIVIDEND", "RIGHTS"):
            confidence = 0.90
        else:
            confidence = 0.50

        return {
            "isin":          isin,
            "symbol":        symbol,
            "company_name":  comp,
            "action_type":   action_type,
            "ex_date":       ex_date,
            "rec_date":      rec_date,
            "numerator":     numerator,
            "denominator":   denominator,
            "ratio_factor":  ratio_factor,
            "description":   subject,
            "source":        "NSE",
            "confidence":    confidence,
        }

    def _classify_action(self, subject: str) -> Optional[str]:
        """Classifies subject string to SPLIT | BONUS | DIVIDEND | RIGHTS | MERGER."""
        s = subject.lower()
        for action_type, keywords in ACTION_KEYWORDS.items():
            for kw in keywords:
                if kw in s:
                    return action_type
        return None

    def _extract_ratio(
        self, subject: str, action_type: str, face_val: str
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        Extracts (numerator, denominator) ratio from the subject string.

        For SPLIT on NSE: typically described as face-value change
            e.g., "Face Value Changed From Rs. 10/- To Rs. 2/-"
            → old_fv=10, new_fv=2 → split_ratio = 10/2 = 5
            → numerator=5, denominator=1

        For BONUS:
            e.g., "1:1 Bonus" → 1 bonus per 1 existing → qty_factor = 2
            → numerator=1, denominator=1 (bonus shares : existing shares)
        """
        subject_lower = subject.lower()

        if action_type == "SPLIT":
            for pattern in SPLIT_RATIO_PATTERNS:
                m = re.search(pattern, subject_lower)
                if m:
                    try:
                        old_fv = float(m.group(1))
                        new_fv = float(m.group(2))
                        if new_fv > 0:
                            # split_ratio = old_fv / new_fv (e.g., 10/2 = 5)
                            split_ratio = old_fv / new_fv
                            return (split_ratio, 1.0)
                    except (ValueError, IndexError):
                        pass

        elif action_type == "BONUS":
            for pattern in BONUS_RATIO_PATTERNS:
                m = re.search(pattern, subject_lower)
                if m:
                    try:
                        bonus_shares   = float(m.group(1))
                        existing_shares = float(m.group(2))
                        # numerator=bonus_shares, denominator=existing_shares
                        return (bonus_shares, existing_shares)
                    except (ValueError, IndexError):
                        pass

        return (None, None)

    def _parse_date(self, date_str: str) -> Optional[date]:
        """Parses NSE date formats: '05-Feb-2026', '2026-02-05', '05/02/2026'"""
        formats = ["%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%b %d, %Y"]
        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt).date()
            except ValueError:
                continue
        logger.debug(f"[NSEFetcher] Could not parse date: {date_str!r}")
        return None

    # ─────────────────────────────────────
    # Internal: DB upsert
    # ─────────────────────────────────────

    def _upsert_records(self, parsed: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Upserts each parsed record into corporate_actions.

        Logic:
          1. Resolve entity_id from isin_master via ISIN.
          2. If entity_id found: upsert on (entity_id, action_type, effective_date).
          3. Status = CONFIRMED if confidence >= 0.90 AND action_type in (SPLIT, BONUS, DIVIDEND, RIGHTS)
             Status = PROPOSED otherwise (admin needs to review before engine applies it).
        """
        conn = get_connection()
        cur  = conn.cursor()
        inserted = updated = errors = 0

        for rec in parsed:
            try:
                # Resolve entity_id
                entity_id = self._resolve_entity_id(cur, rec["isin"])
                if entity_id is None:
                    logger.debug(f"[NSEFetcher] No entity_id for ISIN {rec['isin']} — skipping.")
                    continue

                # Determine status
                if rec["action_type"] in ("SPLIT", "BONUS") and rec["confidence"] >= 0.90:
                    # For SPLIT/BONUS we require explicit ratio confirmation before applying
                    # Auto-set to CONFIRMED only if ratio was cleanly extracted
                    if rec["numerator"] and rec["denominator"]:
                        status = "CONFIRMED"
                    else:
                        status = "PROPOSED"
                elif rec["action_type"] in ("DIVIDEND", "RIGHTS") and rec["confidence"] >= 0.90:
                    status = "CONFIRMED"
                else:
                    status = "PROPOSED"

                cur.execute("""
                    INSERT INTO corporate_actions (
                        entity_id, effective_date, action_type,
                        ratio_factor, numerator, denominator,
                        description, source, status, confidence_score,
                        is_applied
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        FALSE
                    )
                    ON CONFLICT (entity_id, effective_date, action_type)
                    DO UPDATE SET
                        ratio_factor     = EXCLUDED.ratio_factor,
                        numerator        = EXCLUDED.numerator,
                        denominator      = EXCLUDED.denominator,
                        description      = EXCLUDED.description,
                        source           = EXCLUDED.source,
                        confidence_score = EXCLUDED.confidence_score,
                        -- Only upgrade status, never downgrade a CONFIRMED record
                        status = CASE
                            WHEN corporate_actions.status = 'CONFIRMED' THEN corporate_actions.status
                            ELSE EXCLUDED.status
                        END
                """, (
                    entity_id,
                    rec["ex_date"],
                    rec["action_type"],
                    rec["ratio_factor"],
                    rec["numerator"],
                    rec["denominator"],
                    rec["description"],
                    rec["source"],
                    status,
                    rec["confidence"],
                ))

                if cur.rowcount:
                    # rowcount=1 on INSERT, 0 on no-op DO UPDATE
                    inserted += 1
                else:
                    updated += 1

            except Exception as e:
                logger.warning(f"[NSEFetcher] Upsert failed for {rec.get('isin')}: {e}")
                conn.rollback()
                errors += 1
                continue

        conn.commit()
        cur.close()
        return {"inserted": inserted, "updated": updated, "errors": errors,
                "fetched_nse": len(parsed)}

    def _resolve_entity_id(self, cur, isin: str) -> Optional[int]:
        """Looks up entity_id from isin_master. Returns None if not found."""
        cur.execute(
            "SELECT entity_id FROM isin_master WHERE isin = %s LIMIT 1",
            (isin,)
        )
        row = cur.fetchone()
        return row[0] if row else None

    # ─────────────────────────────────────
    # Internal: Enqueue reprocessing
    # ─────────────────────────────────────

    def _enqueue_quantity_affecting_isins(self, parsed: List[Dict[str, Any]]) -> int:
        """
        Enqueues ISINs for adj_quantity reprocessing — but ONLY for
        SPLIT and BONUS actions (which change share count).
        DIVIDEND, RIGHTS, MERGER are excluded from qty reprocessing
        unless the caller specifically requests it.
        """
        qty_affecting = [
            r["isin"] for r in parsed
            if r["action_type"] in ("SPLIT", "BONUS")
        ]
        if not qty_affecting:
            return 0

        from src.corporate_actions.reprocessing_worker import ReprocessingWorker
        worker = ReprocessingWorker()
        return worker.enqueue_batch(qty_affecting, reason="NEW_ACTION")
