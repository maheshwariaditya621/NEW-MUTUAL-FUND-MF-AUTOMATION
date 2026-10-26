"""
Financial Results and Board Meeting Classifier.

Extracts fiscal period (Q1-Q4, FY), financial year, audit status,
result type (Standalone vs Consolidated), meeting purpose, and revision flags
from Indian exchange disclosures.
"""

import re
from datetime import datetime, date
from typing import Dict, Any, Optional, Tuple, List


# Month map for text extraction
MONTH_MAP = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12
}


class EarningsClassifier:
    """Deterministic classifier for financial results and board meetings."""

    @staticmethod
    def extract_fiscal_period(text: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract financial year (e.g. 'FY2026', 'FY2027') and fiscal period ('Q1', 'Q2', 'Q3', 'Q4', 'FY', 'H1', 'H2').
        Returns (financial_year, fiscal_period).
        """
        if not text:
            return None, None

        t = text.lower()

        # 1. Direct Q1/Q2/Q3/Q4 patterns: e.g. "Q2 FY27", "Q2 of FY 2026-27", "Quarter 2"
        q_match = re.search(r"\b(?:q([1-4])|quarter\s*([1-4])|1st\s+quarter|2nd\s+quarter|3rd\s+quarter|4th\s+quarter)\b", t)
        fiscal_period = None
        if q_match:
            if q_match.group(1):
                fiscal_period = f"Q{q_match.group(1)}"
            elif q_match.group(2):
                fiscal_period = f"Q{q_match.group(2)}"
            elif "1st" in q_match.group(0):
                fiscal_period = "Q1"
            elif "2nd" in q_match.group(0):
                fiscal_period = "Q2"
            elif "3rd" in q_match.group(0):
                fiscal_period = "Q3"
            elif "4th" in q_match.group(0):
                fiscal_period = "Q4"

        # Check for Half year ended
        if not fiscal_period:
            if re.search(r"\b(?:half\s+year|h1)\b", t):
                fiscal_period = "Q2"  # Indian H1 corresponds to Q2 (Sep 30)
            elif re.search(r"\bh2\b", t):
                fiscal_period = "Q4"

        # Check for Period Ended / Quarter Ended dates:
        # e.g. "Quarter Ended September 30, 2026", "Quarter Ended 30th June, 2026", "31st March 2026"
        month_period_match = re.search(
            r"(?:ended|ending|period\s+ended|quarter\s+ended)\s+(?:on\s+)?(?:(\d{1,2})(?:st|nd|rd|th)?\s+)?([a-z]+)(?:\s+(\d{1,2})(?:st|nd|rd|th)?)?[,\s]+(\d{4})",
            t
        )
        detected_fy = None
        if month_period_match:
            month_str = month_period_match.group(2).lower()
            year_int = int(month_period_match.group(4))
            month_num = MONTH_MAP.get(month_str[:3])
            if month_num:
                if month_num in (4, 5, 6):  # June quarter -> Q1
                    if not fiscal_period:
                        fiscal_period = "Q1"
                    detected_fy = f"FY{year_int + 1}"
                elif month_num in (7, 8, 9):  # September quarter -> Q2
                    if not fiscal_period:
                        fiscal_period = "Q2"
                    detected_fy = f"FY{year_int + 1}"
                elif month_num in (10, 11, 12):  # December quarter -> Q3
                    if not fiscal_period:
                        fiscal_period = "Q3"
                    detected_fy = f"FY{year_int + 1}"
                elif month_num in (1, 2, 3):  # March quarter -> Q4
                    if not fiscal_period:
                        fiscal_period = "Q4"
                    detected_fy = f"FY{year_int}"

        # 2. Extract FY if not detected yet
        # e.g. "FY2026", "FY 2026-27", "FY27", "2026-2027"
        if not detected_fy:
            fy_match = re.search(r"\bfy\s*(\d{2,4})(?:[/-]\s*(\d{2,4}))?\b", t)
            if fy_match:
                y1 = fy_match.group(1)
                y2 = fy_match.group(2)
                if len(y1) == 2:
                    detected_fy = f"FY20{y1}"
                elif len(y1) == 4:
                    if y2:
                        detected_fy = f"FY20{y2[-2:]}" if len(y2) == 2 else f"FY{y2}"
                    else:
                        detected_fy = f"FY{y1}"
            else:
                # Year span match e.g. "2026-27"
                span_match = re.search(r"\b(20\d{2})[-/](\d{2})\b", t)
                if span_match:
                    detected_fy = f"FY20{span_match.group(2)}"

        # Default fallbacks if annual results
        if not fiscal_period:
            if re.search(r"\b(?:annual\s+results|full\s+year|year\s+ended)\b", t):
                fiscal_period = "FY"

        return detected_fy, fiscal_period

    @staticmethod
    def classify_result_type(text: str) -> str:
        """
        Classifies result filing into 'STANDALONE', 'CONSOLIDATED', 'BOTH', or 'UNKNOWN'.
        """
        if not text:
            return "UNKNOWN"
        t = text.lower()
        has_standalone = bool(re.search(r"\bstandalone\b", t))
        has_consolidated = bool(re.search(r"\bconsolidated\b", t))

        if has_standalone and has_consolidated:
            return "BOTH"
        if has_consolidated:
            return "CONSOLIDATED"
        if has_standalone:
            return "STANDALONE"
        return "UNKNOWN"

    @staticmethod
    def classify_audit_status(text: str) -> str:
        """
        Classifies result filing into 'AUDITED', 'UNAUDITED', 'LIMITED_REVIEW', or 'UNKNOWN'.
        """
        if not text:
            return "UNKNOWN"
        t = text.lower()
        if re.search(r"\bunaudited\b|\bun-audited\b", t):
            return "UNAUDITED"
        if re.search(r"\blimited\s+review\b", t):
            return "LIMITED_REVIEW"
        if re.search(r"\baudited\b", t):
            return "AUDITED"
        return "UNKNOWN"

    @staticmethod
    def is_results_meeting(purpose: str) -> bool:
        """
        Determines if a board meeting purpose is to consider financial results.
        """
        if not purpose:
            return False
        p = purpose.lower()
        patterns = [
            r"\bresults?\b",
            r"\bfinancial\s+results\b",
            r"\bfinancial\s+statements\b",
            r"\baccounts\b",
            r"\bunaudited\b",
            r"\baudited\b",
            r"\blimited\s+review\b",
            r"\bquarterly\b"
        ]
        return any(re.search(pattern, p) for pattern in patterns)

    @staticmethod
    def detect_meeting_status(subject: str, details: str = "") -> str:
        """
        Detects if board meeting is SCHEDULED, RESCHEDULED, POSTPONED, or CANCELLED.
        """
        combined = f"{subject} {details}".lower()
        if re.search(r"\bcancell", combined):
            return "CANCELLED"
        if re.search(r"\bpostpon|\bdeferr", combined):
            return "POSTPONED"
        if re.search(r"\breschedul|\bchange\s+in\s+date|\badvance", combined):
            return "RESCHEDULED"
        return "SCHEDULED"

    @staticmethod
    def extract_meeting_date_from_text(text: str, default_date: Optional[date] = None) -> Optional[date]:
        """
        Parses scheduled meeting date from announcements like:
        "Board Meeting To Be Held On October 27, 2026"
        "Meeting scheduled on 24th October 2026"
        "Meeting on 14-Oct-2026"
        """
        if not text:
            return default_date

        t = text.strip()

        # Pattern 1: Month Name DD, YYYY e.g. "October 27, 2026" or "Oct 27, 2026"
        m1 = re.search(
            r"\b(?:held\s+on|scheduled\s+on|meeting\s+on|on)\s+([a-zA-Z]+)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})\b",
            t, re.IGNORECASE
        )
        if m1:
            month_str, day_str, year_str = m1.group(1).lower(), m1.group(2), m1.group(3)
            month_num = MONTH_MAP.get(month_str[:3])
            if month_num:
                try:
                    return date(int(year_str), month_num, int(day_str))
                except ValueError:
                    pass

        # Pattern 2: DDth Month YYYY e.g. "27th October 2026" or "14th October 2026"
        m2 = re.search(
            r"\b(\d{1,2})(?:st|nd|rd|th)?\s+([a-zA-Z]+),?\s+(\d{4})\b",
            t, re.IGNORECASE
        )
        if m2:
            day_str, month_str, year_str = m2.group(1), m2.group(2).lower(), m2.group(3)
            month_num = MONTH_MAP.get(month_str[:3])
            if month_num:
                try:
                    return date(int(year_str), month_num, int(day_str))
                except ValueError:
                    pass

        # Pattern 3: DD-MMM-YYYY or DD/MM/YYYY e.g. "14-10-2026" or "14-Oct-2026"
        m3 = re.search(r"\b(\d{1,2})[-/]([a-zA-Z]{3}|\d{1,2})[-/](\d{4})\b", t)
        if m3:
            day_str, month_str, year_str = m3.group(1), m3.group(2), m3.group(3)
            if month_str.isdigit():
                month_num = int(month_str)
            else:
                month_num = MONTH_MAP.get(month_str.lower()[:3], 0)
            if 1 <= month_num <= 12:
                try:
                    return date(int(year_str), month_num, int(day_str))
                except ValueError:
                    pass

        return default_date

    @staticmethod
    def detect_filing_revision(subject: str, details: str = "") -> Tuple[bool, Optional[str]]:
        """
        Detects if a filing is a revision, corrigendum, or resubmission.
        """
        text = f"{subject} {details}".lower()
        if "corrigendum" in text:
            return True, "CORRIGENDUM"
        if "resubmission" in text:
            return True, "RESUBMISSION"
        if "revised" in text or "revision" in text:
            return True, "REVISION"
        if "amendment" in text:
            return True, "AMENDMENT"
        if "rectification" in text:
            return True, "RECTIFICATION"
        if "clarification" in text:
            return True, "CLARIFICATION"
        return False, None
