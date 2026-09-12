"""
Text & Timestamp Normalizer for Corporate Announcements
"""
import re
import hashlib
from datetime import datetime, timezone, timedelta
from typing import List, Optional

IST = timezone(timedelta(hours=5, minutes=30))

STOP_WORDS = {
    "a", "an", "the", "and", "or", "of", "to", "in", "on", "at", "for", "with", "by",
    "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "do",
    "does", "did", "as", "from", "that", "this", "these", "those", "it", "its"
}


def parse_nse_timestamp(dt_str: str) -> datetime:
    """
    Parses NSE timestamp formats:
    - '12-Sep-2026 22:26:11'
    - '12-Sep-2026 22:26'
    Returns timezone-aware UTC datetime.
    """
    if not dt_str:
        return datetime.now(timezone.utc)
        
    s = dt_str.strip()
    for fmt in ("%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            dt_ist = dt.replace(tzinfo=IST)
            return dt_ist.astimezone(timezone.utc)
        except ValueError:
            continue
            
    return datetime.now(timezone.utc)


def parse_bse_timestamp(dt_str: str) -> datetime:
    """
    Parses BSE timestamp formats:
    - '2026-09-11T18:17:39.39'
    - '2026-09-11T18:17:39'
    Returns timezone-aware UTC datetime.
    """
    if not dt_str:
        return datetime.now(timezone.utc)
        
    s = dt_str.strip()
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S"
    ):
        try:
            dt = datetime.strptime(s, fmt)
            dt_ist = dt.replace(tzinfo=IST)
            return dt_ist.astimezone(timezone.utc)
        except ValueError:
            continue
            
    return datetime.now(timezone.utc)


def parse_announcement_timestamp(dt_str: Optional[str]) -> datetime:
    """Unified timestamp parser that tries NSE, BSE, and ISO formats."""
    if not dt_str:
        return datetime.now(timezone.utc)
    s = str(dt_str).strip()
    if "-" in s and any(month in s for month in ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")):
        return parse_nse_timestamp(s)
    return parse_bse_timestamp(s)


def normalize_subject(text: str) -> str:
    """
    Normalizes subject text for hashing and fuzzy comparison:
    - Lowercases text
    - Strips common regulatory prefixes
    - Removes punctuation and normalizes multiple spaces
    """
    if not text:
        return ""
        
    s = text.lower()
    
    # Strip common boilerplate prefixes
    prefixes = [
        r"^announcement\s+under\s+regulation\s+30\s*(\(lodr\))?[-:\s]*",
        r"^disclosure\s+under\s+regulation\s+30\s*(\(lodr\))?[-:\s]*",
        r"^intimation\s+under\s+regulation\s+30\s*(\(lodr\))?[-:\s]*",
        r"^intimation\s+of\s+",
        r"^outcome\s+of\s+(the\s+)?board\s+meeting[-:\s]*",
        r"^press\s+release[-:\s]*",
        r"^updates?[-:\s]*",
    ]
    for p in prefixes:
        s = re.sub(p, "", s)
        
    # Replace non-alphanumeric characters with space
    s = re.sub(r"[^a-z0-9]", " ", s)
    # Collapse multiple spaces
    s = re.sub(r"\s+", " ", s).strip()

    if not s:
        fallback = re.sub(r"[^a-z0-9]", " ", text.lower())
        return re.sub(r"\s+", " ", fallback).strip()

    return s


normalize_text = normalize_subject


def extract_significant_tokens(text: str) -> List[str]:
    """Extract filtered tokens for Jaccard similarity comparison."""
    norm = re.sub(r"[^a-z0-9]", " ", text.lower())
    tokens = norm.split()
    return [t for t in tokens if len(t) > 2 and t not in STOP_WORDS]


def compute_dedup_signature(company_id: int, dissem_utc: datetime, raw_subject: str) -> str:
    """
    Computes deterministic SHA256 signature for canonical deduplication.
    Uses company_id, calendar date (in IST), and normalized subject prefix.
    """
    dissem_ist = dissem_utc.astimezone(IST)
    cal_date_str = dissem_ist.strftime("%Y-%m-%d")
    norm_sub = normalize_subject(raw_subject)[:100]
    
    key = f"{company_id}_{cal_date_str}_{norm_sub}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()
