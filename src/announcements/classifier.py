"""
Deterministic Multi-Tag Classifier for Corporate Announcements
"""
import re
from typing import List

CATEGORY_RULES = {
    "Dividend": [
        r"\bdividend\b",
        r"\binterim\s+div\b",
        r"\bfinal\s+div\b",
        r"\bspecial\s+div\b",
        r"\brecord\s+date\s+for\s+dividend\b",
    ],
    "Bonus": [
        r"\bbonus\s+issue\b",
        r"\bbonus\s+shares\b",
        r"\ballotment\s+of\s+bonus\b",
    ],
    "Stock Split": [
        r"\bsub-division\b",
        r"\bsub\s+division\b",
        r"\bsplit\b",
        r"\bface\s+value\s+reduction\b",
        r"\bstock\s+split\b",
    ],
    "Rights Issue": [
        r"\brights\s+issue\b",
        r"\brights\s+entitlement\b",
        r"\bdraft\s+letter\s+of\s+offer\b",
    ],
    "Buyback": [
        r"\bbuyback\b",
        r"\bbuy-back\b",
        r"\btender\s+offer\b",
        r"\brepurchase\s+of\s+shares\b",
    ],
    "M&A": [
        r"\bamalgamation\b",
        r"\bmerger\b",
        r"\bdemerger\b",
        r"\bacquisition\b",
        r"\btakeover\b",
        r"\bscheme\s+of\s+arrangement\b",
        r"\bjoint\s+venture\b",
        r"\bjv\b",
    ],
    "Board Meeting": [
        r"\bboard\s+meeting\b",
        r"\bmeeting\s+of\s+board\b",
        r"\bbm\s+outcome\b",
        r"\boutcome\s+of\s+meeting\b",
        r"\bboard\s+to\s+consider\b",
    ],
    "Financial Results": [
        r"\bfinancial\s+results\b",
        r"\baudited\s+results\b",
        r"\bunaudited\s+results\b",
        r"\bquarterly\s+results\b",
        r"\blimited\s+review\b",
        r"\bfinancial\s+statements\b",
    ],
    "Fund Raising": [
        r"\bfund\s+raising\b",
        r"\bqip\b",
        r"\bpreferential\s+allotment\b",
        r"\bwarrants\b",
        r"\bcommercial\s+paper\b",
        r"\bdebentures\b",
        r"\bncd\b",
    ],
    "Credit Rating": [
        r"\bcredit\s+rating\b",
        r"\bcrisil\b",
        r"\bicra\b",
        r"\bcare\b",
        r"\bind-ra\b",
        r"\bdowngrade\b",
        r"\bupgrade\b",
    ],
    "Management Changes": [
        r"\bresignation\b",
        r"\bappointment\b",
        r"\bcessation\b",
        r"\bkmp\b",
        r"\bchief\s+executive\b",
        r"\bcfo\b",
        r"\bceo\b",
        r"\bdirector\b",
        r"\bauditor\b",
    ],
    "Investor/Analyst Meet": [
        r"\binvestor\s+meet\b",
        r"\banalyst\s+meet\b",
        r"\bearnings\s+call\b",
        r"\bconcall\b",
        r"\bpresentation\b",
        r"\binstitutional\s+investors\b",
    ],
    "Orders/Contracts": [
        r"\bbagged\s+order\b",
        r"\baward\s+of\s+contract\b",
        r"\bwork\s+order\b",
        r"\bagreement\s+signed\b",
        r"\bloi\s+received\b",
        r"\border\s+win\b",
    ],
    "Regulatory/Legal": [
        r"\bsebi\b",
        r"\bshow\s+cause\b",
        r"\bpenalty\b",
        r"\blitigation\b",
        r"\bcourt\s+order\b",
        r"\bnclt\b",
        r"\benforcement\b",
    ],
    "Business Updates": [
        r"\bpress\s+release\b",
        r"\boperational\s+update\b",
        r"\bcapacity\s+expansion\b",
        r"\bcommissioning\b",
        r"\bgeneral\s+updates\b",
    ],
}

# Precompile regex patterns
COMPILED_RULES = {
    cat: [re.compile(p, re.IGNORECASE) for p in patterns]
    for cat, patterns in CATEGORY_RULES.items()
}

REVISION_PATTERNS = [
    re.compile(r"\bcorrigendum\b", re.IGNORECASE),
    re.compile(r"\brevised\b", re.IGNORECASE),
    re.compile(r"\brevision\b", re.IGNORECASE),
    re.compile(r"\bamendment\b", re.IGNORECASE),
    re.compile(r"\baddendum\b", re.IGNORECASE),
    re.compile(r"\brectification\b", re.IGNORECASE),
    re.compile(r"\bclarification\b", re.IGNORECASE),
    re.compile(r"\bcancellation\b", re.IGNORECASE),
]

def classify_announcement(subject: str, *args, **kwargs) -> List[str]:
    """
    Returns list of matching category tags. Fallback to ['Other'].
    Accepts any number of additional string fields (category, subcategory, details).
    """
    parts = [str(subject)] if subject else []
    for a in args:
        if a:
            parts.append(str(a))
    for v in kwargs.values():
        if v:
            parts.append(str(v))

    text = " ".join(parts).strip()
    if not text:
        return ["Other"]
        
    tags = []
    for cat, regexes in COMPILED_RULES.items():
        if any(r.search(text) for r in regexes):
            tags.append(cat)
            
    return tags if tags else ["Other"]

def detect_revision(subject: str, details: str = "") -> tuple[bool, str]:
    """
    Detects if the filing is a revision or corrigendum.
    Returns (is_revision, revision_type).
    """
    text = f"{subject} {details}".strip()
    for r in REVISION_PATTERNS:
        match = r.search(text)
        if match:
            found = match.group(0).upper()
            if "CORRIGENDUM" in found:
                return True, "CORRIGENDUM"
            if "REVIS" in found:
                return True, "REVISED"
            if "AMEND" in found or "ADDENDUM" in found:
                return True, "AMENDMENT"
            if "CLARIF" in found:
                return True, "CLARIFICATION"
            if "CANCEL" in found:
                return True, "CANCELLATION"
            return True, "OTHER_REVISION"
            
    return False, None

is_revision_or_corrigendum = detect_revision
