"""
Data Coverage Utility.

Determines if the latest period has complete data from all AMCs,
or if only a subset of AMCs have uploaded for the latest month.

Logic:
- Counts distinct AMCs (not scheme counts) that have uploaded snapshots.
- Compares how many AMCs uploaded for the LATEST period vs the PREVIOUS period.
- "Partial" = at least 1 AMC that uploaded last month hasn't uploaded yet this month.
- "Complete" = 100% of last month's AMCs have also uploaded this month.

The warning disappears AUTOMATICALLY once all AMCs upload.
The live AMC count increments as each new AMC submits data.
"""

from datetime import date
from psycopg2.extensions import cursor
from typing import Optional
from src.config import logger


def get_period_coverage(cur: cursor) -> dict:
    """
    Returns AMC-level coverage info for the latest two periods.

    An AMC is "uploaded" for a period when at least one scheme_snapshot
    exists for any of its schemes in that period.

    Returns:
        {
            "latest": {
                "year": 2026, "month": 2, "label": "FEB-26",
                "amc_count": 5, "period_id": 45
            },
            "prev": {
                "year": 2026, "month": 1, "label": "JAN-26",
                "amc_count": 42, "period_id": 44
            },
            "coverage_pct": 11.9,       # (latest_amc_count / prev_amc_count) * 100
            "amcs_uploaded": 5,         # AMCs that have uploaded for latest month
            "amcs_expected": 42,        # AMCs that uploaded last month (= expected)
            "amcs_pending": 37,         # expected - uploaded
            "is_partial": True,         # True if amcs_uploaded < amcs_expected
        }
    """
    try:
        cur.execute(
            """
            SELECT period_id, year, month
            FROM periods
            ORDER BY year DESC, month DESC
            LIMIT 2
            """
        )
        periods = cur.fetchall()

        if not periods:
            return _empty_coverage()

        latest_pid, latest_yr, latest_mo = periods[0]
        latest_label = date(latest_yr, latest_mo, 1).strftime("%b-%y").upper()

        if len(periods) < 2:
            # Only one period in DB — treat as complete, no comparison possible
            return {
                "latest": {
                    "year": latest_yr, "month": latest_mo,
                    "label": latest_label, "amc_count": 0, "period_id": latest_pid
                },
                "prev": None,
                "coverage_pct": 100.0,
                "amcs_uploaded": 0,
                "amcs_expected": 0,
                "amcs_pending": 0,
                "is_partial": False,
            }

        prev_pid, prev_yr, prev_mo = periods[1]
        prev_label = date(prev_yr, prev_mo, 1).strftime("%b-%y").upper()

        # Count distinct AMCs that have at least one snapshot for each period.
        # Also fetch their names so we can show the uploaded/pending lists.
        cur.execute(
            """
            SELECT
                COUNT(DISTINCT s.amc_id) FILTER (WHERE ss.period_id = %s) AS latest_amc_count,
                COUNT(DISTINCT s.amc_id) FILTER (WHERE ss.period_id = %s) AS prev_amc_count
            FROM scheme_snapshots ss
            JOIN schemes s ON ss.scheme_id = s.scheme_id
            WHERE ss.period_id IN (%s, %s)
            """,
            (latest_pid, prev_pid, latest_pid, prev_pid)
        )
        row = cur.fetchone()
        amcs_uploaded = row[0] if row else 0   # AMCs that have uploaded this month
        amcs_expected = row[1] if row else 0   # AMCs that uploaded last month

        # Fetch the AMC names that uploaded last month (expected set)
        cur.execute(
            """
            SELECT DISTINCT a.amc_name
            FROM scheme_snapshots ss
            JOIN schemes s ON ss.scheme_id = s.scheme_id
            JOIN amcs a ON s.amc_id = a.amc_id
            WHERE ss.period_id = %s
            ORDER BY a.amc_name
            """,
            (prev_pid,)
        )
        prev_amc_names = set(row[0] for row in cur.fetchall())

        # Fetch the AMC names that have uploaded this month
        cur.execute(
            """
            SELECT DISTINCT a.amc_name
            FROM scheme_snapshots ss
            JOIN schemes s ON ss.scheme_id = s.scheme_id
            JOIN amcs a ON s.amc_id = a.amc_id
            WHERE ss.period_id = %s
            ORDER BY a.amc_name
            """,
            (latest_pid,)
        )
        latest_amc_names = set(row[0] for row in cur.fetchall())

        # Pending = uploaded last month but NOT yet uploaded this month
        pending_amc_names = sorted(prev_amc_names - latest_amc_names)
        uploaded_amc_names = sorted(latest_amc_names)

        amcs_uploaded = len(uploaded_amc_names)
        amcs_expected = len(prev_amc_names)

        if amcs_expected == 0:
            # No previous data to compare against — treat as complete
            coverage_pct = 100.0
            is_partial = False
        else:
            coverage_pct = round((amcs_uploaded / amcs_expected) * 100, 1)
            # Partial = any expected AMC hasn't uploaded yet (threshold = 100%)
            is_partial = amcs_uploaded < amcs_expected

        amcs_pending = max(0, amcs_expected - amcs_uploaded)

        return {
            "latest": {
                "year": latest_yr, "month": latest_mo,
                "label": latest_label, "amc_count": amcs_uploaded, "period_id": latest_pid
            },
            "prev": {
                "year": prev_yr, "month": prev_mo,
                "label": prev_label, "amc_count": amcs_expected, "period_id": prev_pid
            },
            "coverage_pct": coverage_pct,
            "amcs_uploaded": amcs_uploaded,
            "amcs_expected": amcs_expected,
            "amcs_pending": amcs_pending,
            "uploaded_amc_names": uploaded_amc_names,
            "pending_amc_names": pending_amc_names,
            "is_partial": is_partial,
        }

    except Exception as e:
        logger.error(f"Failed to get period coverage: {e}")
        return _empty_coverage()


def get_data_warning(coverage: dict) -> Optional[dict]:
    """
    Returns a data_warning dict if not all AMCs have uploaded yet, else None.

    The warning disappears automatically once amcs_uploaded == amcs_expected.
    """
    if not coverage.get("is_partial"):
        return None

    latest = coverage["latest"]
    prev = coverage.get("prev")
    prev_label = prev["label"] if prev else "previous month"

    amcs_uploaded = coverage.get("amcs_uploaded", 0)
    amcs_expected = coverage.get("amcs_expected", 0)
    amcs_pending = coverage.get("amcs_pending", 0)
    coverage_pct = coverage.get("coverage_pct", 0)
    uploaded_amc_names = coverage.get("uploaded_amc_names", [])
    pending_amc_names = coverage.get("pending_amc_names", [])

    return {
        "is_partial": True,
        "coverage_pct": coverage_pct,
        "amcs_uploaded": amcs_uploaded,
        "amcs_expected": amcs_expected,
        "amcs_pending": amcs_pending,
        "uploaded_amc_names": uploaded_amc_names,
        "pending_amc_names": pending_amc_names,
        "latest_label": latest["label"],
        "complete_label": prev_label,
        "message": (
            f"{latest['label']} data is incomplete — "
            f"{amcs_uploaded} of {amcs_expected} AMCs have uploaded "
            f"({amcs_pending} still pending). "
            f"Summary figures reflect {prev_label} (last complete month)."
        )
    }


def _empty_coverage() -> dict:
    """Return a safe default when coverage cannot be determined."""
    return {
        "latest": None,
        "prev": None,
        "coverage_pct": 100.0,
        "amcs_uploaded": 0,
        "amcs_expected": 0,
        "amcs_pending": 0,
        "is_partial": False,
    }
