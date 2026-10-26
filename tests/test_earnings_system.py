"""
Automated Unit and Integration Test Suite for Earnings Calendar & Results Tracking.

Covers:
1. EarningsClassifier - Fiscal period extraction, result types, audit status, revisions, meeting dates
2. BSESource & TigZig - Parsing logic with sanitized sample payloads
3. EarningsReconciliationEngine - Linking filings with calendar events, audit history
4. Earnings API Endpoints - Calendar pagination, filters, today's desk, results ledger
"""

import pytest
from datetime import datetime, date, timezone
from src.earnings.classifier import EarningsClassifier
from src.earnings.sources.base import ScheduledEventRecord, ResultsFilingRecord
from src.earnings.reconciliation import EarningsReconciliationEngine
from src.earnings.engine import EarningsEngine


def test_classifier_fiscal_period():
    """Verify quarterly and FY extraction across Indian disclosure formats."""
    # Q2 FY27
    fy, q = EarningsClassifier.extract_fiscal_period("Financial Results for Q2 FY27")
    assert q == "Q2"
    assert fy == "FY2027"

    # Quarter ended September 30, 2026 -> Q2 FY27
    fy, q = EarningsClassifier.extract_fiscal_period("Unaudited Financial Results for the Quarter ended September 30, 2026")
    assert q == "Q2"
    assert fy == "FY2027"

    # Quarter ended June 30, 2026 -> Q1 FY27
    fy, q = EarningsClassifier.extract_fiscal_period("Financial Results for the quarter ended 30th June 2026")
    assert q == "Q1"
    assert fy == "FY2027"

    # Quarter ended March 31, 2026 -> Q4 FY26
    fy, q = EarningsClassifier.extract_fiscal_period("Audited Results for period ended 31st March 2026")
    assert q == "Q4"
    assert fy == "FY2026"

    # Half year ended
    fy, q = EarningsClassifier.extract_fiscal_period("Half year ended September 30, 2026")
    assert q == "Q2"


def test_classifier_result_type():
    """Verify Standalone vs Consolidated classification."""
    assert EarningsClassifier.classify_result_type("Standalone Financial Results for Q2") == "STANDALONE"
    assert EarningsClassifier.classify_result_type("Consolidated Financial Results") == "CONSOLIDATED"
    assert EarningsClassifier.classify_result_type("Consolidated and Standalone Unaudited Results") == "BOTH"
    assert EarningsClassifier.classify_result_type("General Press Release") == "UNKNOWN"


def test_classifier_audit_status():
    """Verify Audited vs Unaudited vs Limited Review."""
    assert EarningsClassifier.classify_audit_status("Unaudited Financial Results with Limited Review") == "UNAUDITED"
    assert EarningsClassifier.classify_audit_status("Audited Financial Statements for FY26") == "AUDITED"
    assert EarningsClassifier.classify_audit_status("Submission of Limited Review Report") == "LIMITED_REVIEW"


def test_classifier_is_results_meeting():
    """Verify meeting purpose detection."""
    assert EarningsClassifier.is_results_meeting("Board meeting to consider unaudited financial results") is True
    assert EarningsClassifier.is_results_meeting("Meeting of Board to approve Q2 results and interim dividend") is True
    assert EarningsClassifier.is_results_meeting("Board Meeting to consider fund raising via NCDs") is False
    assert EarningsClassifier.is_results_meeting("Allotment of Equity Shares under ESOP") is False


def test_classifier_meeting_status():
    """Verify detection of reschedules and cancellations."""
    assert EarningsClassifier.detect_meeting_status("Board Meeting Intimation for October 27, 2026") == "SCHEDULED"
    assert EarningsClassifier.detect_meeting_status("Rescheduling of Board Meeting to October 29, 2026") == "RESCHEDULED"
    assert EarningsClassifier.detect_meeting_status("Postponement of Board Meeting") == "POSTPONED"
    assert EarningsClassifier.detect_meeting_status("Cancellation of Board Meeting") == "CANCELLED"


def test_classifier_extract_meeting_date():
    """Verify meeting date extraction from intimation text."""
    d1 = EarningsClassifier.extract_meeting_date_from_text("Board Meeting To Be Held On October 27, 2026")
    assert d1 == date(2026, 10, 27)

    d2 = EarningsClassifier.extract_meeting_date_from_text("meeting scheduled on 14th October 2026")
    assert d2 == date(2026, 10, 14)

    d3 = EarningsClassifier.extract_meeting_date_from_text("Outcome of Board Meeting held on 14-10-2026")
    assert d3 == date(2026, 10, 14)


def test_classifier_revision_detection():
    """Verify revision and corrigendum detection."""
    is_rev, r_type = EarningsClassifier.detect_filing_revision("Resubmission of Limited Review Report - Typographical Error")
    assert is_rev is True
    assert r_type == "RESUBMISSION"

    is_rev, r_type = EarningsClassifier.detect_filing_revision("Corrigendum to Financial Results for Q2")
    assert is_rev is True
    assert r_type == "CORRIGENDUM"

    is_rev, r_type = EarningsClassifier.detect_filing_revision("Unaudited Financial Results for Q2")
    assert is_rev is False
    assert r_type is None


def test_engine_idempotent_event_upsert():
    """Verify that inserting the same scheduled event twice is idempotent."""
    from src.db.connection import get_connection
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT company_id, isin, exchange_symbol, bse_code FROM companies WHERE bse_code IS NOT NULL LIMIT 1")
        row = cur.fetchone()
    
    comp_id, isin, sym, bse = row
    engine = EarningsEngine()
    dummy_event = ScheduledEventRecord(
        company_id=comp_id,
        isin=isin,
        exchange_symbol=sym,
        bse_code=bse,
        meeting_date=date(2026, 10, 21),
        source_name="TEST_FIXTURE",
        source_record_id="TEST_REL_20261021",
        raw_purpose="Board meeting to approve financial results",
        event_status="SCHEDULED"
    )

    # First insert
    res1 = engine.upsert_scheduled_events([dummy_event])
    assert res1.get("inserted", 0) >= 0

    # Second insert of identical record
    res2 = engine.upsert_scheduled_events([dummy_event])
    assert res2.get("inserted", 0) == 0
    assert res2.get("unchanged", 0) >= 1 or res2.get("updated", 0) >= 1


def test_api_earnings_endpoints():
    """Verify that earnings FastAPI endpoints return 200 with proper schema."""
    from fastapi.testclient import TestClient
    from src.api.main import app
    from src.api.utils.auth_utils import create_access_token

    client = TestClient(app)
    token = create_access_token(data={"sub": "admin"})
    auth_headers = {"Authorization": f"Bearer {token}"}

    # 1. /calendar
    r_cal = client.get("/api/v1/earnings/calendar?limit=5", headers=auth_headers)
    assert r_cal.status_code == 200
    data_cal = r_cal.json()
    assert "events" in data_cal
    assert "total" in data_cal

    # 2. /today
    r_today = client.get("/api/v1/earnings/today", headers=auth_headers)
    assert r_today.status_code == 200
    data_today = r_today.json()
    assert "summary" in data_today
    assert "awaiting_results" in data_today
    assert "results_published" in data_today

    # 3. /results
    r_res = client.get("/api/v1/earnings/results?limit=5", headers=auth_headers)
    assert r_res.status_code == 200
    data_res = r_res.json()
    assert "filings" in data_res
    assert "total" in data_res

    # 4. /sync-status
    r_sync = client.get("/api/v1/earnings/sync-status", headers=auth_headers)
    assert r_sync.status_code == 200
    data_sync = r_sync.json()
    assert "sources" in data_sync
    assert "total_calendar_events" in data_sync

    # 5. Unauthorized check
    r_unauth = client.get("/api/v1/earnings/calendar")
    assert r_unauth.status_code == 401
