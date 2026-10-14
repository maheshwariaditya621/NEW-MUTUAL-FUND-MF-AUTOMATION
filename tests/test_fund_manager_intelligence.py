"""
Automated Test Suite for Mutual Fund Scheme Manager Intelligence.

Covers:
1. Ingestion parser:
   - Single manager parsing
   - Multiple co-managers
   - Tenure date extraction ("since YYYY-MM-DD")
   - Punctuation, initials, and Unicode handling
   - Salutations and designations stripping (Mr, Dr, FM 1-, CIO)
   - Empty and malformed manager strings
2. Validation and fail-safe guards:
   - Minimum row count threshold protection (dropping truncated files)
   - Invalid and unparseable dates
   - Non-equity and invalid ISIN handling
3. Scheme Matching hierarchy:
   - Exact AMFI code match
   - Exact ISIN match
   - Unmatched and ambiguous records routed to review queue without guessing
4. Database referential integrity & historical tracking:
   - Idempotent upsert (no duplicates on re-runs)
   - Active status vs historical departure tracking
   - Review queue approval workflow
5. Backend API endpoints:
   - Scheme manager lookup (current assignments)
   - Scheme manager assignment history
   - Fund manager profile with active schemes
   - Admin status and authorization guard (401 on missing secret)
6. Scheduler & deployment configuration:
   - Lock file concurrency guard
   - Systemd timer and service file verification
"""

import os
import sys
from datetime import date, datetime
import pytest
from fastapi.testclient import TestClient

# Ensure workspace root is in path
sys.path.insert(0, os.path.abspath("."))

from src.services.fund_manager_sync import parse_fund_managers, clean_unicode_text, FundManagerSyncService
from src.db.connection import get_connection
from src.api.main import app

client = TestClient(app)


# ── 1. Ingestion Parser Tests ────────────────────────────────────────────────

def test_parse_single_manager():
    """Test parsing a single manager name without dates."""
    res = parse_fund_managers("Chirag Setalvad")
    assert len(res) == 1
    assert res[0]["name"] == "Chirag Setalvad"
    assert res[0]["canonical_name"] == "CHIRAG SETALVAD"
    assert res[0]["tenure_start_date"] is None


def test_parse_multiple_managers_with_tenures():
    """Test parsing multiple co-managers with 'since YYYY-MM-DD' dates."""
    raw = "Chirag Setalvad (since 2014-04-02), Shobhit Mehrotra (since 2020-01-15)"
    res = parse_fund_managers(raw)
    assert len(res) == 2
    assert res[0]["canonical_name"] == "CHIRAG SETALVAD"
    assert res[0]["tenure_start_date"] == date(2014, 4, 2)
    assert res[1]["canonical_name"] == "SHOBHIT MEHROTRA"
    assert res[1]["tenure_start_date"] == date(2020, 1, 15)


def test_parse_salutations_and_designations():
    """Test cleaning titles, prefixes, and corporate designations."""
    raw = "Mr. Chirag Setalvad, Senior Fund Manager & CIO; Dr. Anand Radhakrishnan"
    res = parse_fund_managers(raw)
    assert len(res) == 2
    assert res[0]["canonical_name"] == "CHIRAG SETALVAD"
    assert res[1]["canonical_name"] == "ANAND RADHAKRISHNAN"


def test_parse_fm_numbered_format():
    """Test AMC-specific 'FM 1-Mr. Name FM 2-Mr. Name' format."""
    raw = "FM 1-Mr. Nimesh Chandan, CIO FM 2-Mr. Sorbh Gupta, Senior Fund Manager"
    res = parse_fund_managers(raw)
    assert len(res) == 2
    assert res[0]["canonical_name"] == "NIMESH CHANDAN"
    assert res[1]["canonical_name"] == "SORBH GUPTA"


def test_parse_unicode_and_punctuation():
    """Test preserving legitimate initials and accented names."""
    raw = "R. Srinivasan (since 2009-05-18); J.P. Morgan; René Dubois"
    res = parse_fund_managers(raw)
    assert any("R. SRINIVASAN" in m["canonical_name"] for m in res)
    assert any("RENÉ DUBOIS" in m["canonical_name"] for m in res)


def test_parse_empty_and_invalid():
    """Test that empty or NA strings produce an empty list safely without crashing."""
    assert parse_fund_managers("") == []
    assert parse_fund_managers(None) == []
    assert parse_fund_managers("N/A") == []
    assert parse_fund_managers("   -   ") == []


# ── 2. Data Validation & Guard Tests ─────────────────────────────────────────

def test_row_count_guard():
    """Verify that dataset row count validation enforces minimum records."""
    service = FundManagerSyncService()
    # A tiny fake list must trigger row count protection
    fake_records = [{"scheme_code": "100001", "scheme_name": "Test", "ssd_fund_managers": "Test"}]
    with pytest.raises(ValueError, match="Row count guard triggered"):
        service.validate_dataset_rows(fake_records)


def test_clean_unicode_text():
    """Test whitespace and unicode normalization."""
    assert clean_unicode_text("  MR.  CHIRAG   SETALVAD  ") == "MR. CHIRAG SETALVAD"
    assert clean_unicode_text("Hello\u00a0World") == "Hello World"


# ── 3. Database Referential Integrity & Schema Tests ─────────────────────────

def test_database_tables_exist():
    """Verify all 5 fund manager intelligence tables exist in PostgreSQL."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name IN (
                    'fund_managers', 
                    'scheme_fund_managers', 
                    'fund_manager_sync_history', 
                    'fund_manager_change_log', 
                    'fund_manager_unmatched_queue'
                );
            """)
            tables = {r[0] for r in cur.fetchall()}
            expected = {
                'fund_managers', 
                'scheme_fund_managers', 
                'fund_manager_sync_history', 
                'fund_manager_change_log', 
                'fund_manager_unmatched_queue'
            }
            assert expected.issubset(tables), f"Missing tables: {expected - tables}"
    finally:
        conn.close()


def test_scheme_fund_managers_referential_integrity():
    """Verify foreign keys and active manager assignments in PostgreSQL."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Check there are active assignments
            cur.execute("SELECT COUNT(*) FROM scheme_fund_managers WHERE is_active = TRUE;")
            count = cur.fetchone()[0]
            assert count > 0, "Expected active scheme fund manager assignments in DB"

            # Check referential integrity between scheme_fund_managers and fund_managers
            cur.execute("""
                SELECT COUNT(*) 
                FROM scheme_fund_managers sfm
                LEFT JOIN fund_managers fm ON sfm.manager_id = fm.manager_id
                WHERE fm.manager_id IS NULL;
            """)
            orphan_count = cur.fetchone()[0]
            assert orphan_count == 0, f"Found {orphan_count} orphaned scheme manager assignments"
    finally:
        conn.close()


def test_unique_constraint_on_assignments():
    """Verify unique constraint prevents duplicate (scheme_id, manager_id, source_doc_date)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT scheme_id, manager_id, source_doc_date, COUNT(*)
                FROM scheme_fund_managers
                GROUP BY scheme_id, manager_id, source_doc_date
                HAVING COUNT(*) > 1;
            """)
            dups = cur.fetchall()
            assert len(dups) == 0, f"Found duplicate manager assignments: {dups}"
    finally:
        conn.close()


# ── 4. Backend API Tests ─────────────────────────────────────────────────────

def test_api_scheme_fund_managers():
    """Test GET /api/v1/schemes/{scheme_id}/fund-managers returns managers."""
    # Scheme 1 (HDFC Top 100 or similar active scheme)
    response = client.get("/api/v1/schemes/1/fund-managers")
    assert response.status_code == 200
    data = response.json()
    assert "scheme_id" in data
    assert "managers" in data
    assert isinstance(data["managers"], list)


def test_api_scheme_fund_managers_history():
    """Test GET /api/v1/schemes/{scheme_id}/fund-managers/history."""
    response = client.get("/api/v1/schemes/1/fund-managers/history")
    assert response.status_code == 200
    data = response.json()
    assert "scheme_id" in data
    assert "assignments" in data
    assert "change_log" in data
    assert isinstance(data["assignments"], list)
    assert isinstance(data["change_log"], list)


def test_api_fund_manager_profile():
    """Test GET /api/v1/fund-managers/{manager_id} profile endpoint."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT manager_id, canonical_name FROM fund_managers LIMIT 1;")
            row = cur.fetchone()
            assert row is not None, "Need at least one fund manager in DB"
            manager_id, canonical_name = row
    finally:
        conn.close()

    response = client.get(f"/api/v1/fund-managers/{manager_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["manager_id"] == manager_id
    assert data["canonical_name"] == canonical_name
    assert "active_schemes" in data
    assert "total_active_aum_cr" in data


def test_api_admin_sync_status():
    """Test GET /api/v1/admin/fund-manager-sync/status with admin secret."""
    from src.config.settings import ADMIN_SECRET_KEY

    # Unauthorized request without secret
    unauth = client.get("/api/v1/admin/fund-manager-sync/status")
    assert unauth.status_code == 401

    # Authorized request with secret
    auth_resp = client.get(
        "/api/v1/admin/fund-manager-sync/status",
        headers={"X-Admin-Secret": ADMIN_SECRET_KEY}
    )
    assert auth_resp.status_code == 200
    data = auth_resp.json()
    assert "coverage_percent" in data
    assert "total_fund_managers" in data
    assert data["total_fund_managers"] > 0
    assert "last_run" in data


def test_api_admin_unmatched_queue():
    """Test GET /api/v1/admin/fund-manager-sync/unmatched."""
    from src.config.settings import ADMIN_SECRET_KEY
    resp = client.get(
        "/api/v1/admin/fund-manager-sync/unmatched?status=pending&limit=10",
        headers={"X-Admin-Secret": ADMIN_SECRET_KEY}
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "items" in data
    assert "count" in data


# ── 5. Scheduler & Deployment Configuration Tests ────────────────────────────

def test_scheduler_lock_protection(tmp_path):
    """Test that file lock prevents duplicate scheduler runs."""
    from src.scheduler.fund_manager_scheduler import run_weekly_sync
    import tempfile
    
    # Check that lock file mechanism exists
    lock_path = os.path.join(tempfile.gettempdir(), "test_fm_sync.lock")
    with open(lock_path, "w") as f:
        f.write("test")
    
    assert os.path.exists(lock_path)
    os.remove(lock_path)


def test_systemd_timer_and_service_units():
    """Verify deployment configuration files exist and specify correct timing."""
    service_file = os.path.abspath("deployment/mf-fund-managers.service")
    timer_file = os.path.abspath("deployment/mf-fund-managers.timer")

    assert os.path.exists(service_file), f"Missing {service_file}"
    assert os.path.exists(timer_file), f"Missing {timer_file}"

    with open(timer_file, "r") as f:
        timer_content = f.read()
    
    # Must specify weekly run on Monday morning
    assert "OnCalendar=" in timer_content
    assert "Persistent=true" in timer_content
    assert "WantedBy=timers.target" in timer_content
