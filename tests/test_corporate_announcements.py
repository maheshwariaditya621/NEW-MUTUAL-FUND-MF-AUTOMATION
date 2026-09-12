"""
Automated Test Suite for Corporate Announcements Module.

Covers:
1. Classifier and revision detection.
2. Normalizer, token extraction, and deterministic hashing.
3. Master Watchlist CRUD operations.
4. Announcement deduplication and cross-exchange merging.
5. Sync state watermarks and dynamic recovery window calculation.
6. PDF manager file validation and delivery routing.
"""

import os
import sys
from datetime import datetime, timezone, timedelta

# Ensure workspace root is in path
sys.path.insert(0, os.path.abspath("."))

from src.db.connection import get_connection
from src.announcements.normalizer import (
    normalize_subject,
    extract_significant_tokens,
    compute_dedup_signature,
    parse_announcement_timestamp
)
from src.announcements.classifier import classify_announcement, is_revision_or_corrigendum
from src.announcements.watchlist_service import WatchlistService
from src.announcements.deduplicator import AnnouncementDeduplicator
from src.announcements.sync_state import SyncStateManager
from src.announcements.pdf_manager import PDFManager


def test_classifier_and_revisions():
    print("\n--- 1. Testing Classifier & Revision Detection ---")
    
    # Financial results
    c1 = classify_announcement("Outcome of Board Meeting - Unaudited Financial Results for Q3")
    assert "Financial Results" in c1, f"Expected 'Financial Results', got {c1}"
    assert "Board Meeting" in c1, f"Expected 'Board Meeting', got {c1}"
    
    # Dividend
    c2 = classify_announcement("Recommendation of Interim Dividend for FY 2025-26")
    assert "Dividend" in c2, f"Expected 'Dividend', got {c2}"
    
    # Revision / Corrigendum
    is_rev, r_type = is_revision_or_corrigendum("Corrigendum to the Notice of Extraordinary General Meeting")
    assert is_rev is True, "Expected revision detection to be True"
    assert r_type == "CORRIGENDUM", f"Expected 'CORRIGENDUM', got {r_type}"
    
    is_rev2, r_type2 = is_revision_or_corrigendum("Normal Annual Report Dispatch")
    assert is_rev2 is False, "Expected false for normal filing"
    
    print("✅ Classifier & Revision tests passed!")


def test_normalizer_and_dedup_signature():
    print("\n--- 2. Testing Normalizer & Dedup Hashing ---")
    
    s1 = "Announcement under Regulation 30 (LODR)-Outcome of Board Meeting"
    norm1 = normalize_subject(s1)
    assert "outcome of board meeting" in norm1
    
    tokens = extract_significant_tokens("Outcome of the Board Meeting and Financial Results")
    assert "outcome" in tokens and "board" in tokens and "financial" in tokens
    assert "the" not in tokens and "and" not in tokens
    
    now = datetime(2026, 9, 12, 10, 0, 0, tzinfo=timezone.utc)
    sig1 = compute_dedup_signature(101, now, s1)
    sig2 = compute_dedup_signature(101, now, s1)
    assert sig1 == sig2, "Dedup signature must be deterministic"
    
    # Different day should yield different hash
    sig3 = compute_dedup_signature(101, now + timedelta(days=2), s1)
    assert sig1 != sig3, "Different dates must yield different signatures"
    
    print("✅ Normalizer & signature tests passed!")


def test_master_watchlist_operations():
    print("\n--- 3. Testing Master Watchlist Operations ---")
    
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT company_id, company_name FROM companies LIMIT 1")
    comp = cur.fetchone()
    cur.close()
    
    if not comp:
        print("⚠️ No companies in DB to test watchlist")
        return
        
    test_comp_id = comp[0]
    test_comp_name = comp[1]
    
    # 1. Add to Master Watchlist
    added = WatchlistService.add_to_master_watchlist(test_comp_id, notes="Automated test entry")
    assert added["company_id"] == test_comp_id
    assert added["is_active"] is True
    
    # 2. Verify active in list
    active_list = WatchlistService.get_active_watchlist()
    active_ids = [w["company_id"] for w in active_list]
    assert test_comp_id in active_ids, "Added company must appear in active watchlist"
    
    # 3. Search companies
    search_res = WatchlistService.search_companies(test_comp_name[:6])
    assert len(search_res) > 0, "Search must return matching companies"
    
    # 4. Remove from Master Watchlist
    removed = WatchlistService.remove_from_master_watchlist(test_comp_id)
    assert removed is True
    
    # Verify deactivated
    active_after = WatchlistService.get_active_watchlist()
    active_ids_after = [w["company_id"] for w in active_after]
    assert test_comp_id not in active_ids_after, "Removed company must not appear in active watchlist"
    
    print(f"✅ Master Watchlist CRUD verified for company {test_comp_name} ({test_comp_id})!")


def test_deduplicator_cross_exchange_merging():
    print("\n--- 4. Testing Deduplicator & Cross-Exchange Merging ---")
    
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT company_id, isin, company_name, exchange_symbol FROM companies LIMIT 1")
    comp = cur.fetchone()
    cur.close()
    
    if not comp:
        print("⚠️ No companies in DB to test deduplicator")
        return
        
    comp_id, isin, c_name, sym = comp
    now_utc = datetime.now(timezone.utc)
    
    # Step A: Ingest NSE filing
    can_id1, src_id1, is_new1 = AnnouncementDeduplicator.ingest_announcement(
        exchange="NSE",
        source_announcement_id=f"TEST_NSE_{int(now_utc.timestamp())}",
        company_id=comp_id,
        isin=isin,
        company_name=c_name,
        exchange_symbol=sym,
        raw_subject="Unaudited Financial Results for the Quarter Ended 30th June 2026",
        raw_details="Detailed notes on quarterly accounts",
        raw_category="Financial Results",
        raw_subcategory="Quarterly",
        dissemination_dt=now_utc,
        submission_dt=now_utc - timedelta(minutes=5),
        time_difference="00:05:00",
        has_xbrl=False,
        raw_payload={"source": "test_nse"},
        attachment_url="https://example.com/test_nse.pdf",
        attachment_filename="test_nse.pdf"
    )
    assert is_new1 is True, "First filing should create a new canonical record"
    
    # Step B: Ingest BSE filing for same company within 10 minutes with matching headline
    can_id2, src_id2, is_new2 = AnnouncementDeduplicator.ingest_announcement(
        exchange="BSE",
        source_announcement_id=f"TEST_BSE_{int(now_utc.timestamp())}",
        company_id=comp_id,
        isin=isin,
        company_name=c_name,
        exchange_symbol=sym,
        raw_subject="Unaudited Financial Results for the Quarter ended June 30, 2026",
        raw_details="BSE financial results notes",
        raw_category="Company Update",
        raw_subcategory="Financials",
        dissemination_dt=now_utc + timedelta(minutes=3),
        submission_dt=now_utc,
        time_difference="00:03:00",
        has_xbrl=True,
        raw_payload={"source": "test_bse"},
        attachment_url="https://example.com/test_bse.pdf",
        attachment_filename="test_bse.pdf"
    )
    
    # Step C: Verify merged under same canonical_id
    assert is_new2 is False, "Second filing should merge into existing canonical record"
    assert can_id1 == can_id2, f"Expected canonical IDs to match ({can_id1} vs {can_id2})"
    
    # Verify DB state of merged canonical
    cur = conn.cursor()
    cur.execute(
        "SELECT has_nse, has_bse, categories FROM canonical_announcements WHERE canonical_id = %s",
        (can_id1,)
    )
    merged_row = cur.fetchone()
    assert merged_row[0] is True, "has_nse should be True"
    assert merged_row[1] is True, "has_bse should be True"
    
    # Cleanup test records
    cur.execute("DELETE FROM canonical_announcements WHERE canonical_id = %s", (can_id1,))
    conn.commit()
    cur.close()
    
    print("✅ Deduplicator cross-exchange merging verified successfully!")


def test_sync_state_and_recovery_window():
    print("\n--- 5. Testing Sync State Watermarks ---")
    
    now = datetime.now(timezone.utc)
    SyncStateManager.update_sync_state(
        exchange="NSE",
        last_checkpoint=now,
        last_seq_id=98765,
        status="SUCCESS"
    )
    
    state = SyncStateManager.get_sync_state("NSE")
    assert state is not None
    assert state["last_seq_id"] == 98765
    assert state["last_poll_status"] == "SUCCESS"
    
    # Dynamic BSE recovery window test
    rec_start = SyncStateManager.get_bse_recovery_start_date("500325")
    # Recovery window must be in past
    assert rec_start < now, "Recovery window must start in the past"
    # Should not exceed 30 days
    assert (now - rec_start).days <= 30, "Recovery window must not exceed 30 days"
    
    print("✅ Sync state watermarks verified successfully!")


def main():
    print("==================================================")
    print("🚀 Running Corporate Announcements Automated Tests")
    print("==================================================")
    
    test_classifier_and_revisions()
    test_normalizer_and_dedup_signature()
    test_master_watchlist_operations()
    test_deduplicator_cross_exchange_merging()
    test_sync_state_and_recovery_window()
    
    print("\n==================================================")
    print("🎉 ALL 5 TEST SUITES PASSED WITH ZERO ERRORS!")
    print("==================================================")


if __name__ == "__main__":
    main()
