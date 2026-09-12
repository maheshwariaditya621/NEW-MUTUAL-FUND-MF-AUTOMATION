"""
Deduplicator and Canonical Merger for Corporate Announcements.

Handles:
1. Deterministic signature matching (SHA-256).
2. Cross-exchange fuzzy matching (same company, within 4h window, high token overlap).
3. Revision/corrigendum linking to parent canonical announcements.
4. Idempotent insertion and merging into canonical_announcements, announcement_sources, and announcement_attachments.
"""

from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple
import difflib
import psycopg2.extras

from src.db.connection import get_connection
from src.config import logger
from src.announcements.normalizer import (
    normalize_text,
    extract_significant_tokens,
    compute_dedup_signature
)
from src.announcements.classifier import classify_announcement, is_revision_or_corrigendum


class AnnouncementDeduplicator:
    """Merges exchange filings into canonical announcements and links revisions."""

    @staticmethod
    def _token_similarity(s1: str, s2: str) -> float:
        """Calculate token Jaccard similarity and sequence matcher ratio."""
        t1 = set(extract_significant_tokens(s1))
        t2 = set(extract_significant_tokens(s2))
        if not t1 or not t2:
            return 0.0

        jaccard = len(t1 & t2) / len(t1 | t2)
        seq_ratio = difflib.SequenceMatcher(None, normalize_text(s1), normalize_text(s2)).ratio()
        # Weighted combination: 60% token overlap, 40% sequence match
        return 0.6 * jaccard + 0.4 * seq_ratio

    @staticmethod
    def find_matching_canonical(
        company_id: int,
        primary_dt: datetime,
        normalized_subject: str,
        dedup_sig: str
    ) -> Optional[int]:
        """
        Check if an announcement matches an existing canonical announcement.
        1. Direct match on dedup_signature.
        2. Fuzzy cross-exchange match: same company_id, within 4 hours, similarity >= 0.85.
        """
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                # 1. Exact signature match
                cur.execute(
                    "SELECT canonical_id FROM canonical_announcements WHERE dedup_signature = %s",
                    (dedup_sig,)
                )
                row = cur.fetchone()
                if row:
                    return row[0]

                # 2. Windowed candidate search (within 4 hours)
                window_start = primary_dt - timedelta(hours=4)
                window_end = primary_dt + timedelta(hours=4)
                cur.execute(
                    """
                    SELECT canonical_id, title
                    FROM canonical_announcements
                    WHERE company_id = %s
                      AND primary_timestamp BETWEEN %s AND %s
                    """,
                    (company_id, window_start, window_end)
                )
                candidates = cur.fetchall()
                for cand_id, cand_title in candidates:
                    sim = AnnouncementDeduplicator._token_similarity(normalized_subject, cand_title)
                    if sim >= 0.85:
                        return cand_id

            return None
        except Exception as e:
            logger.error(f"Error finding matching canonical for company {company_id}: {e}")
            return None

    @staticmethod
    def find_parent_canonical_for_revision(
        company_id: int,
        primary_dt: datetime,
        subject: str,
        lookback_days: int = 14
    ) -> Optional[int]:
        """
        If an announcement is a revision/corrigendum, search for the original parent filing
        within the past lookback_days for the same company.
        """
        conn = get_connection()
        try:
            lookback_start = primary_dt - timedelta(days=lookback_days)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT canonical_id, title
                    FROM canonical_announcements
                    WHERE company_id = %s
                      AND primary_timestamp BETWEEN %s AND %s
                      AND is_revision = FALSE
                    ORDER BY primary_timestamp DESC
                    LIMIT 25
                    """,
                    (company_id, lookback_start, primary_dt)
                )
                candidates = cur.fetchall()
                best_id = None
                best_sim = 0.0

                for cand_id, cand_title in candidates:
                    sim = AnnouncementDeduplicator._token_similarity(subject, cand_title)
                    if sim > best_sim and sim >= 0.65:
                        best_sim = sim
                        best_id = cand_id

                return best_id
        except Exception as e:
            logger.error(f"Error finding parent revision for company {company_id}: {e}")
            return None

    @staticmethod
    def ingest_announcement(
        exchange: str,
        source_announcement_id: str,
        company_id: int,
        isin: str,
        company_name: str,
        exchange_symbol: Optional[str],
        raw_subject: str,
        raw_details: Optional[str],
        raw_category: Optional[str],
        raw_subcategory: Optional[str],
        dissemination_dt: datetime,
        submission_dt: Optional[datetime],
        time_difference: Optional[str],
        has_xbrl: bool,
        raw_payload: Dict[str, Any],
        attachment_url: Optional[str],
        attachment_filename: Optional[str]
    ) -> Tuple[int, int, bool]:
        """
        Atomically process and insert an announcement into:
        1. canonical_announcements
        2. announcement_sources
        3. announcement_attachments (if attachment present)

        Returns: (canonical_id, source_id, is_new_canonical)
        """
        conn = get_connection()
        is_new_canonical = False

        try:
            # 1. Check if this exact source filing already exists
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT source_id, canonical_id 
                    FROM announcement_sources 
                    WHERE exchange = %s AND source_announcement_id = %s
                    """,
                    (exchange, source_announcement_id)
                )
                existing_source = cur.fetchone()
                if existing_source:
                    # Idempotent skip
                    return existing_source[1], existing_source[0], False

            # 2. Analyze categories and revision status
            categories = classify_announcement(raw_subject, raw_category, raw_subcategory, raw_details)
            is_rev, rev_type = is_revision_or_corrigendum(raw_subject)

            parent_canonical_id = None
            if is_rev:
                parent_canonical_id = AnnouncementDeduplicator.find_parent_canonical_for_revision(
                    company_id=company_id,
                    primary_dt=dissemination_dt,
                    subject=raw_subject
                )

            dedup_sig = compute_dedup_signature(company_id, dissemination_dt, raw_subject)

            # 3. Find matching canonical or create a new one
            canonical_id = AnnouncementDeduplicator.find_matching_canonical(
                company_id=company_id,
                primary_dt=dissemination_dt,
                normalized_subject=raw_subject,
                dedup_sig=dedup_sig
            )

            with conn.cursor() as cur:
                if canonical_id:
                    # Merge with existing canonical
                    update_sql = """
                        UPDATE canonical_announcements
                        SET has_nse = (has_nse OR %s),
                            has_bse = (has_bse OR %s),
                            categories = ARRAY(SELECT DISTINCT unnest(categories || %s::text[])),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE canonical_id = %s
                    """
                    cur.execute(
                        update_sql,
                        (exchange == "NSE", exchange == "BSE", categories, canonical_id)
                    )
                else:
                    # Insert new canonical record
                    insert_can_sql = """
                        INSERT INTO canonical_announcements (
                            company_id, isin, company_name, primary_timestamp,
                            title, summary_text, has_nse, has_bse, categories,
                            is_revision, revision_type, parent_canonical_id, dedup_signature
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (dedup_signature) DO UPDATE SET
                            has_nse = (canonical_announcements.has_nse OR EXCLUDED.has_nse),
                            has_bse = (canonical_announcements.has_bse OR EXCLUDED.has_bse),
                            updated_at = CURRENT_TIMESTAMP
                        RETURNING canonical_id
                    """
                    cur.execute(
                        insert_can_sql,
                        (
                            company_id, isin, company_name, dissemination_dt,
                            raw_subject.strip(), raw_details.strip() if raw_details else None,
                            exchange == "NSE", exchange == "BSE", categories,
                            is_rev, rev_type, parent_canonical_id, dedup_sig
                        )
                    )
                    row = cur.fetchone()
                    canonical_id = row[0]
                    is_new_canonical = True

                # 4. Insert announcement_sources record
                insert_source_sql = """
                    INSERT INTO announcement_sources (
                        canonical_id, exchange, source_announcement_id, company_id,
                        exchange_symbol, raw_subject, raw_details, raw_category,
                        raw_subcategory, submission_timestamp, dissemination_timestamp,
                        time_difference, has_xbrl, raw_payload
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    RETURNING source_id
                """
                cur.execute(
                    insert_source_sql,
                    (
                        canonical_id, exchange, source_announcement_id, company_id,
                        exchange_symbol, raw_subject, raw_details, raw_category,
                        raw_subcategory, submission_dt, dissemination_dt,
                        time_difference, has_xbrl, psycopg2.extras.Json(raw_payload)
                    )
                )
                source_id = cur.fetchone()[0]

                # 5. Insert attachment metadata if provided
                if attachment_url and attachment_filename:
                    insert_att_sql = """
                        INSERT INTO announcement_attachments (
                            source_id, canonical_id, exchange, original_file_name,
                            source_file_url, lifecycle_stage, is_locally_available
                        ) VALUES (
                            %s, %s, %s, %s, %s, 'HOT', FALSE
                        )
                        ON CONFLICT (source_id, source_file_url) DO NOTHING
                    """
                    cur.execute(
                        insert_att_sql,
                        (source_id, canonical_id, exchange, attachment_filename, attachment_url)
                    )

            conn.commit()
            return canonical_id, source_id, is_new_canonical

        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to ingest announcement for {exchange} {source_announcement_id}: {e}")
            raise
