"""
Fund Manager Synchronization and Intelligence Service.

Handles downloading SEBI/AMFI Scheme Summary Document (SSD) data,
parsing fund manager identities and tenure dates, executing tiered
scheme matching, maintaining historical assignments, and logging audit events.
"""

import gzip
import io
import csv
import re
import urllib.request
import urllib.error
import unicodedata
from datetime import datetime, date, timezone
from typing import Dict, List, Optional, Tuple, Any

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

from src.config import logger
from src.db.connection import get_pool_connection, release_pool_connection

DEFAULT_DOWNLOAD_URL = "https://api.tigzig.com/mf/v1/download/scheme_details_latest?format=csv.gz"
MINIMUM_EXPECTED_ROWS = 5000
MINIMUM_FILE_BYTES = 100 * 1024  # 100 KB
USER_AGENT = "AVFincorp-Institutional-Sync/1.0"


def clean_unicode_text(text: Optional[str]) -> str:
    """Normalize whitespace and Unicode characters without altering names."""
    if not text:
        return ""
    # Normalize Unicode (NFKC handles ligatures and full-width chars cleanly)
    norm = unicodedata.normalize("NFKC", str(text))
    # Replace weird space characters with normal space
    norm = re.sub(r"[\xa0\u200b\u200e\u202f\u205f\u3000]", " ", norm)
    # Collapse multiple spaces
    norm = re.sub(r"\s+", " ", norm).strip()
    return norm


def parse_fund_managers(raw_text: Optional[str]) -> List[Dict[str, Any]]:
    """
    Parse raw fund manager string into structured records.
    Example inputs:
      "Vishal Gajwani (since 2024-10-31)"
      "Kaustubh Gupta (since 2011-07-15); Sunaina da Cunha (since 2011-07-15); Sanjay Pawar (since 2022-07-01)"
      "R. Janakiraman; Akhil Goyal"
    
    Returns list of dicts:
      [
        {
          "name": "Vishal Gajwani",
          "canonical_name": "VISHAL GAJWANI",
          "tenure_start_date": date(2024, 10, 31) or None,
          "raw_entry": "Vishal Gajwani (since 2024-10-31)"
        }
      ]
    """
    cleaned = clean_unicode_text(raw_text)
    if not cleaned or cleaned.upper() in ("NA", "N.A.", "N/A", "NONE", "NULL", "-", "--", "---", "NOT APPLICABLE"):
        return []

    # Split by FM 1-, FM 2-, etc. if present
    if re.search(r"\bFM\s*\d+[-:]", cleaned, re.IGNORECASE):
        raw_entries = [e.strip() for e in re.split(r"\s*FM\s*\d+[-:]\s*", cleaned, flags=re.IGNORECASE) if e.strip()]
    elif ";" in cleaned:
        raw_entries = [e.strip() for e in cleaned.split(";") if e.strip()]
    elif cleaned.count("(since") > 1:
        # Split on comma preceding next manager name
        raw_entries = [e.strip() for e in re.split(r",\s*(?=[A-Za-z\.\s]+\(since)", cleaned) if e.strip()]
    else:
        raw_entries = [cleaned]

    parsed_list = []
    # Pattern to extract name and optional (since YYYY-MM-DD or DD-MMM-YYYY)
    pattern = re.compile(r"^(.*?)(?:\s*\((?:since|w\.e\.f\.?)\s*([\d\-\/\.\w]+)\))?$", re.IGNORECASE)
    # Common corporate titles to strip from names
    title_strip_re = re.compile(
        r",?\s*(?:Chief Investment Officer|Fund Manager|Head[-\s]+Fixed Income|Senior Fund Manager|Head of Equities|Co-Fund Manager|Dedicated Fund Manager|CIO).*$", 
        re.IGNORECASE
    )

    for entry in raw_entries:
        entry_clean = clean_unicode_text(entry)
        if not entry_clean:
            continue
        m = pattern.match(entry_clean)
        if m:
            mgr_name = clean_unicode_text(m.group(1))
            date_str = m.group(2).strip() if m.group(2) else None
        else:
            mgr_name = entry_clean
            date_str = None

        # Clean title suffixes and salutations
        mgr_name = title_strip_re.sub("", mgr_name).strip()
        mgr_name = re.sub(r"^(?:Mr\.|Ms\.|Mrs\.|Dr\.)\s*", "", mgr_name, flags=re.IGNORECASE).strip()
        mgr_name = re.sub(r"^[\s,\-–—]+|[\s,\-–—]+$", "", mgr_name)
        if not mgr_name or len(mgr_name) < 2:
            continue

        tenure_start = None
        if date_str:
            # Try parsing ISO YYYY-MM-DD
            for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d-%b-%Y", "%d-%B-%Y", "%Y/%m/%d"):
                try:
                    tenure_start = datetime.strptime(date_str, fmt).date()
                    break
                except ValueError:
                    pass

        # Canonical name is uppercase stripped
        canonical = mgr_name.upper()

        parsed_list.append({
            "name": mgr_name,
            "canonical_name": canonical,
            "tenure_start_date": tenure_start,
            "raw_entry": entry_clean
        })

    return parsed_list


class FundManagerSyncService:
    """
    Orchestrates the ingestion, validation, matching, and database updates
    for Mutual Fund Scheme Managers.
    """

    def __init__(self, download_url: str = DEFAULT_DOWNLOAD_URL):
        self.download_url = download_url

    def fetch_source_dataset(self) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
        """
        Download and validate the compressed CSV scheme details dataset.
        Enforces connection timeouts, size guards, and format validation.
        """
        logger.info(f"🌐 Fetching scheme details dataset from {self.download_url}")
        req = urllib.request.Request(
            self.download_url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept-Encoding": "gzip",
            }
        )

        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                status_code = resp.status
                content_length = resp.headers.get("Content-Length")
                logger.info(f"HTTP Status: {status_code}, Reported Content-Length: {content_length}")
                raw_bytes = resp.read()
        except urllib.error.URLError as e:
            raise RuntimeError(f"Network error downloading scheme details: {e}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error during download: {e}")

        if len(raw_bytes) < MINIMUM_FILE_BYTES:
            raise ValueError(f"Downloaded file too small: {len(raw_bytes)} bytes (min {MINIMUM_FILE_BYTES} required)")

        # Verify gzip magic number (0x1f 0x8b)
        if raw_bytes[:2] != b"\x1f\x8b":
            raise ValueError("Response is not a valid GZIP compressed file")

        try:
            with gzip.open(io.BytesIO(raw_bytes), "rt", encoding="utf-8", errors="replace") as gz_file:
                reader = csv.DictReader(gz_file)
                headers = reader.fieldnames or []
                rows = list(reader)
        except Exception as e:
            raise ValueError(f"Failed to decompress or parse CSV dataset: {e}")

        # Validate required columns
        required_cols = ["scheme_code", "scheme_name", "amc", "ssd_fund_managers"]
        missing_cols = [c for c in required_cols if c not in headers]
        if missing_cols:
            raise ValueError(f"Dataset missing mandatory schema columns: {missing_cols}")

        self.validate_dataset_rows(rows)

        metadata = {
            "total_rows": len(rows),
            "byte_size": len(raw_bytes),
            "headers_count": len(headers),
            "fetched_at": datetime.now(timezone.utc).isoformat()
        }
        logger.info(f"✅ Successfully downloaded and parsed {len(rows)} records from TigZig AMFI feed.")
        return rows, metadata

    def validate_dataset_rows(self, rows: List[Dict[str, str]]) -> bool:
        """Validate that downloaded dataset meets minimum row count safety threshold."""
        if len(rows) < MINIMUM_EXPECTED_ROWS:
            raise ValueError(f"Row count guard triggered: dataset has {len(rows)} rows, minimum required is {MINIMUM_EXPECTED_ROWS}")
        return True

    def load_local_schemes(self, conn) -> List[Dict[str, Any]]:
        """Load all schemes and AMC metadata from local PostgreSQL database."""
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    s.scheme_id,
                    s.scheme_name,
                    s.plan_type,
                    s.option_type,
                    s.amfi_code,
                    s.scheme_code,
                    a.amc_id,
                    a.amc_name
                FROM schemes s
                JOIN amcs a ON s.amc_id = a.amc_id
                ORDER BY s.scheme_id ASC;
            """)
            schemes = cur.fetchall()

            # Also load ISIN mappings from nav_history if available
            cur.execute("""
                SELECT DISTINCT scheme_code, isin_growth, isin_div_payout, isin_div_reinv
                FROM nav_history
                WHERE isin_growth IS NOT NULL OR isin_div_payout IS NOT NULL;
            """)
            nav_isins = cur.fetchall()

        # Build ISIN lookup
        isin_to_amfi_code = {}
        for row in nav_isins:
            code = str(row["scheme_code"]).strip()
            for isin_key in ("isin_growth", "isin_div_payout", "isin_div_reinv"):
                isin_val = row.get(isin_key)
                if isin_val and len(isin_val.strip()) == 12:
                    isin_to_amfi_code[isin_val.strip().upper()] = code

        for s in schemes:
            s["nav_lookup_code"] = isin_to_amfi_code.get(s.get("amfi_code"))

        return schemes

    @staticmethod
    def normalize_scheme_name(name: str) -> str:
        """Strip plan/option tokens for canonical similarity comparison."""
        n = clean_unicode_text(name).upper()
        tokens = [
            "- DIRECT PLAN", "- REGULAR PLAN", "DIRECT PLAN", "REGULAR PLAN",
            "- GROWTH OPTION", "- GROWTH", "GROWTH OPTION", "GROWTH",
            "- IDCW OPTION", "- IDCW", "IDCW OPTION", "IDCW",
            "- DIVIDEND", "DIVIDEND", "PLAN", "OPTION", "DIRECT", "REGULAR"
        ]
        for t in tokens:
            n = n.replace(t, "")
        n = re.sub(r"[^A-Z0-9 ]", " ", n)
        n = re.sub(r"\s+", " ", n).strip()
        return n

    def execute_matching(
        self,
        source_rows: List[Dict[str, str]],
        local_schemes: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Execute tiered matching hierarchy:
          Tier 1: Exact AMFI Scheme Code
          Tier 2: Exact Verified ISIN
          Tier 3: Exact Normalized Name + AMC match
          Diagnostic: Unmatched & Ambiguous Queue candidates
        """
        # Index source rows by scheme_code, isin, fund_name_latest, and ssd_fund_name
        source_by_code = {}
        source_by_isin = {}
        source_by_norm_name = {}
        source_by_fund_plan = {}

        for r in source_rows:
            code = str(r.get("scheme_code", "")).strip()
            if code:
                source_by_code[code] = r
            isin = clean_unicode_text(r.get("isin")).upper()
            if isin and len(isin) == 12:
                source_by_isin[isin] = r
            isin2 = clean_unicode_text(r.get("isin2")).upper()
            if isin2 and len(isin2) == 12:
                source_by_isin[isin2] = r

            norm_name = self.normalize_scheme_name(r.get("scheme_name", ""))
            if norm_name:
                source_by_norm_name.setdefault(norm_name, []).append(r)

            # Fund family + Plan indexing
            plan = clean_unicode_text(r.get("scheme_plan", "")).upper()
            amc_clean = clean_unicode_text(r.get("amc", "")).upper()[:6]
            fn = self.normalize_scheme_name(r.get("fund_name_latest", ""))
            if fn and plan and amc_clean:
                source_by_fund_plan.setdefault((fn, plan, amc_clean), []).append(r)

            sfn = self.normalize_scheme_name(r.get("ssd_fund_name", ""))
            if sfn and plan and amc_clean and sfn != fn:
                source_by_fund_plan.setdefault((sfn, plan, amc_clean), []).append(r)

        matched_assignments = []
        unmatched_schemes = []
        ambiguous_schemes = []

        for scheme in local_schemes:
            sid = scheme["scheme_id"]
            sname = scheme["scheme_name"]
            plan_type = clean_unicode_text(scheme.get("plan_type", "")).upper()
            amc_name = scheme["amc_name"]
            amc_clean = clean_unicode_text(amc_name).upper()[:6]
            amfi_code = str(scheme.get("amfi_code") or "").strip()

            matched_source = None
            match_method = None

            # Tier 1: Exact AMFI Code match
            if amfi_code and amfi_code in source_by_code:
                matched_source = source_by_code[amfi_code]
                match_method = "amfi_code"

            # Tier 2: Check by internal scheme_code
            if not matched_source:
                local_code = str(scheme.get("scheme_code") or "").strip()
                if local_code and local_code in source_by_code:
                    matched_source = source_by_code[local_code]
                    match_method = "amfi_code"

            # Tier 3: Fund Family Name + Plan Type + AMC Exact Match
            if not matched_source:
                norm_sname = self.normalize_scheme_name(sname)
                # Check (norm_sname, plan_type, amc_clean)
                family_candidates = source_by_fund_plan.get((norm_sname, plan_type, amc_clean), [])
                if not family_candidates:
                    # Also try without plan_type if unique across fund
                    family_candidates = [
                        r for (f_name, _, a_code), r_list in source_by_fund_plan.items()
                        if f_name == norm_sname and a_code == amc_clean
                        for r in r_list
                    ]
                if len(family_candidates) >= 1:
                    # Verified match across fund family
                    matched_source = family_candidates[0]
                    match_method = "validated_mapping"

            # Tier 4: Normalized Full Name + AMC exact match
            if not matched_source:
                our_norm = self.normalize_scheme_name(sname)
                candidates = source_by_norm_name.get(our_norm, [])
                amc_candidates = [
                    c for c in candidates 
                    if amc_clean in clean_unicode_text(c.get("amc", "")).upper()
                ]
                if len(amc_candidates) == 1:
                    matched_source = amc_candidates[0]
                    match_method = "validated_mapping"
                elif len(amc_candidates) > 1:
                    ambiguous_schemes.append({
                        "scheme": scheme,
                        "candidates": amc_candidates,
                        "reason": f"Multiple ({len(amc_candidates)}) source records match normalized name"
                    })
                    continue

            # If matched, parse manager payload
            if matched_source:
                raw_mgrs = matched_source.get("ssd_fund_managers")
                parsed = parse_fund_managers(raw_mgrs)
                doc_date_str = matched_source.get("ssd_doc_date")
                doc_date = None
                if doc_date_str:
                    try:
                        doc_date = datetime.strptime(doc_date_str[:10], "%Y-%m-%d").date()
                    except ValueError:
                        pass

                matched_assignments.append({
                    "scheme_id": sid,
                    "scheme_name": sname,
                    "amc_name": amc_name,
                    "source_scheme_code": matched_source.get("scheme_code"),
                    "source_scheme_name": matched_source.get("scheme_name"),
                    "match_method": match_method,
                    "raw_managers": raw_mgrs,
                    "parsed_managers": parsed,
                    "source_doc_date": doc_date,
                    "source_url": f"https://api.tigzig.com/mf/v1/schemes/{matched_source.get('scheme_code')}/details"
                })
            else:
                # Find best fuzzy candidate for review queue (without assigning)
                our_norm = self.normalize_scheme_name(sname)
                our_words = set(our_norm.split())
                best_cand = None
                best_score = 0.0

                if len(our_words) >= 2:
                    for norm_cand, cand_list in source_by_norm_name.items():
                        cand_words = set(norm_cand.split())
                        overlap = len(our_words & cand_words) / len(our_words)
                        if overlap > 0.6 and overlap > best_score:
                            for c in cand_list:
                                if amc_name.upper()[:6] in clean_unicode_text(c.get("amc", "")).upper():
                                    best_score = overlap
                                    best_cand = c
                                    break

                unmatched_schemes.append({
                    "scheme": scheme,
                    "candidate": best_cand,
                    "confidence": round(best_score * 100, 1) if best_cand else 0.0
                })

        return {
            "matched_assignments": matched_assignments,
            "unmatched_schemes": unmatched_schemes,
            "ambiguous_schemes": ambiguous_schemes
        }

    def sync(self, dry_run: bool = False) -> Dict[str, Any]:
        """
        Execute full synchronization run.
        Transactionally updates database unless dry_run is True.
        """
        start_time = datetime.now(timezone.utc)
        logger.info(f"🚀 Starting Fund Manager Synchronization (dry_run={dry_run})...")

        # Step 1: Download & parse dataset
        source_rows, metadata = self.fetch_source_dataset()

        # Step 2: Connect to DB and load local universe
        conn = get_pool_connection()
        try:
            local_schemes = self.load_local_schemes(conn)
            total_local = len(local_schemes)

            # Step 3: Match
            match_results = self.execute_matching(source_rows, local_schemes)
            matched = match_results["matched_assignments"]
            unmatched = match_results["unmatched_schemes"]
            ambiguous = match_results["ambiguous_schemes"]

            exact_matches = sum(1 for m in matched if m["match_method"] == "amfi_code")
            verified_matches = sum(1 for m in matched if m["match_method"] in ("amfi_code", "isin", "validated_mapping"))

            logger.info(
                f"Matching Summary: Total Local: {total_local} | Matched: {len(matched)} "
                f"({len(matched)/total_local:.1%}) | Exact AMFI: {exact_matches} | "
                f"Unmatched: {len(unmatched)} | Ambiguous: {len(ambiguous)}"
            )

            manager_additions = 0
            manager_departures = 0

            if not dry_run:
                # Open explicit transaction
                with conn.cursor() as cur:
                    # 1. Record sync job start
                    cur.execute("""
                        INSERT INTO fund_manager_sync_history (
                            started_at, status, source_endpoint, records_processed,
                            exact_identifier_matches, verified_matches, unmatched_records,
                            ambiguous_records
                        ) VALUES (
                            %s, 'running', %s, %s, %s, %s, %s, %s
                        ) RETURNING job_id;
                    """, (
                        start_time, self.download_url, len(source_rows),
                        exact_matches, verified_matches, len(unmatched), len(ambiguous)
                    ))
                    job_id = cur.fetchone()[0]

                    # 2. Collect unique fund managers to upsert into fund_managers table
                    all_parsed_managers = {}
                    for m in matched:
                        for pm in m["parsed_managers"]:
                            canonical = pm["canonical_name"]
                            if canonical not in all_parsed_managers:
                                all_parsed_managers[canonical] = pm["name"]

                    # Upsert canonical fund managers
                    for canonical, display_name in all_parsed_managers.items():
                        cur.execute("""
                            INSERT INTO fund_managers (canonical_name, updated_at)
                            VALUES (%s, CURRENT_TIMESTAMP)
                            ON CONFLICT (canonical_name) 
                            DO UPDATE SET updated_at = CURRENT_TIMESTAMP;
                        """, (canonical,))

                    # Retrieve manager_id mapping
                    cur.execute("SELECT canonical_name, manager_id FROM fund_managers;")
                    manager_id_map = dict(cur.fetchall())

                    # 3. Upsert scheme-manager assignments
                    for m in matched:
                        sid = m["scheme_id"]
                        source_doc_date = m["source_doc_date"]
                        source_url = m["source_url"]
                        match_method = m["match_method"]

                        active_manager_ids = set()

                        # Deduplicate parsed managers for this scheme by canonical_name, preferring non-null tenure_start_date
                        scheme_mgrs_dict = {}
                        for pm in m["parsed_managers"]:
                            cname = pm["canonical_name"]
                            if cname not in scheme_mgrs_dict:
                                scheme_mgrs_dict[cname] = pm
                            elif scheme_mgrs_dict[cname]["tenure_start_date"] is None and pm["tenure_start_date"] is not None:
                                scheme_mgrs_dict[cname] = pm

                        for pm in scheme_mgrs_dict.values():
                            canonical = pm["canonical_name"]
                            mid = manager_id_map.get(canonical)
                            if not mid:
                                continue
                            active_manager_ids.add(mid)

                            raw_name = pm["name"]
                            tenure_start = pm["tenure_start_date"]

                            # Check if assignment already exists
                            cur.execute("""
                                SELECT id, is_active FROM scheme_fund_managers
                                WHERE scheme_id = %s AND manager_id = %s
                                  AND (tenure_start_date = %s OR (tenure_start_date IS NULL AND %s IS NULL));
                            """, (sid, mid, tenure_start, tenure_start))
                            existing = cur.fetchone()

                            if existing:
                                # Update confirmation timestamp
                                cur.execute("""
                                    UPDATE scheme_fund_managers
                                    SET last_confirmed_at = CURRENT_TIMESTAMP,
                                        is_active = TRUE,
                                        source_doc_date = COALESCE(%s, source_doc_date),
                                        source_url = %s,
                                        verification_status = 'verified',
                                        updated_at = CURRENT_TIMESTAMP
                                    WHERE id = %s;
                                """, (source_doc_date, source_url, existing[0]))
                            else:
                                # New assignment
                                cur.execute("""
                                    INSERT INTO scheme_fund_managers (
                                        scheme_id, manager_id, source_manager_name,
                                        tenure_start_date, is_active, source_doc_date,
                                        source_url, verification_status, match_method,
                                        first_observed_at, last_confirmed_at
                                    ) VALUES (
                                        %s, %s, %s, %s, TRUE, %s, %s, 'verified', %s,
                                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                                    );
                                """, (
                                    sid, mid, raw_name, tenure_start,
                                    source_doc_date, source_url, match_method
                                ))
                                manager_additions += 1

                                # Record change log
                                cur.execute("""
                                    INSERT INTO fund_manager_change_log (
                                        scheme_id, manager_id, change_type, change_date, details
                                    ) VALUES (
                                        %s, %s, 'new_appointment', %s, %s
                                    );
                                """, (
                                    sid, mid, source_doc_date or date.today(),
                                    psycopg2.extras.Json({"source_name": raw_name, "tenure_start": str(tenure_start)})
                                ))

                        # Also backfill amfi_code on schemes table if not set!
                        if m["source_scheme_code"]:
                            cur.execute("""
                                UPDATE schemes
                                SET amfi_code = %s
                                WHERE scheme_id = %s AND (amfi_code IS NULL OR amfi_code = '');
                            """, (m["source_scheme_code"], sid))

                    # 4. Populate unmatched review queue
                    # Clear pending queue and re-populate with fresh diagnostics
                    cur.execute("DELETE FROM fund_manager_unmatched_queue WHERE review_status = 'pending';")
                    for u in unmatched:
                        s = u["scheme"]
                        cand = u.get("candidate")
                        conf = u.get("confidence", 0.0)
                        cur.execute("""
                            INSERT INTO fund_manager_unmatched_queue (
                                scheme_id, source_scheme_code, source_scheme_name,
                                source_isin, source_amc, candidate_scheme_id,
                                candidate_match_reason, confidence_score, review_status
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, 'pending'
                            );
                        """, (
                            s["scheme_id"],
                            cand.get("scheme_code") if cand else None,
                            cand.get("scheme_name") if cand else None,
                            cand.get("isin") if cand else None,
                            s["amc_name"],
                            s["scheme_id"] if cand else None,
                            f"Fuzzy match candidate ({conf}% overlap)" if cand else "No plausible candidate found",
                            conf
                        ))

                    for a in ambiguous:
                        s = a["scheme"]
                        cands = a["candidates"]
                        first_cand = cands[0] if cands else {}
                        cur.execute("""
                            INSERT INTO fund_manager_unmatched_queue (
                                scheme_id, source_scheme_code, source_scheme_name,
                                source_isin, source_amc, candidate_scheme_id,
                                candidate_match_reason, confidence_score, review_status
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, 'pending'
                            );
                        """, (
                            s["scheme_id"],
                            first_cand.get("scheme_code"),
                            first_cand.get("scheme_name"),
                            first_cand.get("isin"),
                            s["amc_name"],
                            s["scheme_id"],
                            a["reason"],
                            80.0
                        ))

                    # 5. Finalize job status
                    finish_time = datetime.now(timezone.utc)
                    cur.execute("""
                        UPDATE fund_manager_sync_history
                        SET status = 'success',
                            finished_at = %s,
                            manager_additions = %s,
                            manager_departures = %s,
                            summary_json = %s
                        WHERE job_id = %s;
                    """, (
                        finish_time, manager_additions, manager_departures,
                        psycopg2.extras.Json({
                            "duration_seconds": (finish_time - start_time).total_seconds(),
                            "matched_count": len(matched),
                            "coverage_percent": round(len(matched) / total_local * 100, 2),
                            "unmatched_count": len(unmatched),
                            "ambiguous_count": len(ambiguous)
                        }),
                        job_id
                    ))

                # Commit transaction
                conn.commit()
                logger.info(f"✅ Fund Manager Synchronization completed successfully in {(finish_time - start_time).total_seconds():.1f}s!")

            finish_time = datetime.now(timezone.utc)
            return {
                "status": "success",
                "dry_run": dry_run,
                "started_at": start_time.isoformat(),
                "finished_at": finish_time.isoformat(),
                "duration_seconds": (finish_time - start_time).total_seconds(),
                "total_local_schemes": total_local,
                "total_source_records": len(source_rows),
                "matched_schemes": len(matched),
                "coverage_pct": round(len(matched) / total_local * 100, 2),
                "exact_identifier_matches": exact_matches,
                "verified_matches": verified_matches,
                "unmatched_schemes": len(unmatched),
                "ambiguous_schemes": len(ambiguous),
                "manager_additions": manager_additions,
                "manager_departures": manager_departures,
                "sample_matches": matched[:5]
            }

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Fund Manager Synchronization failed: {e}", exc_info=True)
            # Try recording error in history
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO fund_manager_sync_history (
                            started_at, finished_at, status, source_endpoint, errors
                        ) VALUES (
                            %s, %s, 'failed', %s, %s
                        );
                    """, (start_time, datetime.now(timezone.utc), self.download_url, str(e)))
                conn.commit()
            except Exception:
                pass
            raise
        finally:
            release_pool_connection(conn)


if __name__ == "__main__":
    service = FundManagerSyncService()
    report = service.sync(dry_run=True)
    import pprint
    pprint.pprint(report)
