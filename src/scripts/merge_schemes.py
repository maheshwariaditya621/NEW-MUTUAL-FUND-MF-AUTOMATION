import argparse
import sys
from src.db.connection import get_connection
from src.config import logger
from rapidfuzz import fuzz

def manual_merge(source_id: int, target_id: int):
    """Merges source_id INTO target_id, deleting source_id."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Check if they belong to same AMC
        cur.execute("SELECT amc_id, scheme_name, plan_type, option_type, is_reinvest FROM schemes WHERE scheme_id = %s", (source_id,))
        src_res = cur.fetchone()
        cur.execute("SELECT amc_id, scheme_name, plan_type, option_type, is_reinvest FROM schemes WHERE scheme_id = %s", (target_id,))
        tgt_res = cur.fetchone()
        
        if not src_res or not tgt_res:
            print("Error: One or both scheme IDs do not exist.")
            sys.exit(1)
            
        if src_res[0] != tgt_res[0]:
            print("Error: Schemes belong to different AMCs.")
            sys.exit(1)
            
        print(f"Merging:\n  Source [{source_id}]: {src_res[1]}\n  -> Target [{target_id}]: {tgt_res[1]}")
        
        # 1. Reassign snapshots (Handling conflicts if snapshot for period already exists in target)
        # We need to find periods in source that are NOT in target and move them.
        cur.execute("SELECT snapshot_id, period_id FROM scheme_snapshots WHERE scheme_id = %s", (source_id,))
        src_snapshots = cur.fetchall()
        
        moved_count = 0
        for s_id, p_id in src_snapshots:
            # Check if target already has this period
            cur.execute("SELECT 1 FROM scheme_snapshots WHERE scheme_id = %s AND period_id = %s", (target_id, p_id))
            if cur.fetchone():
                print(f"  Conflict: Target already has data for period_id {p_id}. Deleting source redundant snapshot {s_id}.")
                # Since we don't want to double count, we just delete the redundant source snapshot (cascade deletes holdings)
                cur.execute("DELETE FROM scheme_snapshots WHERE snapshot_id = %s", (s_id,))
            else:
                print(f"  Moving snapshot {s_id} (Period {p_id}) to target scheme {target_id}.")
                cur.execute("UPDATE scheme_snapshots SET scheme_id = %s WHERE snapshot_id = %s", (target_id, s_id))
                moved_count += 1

        # 1.5 Reassign nav_history records
        cur.execute("UPDATE nav_history SET scheme_id = %s WHERE scheme_id = %s", (target_id, source_id))
        nav_updated = cur.rowcount
        print(f"  Moved {nav_updated} nav_history records.")
                
        # 2. Add rule to alias mapping table (Tier 1 resolution)
        cur.execute(
            """
            INSERT INTO scheme_aliases (amc_id, alias_name, canonical_scheme_id, plan_type, option_type, is_reinvest, approved_by, detection_method)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'MANUAL_MERGE')
            ON CONFLICT (amc_id, alias_name, plan_type, option_type, is_reinvest) 
            DO UPDATE SET canonical_scheme_id = EXCLUDED.canonical_scheme_id
            """,
            (src_res[0], src_res[1], target_id, src_res[2], src_res[3], src_res[4], 'manual')
        )
                
        # 3. Delete source scheme
        cur.execute("DELETE FROM schemes WHERE scheme_id = %s", (source_id,))
        
        conn.commit()
        print(f"Successfully merged. Moved {moved_count} unique snapshots. Source scheme deleted.")
        print(f"Registered alias '{src_res[1]}' -> {target_id} for future extractions.")
        
    except Exception as e:
        conn.rollback()
        print(f"Error during merge: {e}")
    finally:
        cur.close()

def get_product_type(name: str) -> str:
    """Detects if a scheme is an ETF, Index Fund, or Standard Mutual Fund."""
    name_upper = name.upper()
    if " ETF" in name_upper or "ETF " in name_upper or name_upper.endswith("ETF"):
        return "ETF"
    if any(x in name_upper for x in ["INDEX FUND", "INDX FUND", "INDX"]):
        return "INDEX"
    return "STANDARD"

def calculate_detailed_overlap(cur, scheme_id_1, scheme_id_2):
    """
    Calculates detailed overlap between two schemes.
    Returns (isin_match_pc, weight_match_pc, common_count)
    """
    # Get latest snapshot for each
    cur.execute("SELECT snapshot_id FROM scheme_snapshots WHERE scheme_id = %s ORDER BY period_id DESC LIMIT 1", (scheme_id_1,))
    res1 = cur.fetchone()
    cur.execute("SELECT snapshot_id FROM scheme_snapshots WHERE scheme_id = %s ORDER BY period_id DESC LIMIT 1", (scheme_id_2,))
    res2 = cur.fetchone()
    
    if not res1 or not res2:
        return 0, 0, 0
    
    snap1, snap2 = res1[0], res2[0]
    
    # Fetch holdings
    cur.execute("SELECT c.isin, eh.percent_of_nav FROM equity_holdings eh JOIN companies c ON eh.company_id = c.company_id WHERE eh.snapshot_id = %s", (snap1,))
    h1 = {row[0]: float(row[1]) for row in cur.fetchall() if row[0]}
    cur.execute("SELECT c.isin, eh.percent_of_nav FROM equity_holdings eh JOIN companies c ON eh.company_id = c.company_id WHERE eh.snapshot_id = %s", (snap2,))
    h2 = {row[0]: float(row[1]) for row in cur.fetchall() if row[0]}
    
    if not h1 or not h2:
        return 0, 0, 0
    
    isins1, isins2 = set(h1.keys()), set(h2.keys())
    common_isins = isins1.intersection(isins2)
    
    # Layer 2: ISIN Match % (based on total unique ISINs or min of both? Let's use user's 80-90% as common/total_unique or common/max_count)
    isin_match_pc = (len(common_isins) / max(len(isins1), len(isins2))) * 100 if isins1 or isins2 else 0
    
    # Layer 3: Weight Match (within ±10% relative difference)
    weight_matches = 0
    for isin in common_isins:
        w1, w2 = h1[isin], h2[isin]
        if w1 == 0 and w2 == 0:
            weight_matches += 1
            continue
        if w1 == 0 or w2 == 0:
            continue
        
        # Check if w2 is within [0.9*w1, 1.1*w1]
        if 0.9 * w1 <= w2 <= 1.1 * w1:
            weight_matches += 1
            
    weight_match_pc = (weight_matches / len(common_isins)) * 100 if common_isins else 0
    
    return isin_match_pc, weight_match_pc, len(common_isins)

def auto_map_continuity(dry_run=True, output_file=None):
    """
    Finds candidates using the 3-layer strategy:
    1. Fuzzy Text (50-60%)
    2. ISIN Overlap (80-90%)
    3. Holding Weight (within 10% tolerance)
    """
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        WITH snapshot_spans AS (
            SELECT ss.scheme_id, s.scheme_name, s.amc_id, a.amc_name, s.plan_type, s.option_type, s.is_reinvest,
                   MIN(p.year * 100 + p.month) as min_yrmo,
                   MAX(p.year * 100 + p.month) as max_yrmo
            FROM scheme_snapshots ss
            JOIN periods p ON ss.period_id = p.period_id
            JOIN schemes s ON ss.scheme_id = s.scheme_id
            JOIN amcs a ON s.amc_id = a.amc_id
            GROUP BY ss.scheme_id, s.scheme_name, s.amc_id, a.amc_name, s.plan_type, s.option_type, s.is_reinvest
        )
        SELECT 
            old.scheme_id as old_id, old.scheme_name as old_name, old.max_yrmo,
            new.scheme_id as new_id, new.scheme_name as new_name, new.min_yrmo,
            old.amc_id, old.amc_name
        FROM snapshot_spans old
        JOIN snapshot_spans new ON old.amc_id = new.amc_id 
                                AND old.plan_type = new.plan_type 
                                AND old.option_type = new.option_type 
                                AND old.is_reinvest = new.is_reinvest
        WHERE new.min_yrmo - old.max_yrmo IN (1, 89, 0, 88)
          AND old.scheme_id != new.scheme_id
    """)
    candidates = cur.fetchall()
    
    merges_to_do = []
    logger.info(f"Analyzing {len(candidates)} handoff candidates...")

    for old_id, old_name, old_ym, new_id, new_name, new_ym, amc_id, amc_name in candidates:
        # Layer 0: Product Type Consistency (MANDATORY)
        old_type = get_product_type(old_name)
        new_type = get_product_type(new_name)
        if old_type != new_type:
            continue

        # Layer 1: Fuzzy Text (50-60%)
        amc_clean = amc_name.replace("MUTUAL FUND", "").replace("AMC", "").strip().upper()
        old_clean = old_name.replace(amc_clean, "").strip()
        new_clean = new_name.replace(amc_clean, "").strip()
        
        # Normalize text for better comparison (e.g. INDX -> INDEX)
        old_clean = old_clean.replace("INDX", "INDEX")
        new_clean = new_clean.replace("INDX", "INDEX")
        
        text_score = max(fuzz.token_sort_ratio(old_clean, new_clean), fuzz.token_set_ratio(old_clean, new_clean))
        
        if text_score < 50:
            continue
            
        # Layer 2 & 3: ISIN and Weight
        isin_pc, weight_pc, common_count = calculate_detailed_overlap(cur, old_id, new_id)
        
        # User criteria: ISIN (80-90%) and Weight Match (Strict 40% of common ISINs must match)
        if isin_pc >= 80 and weight_pc >= 40: # Using 40% for weight match to include HDFC while being strict on Type/ISIN
            merges_to_do.append({
                "amc": amc_name,
                "text_score": text_score,
                "isin_score": isin_pc,
                "weight_score": weight_pc,
                "common_isins": common_count,
                "old_id": old_id, "old_name": old_name, "old_ym": old_ym,
                "new_id": new_id, "new_name": new_name, "new_ym": new_ym
            })

    merges_to_do.sort(key=lambda x: x["isin_score"], reverse=True)

    if output_file and dry_run:
        with open(output_file, 'w') as f:
            f.write("# Proposed Scheme Merges (4-Layer Strict Strategy)\n\n")
            f.write("Criteria:\n0. Product Type Consistency (ETF vs Index)\n1. Text Match > 50%\n2. ISIN Overlap > 80%\n3. Weight Match (±10% tolerance, min 40% consistency)\n\n")
            for m in merges_to_do:
                f.write(f"### {m['amc']}\n")
                f.write(f"- **Layer 1 (Text)**: {m['text_score']:.1f}%\n")
                f.write(f"- **Layer 2 (ISIN)**: {m['isin_score']:.1f}% ({m['common_isins']} matches)\n")
                f.write(f"- **Layer 3 (Weight Match)**: {m['weight_score']:.1f}% of common ISINs\n")
                f.write(f"- **Old Scheme** ({m['old_id']}): `{m['old_name']}`\n")
                f.write(f"- **New Scheme** ({m['new_id']}): `{m['new_name']}`\n\n")
        print(f"Report written to {output_file}")
        return

    for m in merges_to_do:
        print(f"[{m['amc']}] Text: {m['text_score']:.1f}%, ISIN: {m['isin_score']:.1f}%, Weight: {m['weight_score']:.1f}%")
        print(f"  Old: {m['old_name']} -> New: {m['new_name']}")
            
    if dry_run:
        print(f"\nDry Run complete. Found {len(merges_to_do)} safe continuity merges.")
        print("Run with --execute to perform these merges.")
    else:
        print(f"\nExecuting {len(merges_to_do)} auto-merges...")
        for m in merges_to_do:
            manual_merge(m['old_id'], m['new_id'])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=int, help="Source scheme ID to merge FROM (and delete)")
    parser.add_argument("--target", type=int, help="Target scheme ID to merge INTO (keep)")
    parser.add_argument("--auto", action="store_true", help="Run data continuity auto-mapper")
    parser.add_argument("--execute", action="store_true", help="Actually execute the auto-merges (use with --auto)")
    parser.add_argument("--output", type=str, help="Output file to dump proposed merges (used with --auto without --execute)")
    
    args = parser.parse_args()
    
    if args.auto:
        auto_map_continuity(dry_run=not args.execute, output_file=args.output)
    elif args.source and args.target:
        manual_merge(args.source, args.target)
    else:
        parser.print_help()
