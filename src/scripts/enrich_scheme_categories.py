"""
enrich_scheme_categories.py
===========================
One-time enrichment script to ensure all schemes in the DB have
website_category and website_sub_category populated.

Strategy (in order of priority):
1. Use existing scheme_category_master (keyed by amfi_code) if the scheme has an amfi_code match.
2. Use AMFI live NAV feed to fetch current scheme type text and infer category.
3. Fallback: infer from scheme_name via keyword rules.

Run:
    python -m src.scripts.enrich_scheme_categories [--dry-run]
"""

import sys
import re
import argparse
import psycopg2
import requests
from dotenv import load_dotenv
import os

load_dotenv()


# ── Category mapping from AMFI scheme type text → (website_category, website_sub_category) ──
AMFI_CATEGORY_MAP = {
    # Equity
    "large cap fund": ("Equity Funds", "Large Cap"),
    "large & mid cap fund": ("Equity Funds", "Large & Mid Cap"),
    "large and mid cap fund": ("Equity Funds", "Large & Mid Cap"),
    "mid cap fund": ("Equity Funds", "Mid Cap"),
    "small cap fund": ("Equity Funds", "Small Cap"),
    "multi cap fund": ("Equity Funds", "Multi Cap"),
    "flexi cap fund": ("Equity Funds", "Flexi Cap"),
    "focused fund": ("Equity Funds", "Focused Fund"),
    "dividend yield fund": ("Equity Funds", "Dividend Yield"),
    "value fund": ("Equity Funds", "Value/Contra"),
    "contra fund": ("Equity Funds", "Value/Contra"),
    "elss": ("Tax-Saving Funds (ELSS)", "ELSS"),
    "equity linked savings scheme": ("Tax-Saving Funds (ELSS)", "ELSS"),
    # Hybrid
    "arbitrage fund": ("Hybrid Funds", "Arbitrage Fund"),
    "aggressive hybrid fund": ("Hybrid Funds", "Aggressive Hybrid Fund"),
    "conservative hybrid fund": ("Hybrid Funds", "Conservative Hybrid Fund"),
    "balanced hybrid fund": ("Hybrid Funds", "Balanced Hybrid Fund"),
    "multi asset allocation": ("Hybrid Funds", "Multi Asset Allocation"),
    "dynamic asset allocation": ("Hybrid Funds", "Dynamic Asset Allocation or Balanced Advantage"),
    "balanced advantage": ("Hybrid Funds", "Dynamic Asset Allocation or Balanced Advantage"),
    "equity savings": ("Hybrid Funds", "Equity Savings"),
    # Debt
    "liquid fund": ("Debt Funds", "Liquid Fund"),
    "overnight fund": ("Debt Funds", "Overnight Fund"),
    "ultra short duration fund": ("Debt Funds", "Debt"),
    "low duration fund": ("Debt Funds", "Debt"),
    "short duration fund": ("Debt Funds", "Debt"),
    "medium duration fund": ("Debt Funds", "Debt"),
    "medium to long duration fund": ("Debt Funds", "Medium to Long Duration Fund"),
    "long duration fund": ("Debt Funds", "Debt"),
    "dynamic bond": ("Debt Funds", "Debt"),
    "corporate bond fund": ("Debt Funds", "Debt"),
    "credit risk fund": ("Debt Funds", "Credit Risk Fund"),
    "banking and psu fund": ("Debt Funds", "Debt"),
    "gilt fund": ("Debt Funds", "Debt"),
    "floater fund": ("Debt Funds", "Debt"),
    "money market fund": ("Debt Funds", "Debt"),
    # Thematic / Sectoral
    "sectoral fund": ("Thematic Funds", "Sectoral/Thematic"),
    "thematic fund": ("Thematic Funds", "Sectoral/Thematic"),
    "international fund": ("Thematic Funds", "International/FOF"),
    "fund of funds": ("Thematic Funds", "Fund of Funds"),
    "fof": ("Thematic Funds", "Fund of Funds"),
    # Index
    "index fund": ("Index Funds", "Index Fund"),
    "etf": ("Index Funds", "ETF"),
    "exchange traded fund": ("Index Funds", "ETF"),
}

# ── Keyword rules for name-based inference ──
NAME_RULES = [
    (r"\barbitrage\b", ("Hybrid Funds", "Arbitrage Fund")),
    (r"\belss\b|equity linked savings", ("Tax-Saving Funds (ELSS)", "ELSS")),
    (r"\bsmall\s*cap\b", ("Equity Funds", "Small Cap")),
    (r"\bmid\s*cap\b", ("Equity Funds", "Mid Cap")),
    (r"\blarge\s*cap\b", ("Equity Funds", "Large Cap")),
    (r"\blarge\s*&\s*mid\s*cap\b|large\s*and\s*mid\s*cap\b", ("Equity Funds", "Large & Mid Cap")),
    (r"\bflexi\s*cap\b", ("Equity Funds", "Flexi Cap")),
    (r"\bmulti\s*cap\b", ("Equity Funds", "Multi Cap")),
    (r"\bfocused\b", ("Equity Funds", "Focused Fund")),
    (r"\bdividend\s*yield\b", ("Equity Funds", "Dividend Yield")),
    (r"\bvalue\s*fund\b|\bcontra\b", ("Equity Funds", "Value/Contra")),
    (r"\baggressive\s*hybrid\b", ("Hybrid Funds", "Aggressive Hybrid Fund")),
    (r"\bconservative\s*hybrid\b", ("Hybrid Funds", "Conservative Hybrid Fund")),
    (r"\bbalanced\s*advantage\b|\bdynamic\s*asset\b", ("Hybrid Funds", "Dynamic Asset Allocation or Balanced Advantage")),
    (r"\bequity\s*savings\b", ("Hybrid Funds", "Equity Savings")),
    (r"\bmulti\s*asset\b", ("Hybrid Funds", "Multi Asset Allocation")),
    (r"\bhybrid\b|\bbalanced\b", ("Hybrid Funds", "Hybrid")),
    (r"\bliquid\b", ("Debt Funds", "Liquid Fund")),
    (r"\bovernight\b", ("Debt Funds", "Overnight Fund")),
    (r"\bgilt\b", ("Debt Funds", "Debt")),
    (r"\bdebt\b|\bbond\b|\bduration\b|\bcredit\s*risk\b|\bcorporate\s*bond\b|\bbanking.*psu\b|\bfloater\b|\bmoney\s*market\b", ("Debt Funds", "Debt")),
    (r"\bsectoral\b|\bthematic\b|\binfrastructure\b|\btechnology\b|\bhealthcare\b|\bpharma\b|\bfinancial\s*serv\b|\bfmcg\b|\bmanufacturing\b|\bpsu\b|\bdigital\b|\benergy\b|\bconsumption\b|\bexport\b|\bdefence\b|\breal\s*estate\b|\bmnc\b", ("Thematic Funds", "Sectoral/Thematic")),
    (r"\binternational\b|\bglobal\b|\bnasdaq\b|\bhangseng\b|\bnyse\b|\bus\s*equit\b|\bchina\b|\bemerging\s*market\b|\bworld\b", ("Thematic Funds", "International/FOF")),
    (r"\bfund\s*of\s*fund\b|\bfof\b", ("Thematic Funds", "Fund of Funds")),
    (r"\betf\b|\bexchange\s*traded\b|\bnifty\b|\bsensex\b|\bbse\b|\bnse\b|\bindex\b", ("Index Funds", "Index Fund")),
]


def get_db_conn():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )


def infer_from_name(scheme_name: str):
    """Infer (website_category, website_sub_category) from scheme name keywords."""
    name_lower = scheme_name.lower()
    for pattern, cat_tuple in NAME_RULES:
        if re.search(pattern, name_lower):
            return cat_tuple
    return ("Other", "Uncategorized")


def fetch_amfi_scheme_types():
    """
    Fetch AMFI NAV all.txt and parse scheme type text.
    Returns dict: amfi_code -> (category_text, sub_category_text)
    """
    print("[INFO] Fetching AMFI NAVAll.txt ...")
    try:
        resp = requests.get(
            "https://www.amfiindia.com/spages/NAVAll.txt",
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"[WARN] Could not fetch AMFI data: {e}")
        return {}

    result = {}
    current_category = None
    current_sub = None

    for line in resp.text.splitlines():
        line = line.strip()
        if not line:
            continue

        # Category line — e.g., "Open Ended Schemes(Equity Scheme - Large Cap Fund)"
        if line.startswith("Open Ended") or line.startswith("Close Ended") or line.startswith("Interval"):
            # Extract the sub-category from parentheses
            m = re.search(r'\((.+?)\)', line)
            if m:
                inner = m.group(1).lower()
                # Map inner text to our categories
                matched = None
                for key, val in AMFI_CATEGORY_MAP.items():
                    if key in inner:
                        matched = val
                        break
                current_category = matched[0] if matched else "Other"
                current_sub = matched[1] if matched else "Uncategorized"
            continue

        # Data line: SchemeCode;ISINGr;ISINDiv;SchemeName;NetAssetValue;Date
        parts = line.split(";")
        if len(parts) >= 4 and parts[0].strip().isdigit():
            amfi_code = parts[0].strip()
            if current_category and amfi_code:
                result[amfi_code] = (current_category, current_sub)

    print(f"[OK] AMFI feed parsed: {len(result)} scheme code -> category mappings")
    return result


def run(dry_run=False):
    conn = get_db_conn()
    cur = conn.cursor()

    # 1. Fetch schemes that are NULL or have "Other/Uncategorized"
    cur.execute("""
        SELECT scheme_id, scheme_name, amfi_code, website_category, website_sub_category
        FROM schemes
        WHERE website_category IS NULL
           OR (website_category = 'Other' AND website_sub_category = 'Uncategorized')
        ORDER BY scheme_id
    """)
    null_schemes = cur.fetchall()
    print(f"\n[INFO] Found {len(null_schemes)} schemes needing category enrichment")

    if not null_schemes:
        print("[OK] All schemes already categorized!")
        cur.close()
        conn.close()
        return

    # 2. Fetch AMFI live data
    amfi_map = fetch_amfi_scheme_types()

    # 3. Also check scheme_category_master for matches
    cur.execute("SELECT amfi_code, broad_category, scheme_category FROM scheme_category_master")
    master_rows = cur.fetchall()
    master_map = {r[0]: (r[1], r[2]) for r in master_rows}

    # Helper: master broad_category → website_category
    BROAD_CAT_MAP = {
        "Equity": "Equity Funds",
        "Hybrid": "Hybrid Funds",
        "Debt": "Debt Funds",
        "Solution Oriented": "Other",
        "Other": "Other",
    }

    updated = 0
    failed = []

    for sid, sname, amfi_code, curr_wcat, curr_wsub in null_schemes:
        website_cat = None
        website_sub = None

        # Priority 1: AMFI live feed
        if amfi_code and amfi_code in amfi_map:
            website_cat, website_sub = amfi_map[amfi_code]

        # Priority 2: scheme_category_master
        if not website_cat and amfi_code and amfi_code in master_map:
            broad, sub_cat = master_map[amfi_code]
            website_cat = BROAD_CAT_MAP.get(broad, "Other")
            # infer sub from sub_cat text
            sub_lower = (sub_cat or "").lower()
            matched_sub = None
            for key, val in AMFI_CATEGORY_MAP.items():
                if key in sub_lower:
                    matched_sub = val[1]
                    break
            website_sub = matched_sub or sub_cat or "Uncategorized"

        # Priority 3: name-based inference
        if not website_cat:
            website_cat, website_sub = infer_from_name(sname)

        action = "SKIP (already set)" if (curr_wcat and curr_wcat != 'Other') else f"SET -> {website_cat} / {website_sub}"
        print(f"  [{sid:6d}] {sname[:55]:<55} {action}")

        if not dry_run:
            cur.execute("""
                UPDATE schemes
                SET website_category = %s,
                    website_sub_category = %s,
                    updated_at = NOW()
                WHERE scheme_id = %s
            """, (website_cat, website_sub, sid))
            updated += 1

    if not dry_run:
        conn.commit()
        print(f"\n[OK] Updated {updated} schemes")
    else:
        print(f"\n[DRY RUN] Would have updated {len(null_schemes)} schemes")

    # Also backfill schemes that have AMFI live data even if already categorized (optional refresh)
    if amfi_map and not dry_run:
        print("\n[INFO] Cross-checking AMFI feed for any category updates on existing schemes...")
        cur.execute("SELECT scheme_id, amfi_code, website_category, website_sub_category FROM schemes WHERE amfi_code IS NOT NULL")
        all_with_code = cur.fetchall()
        refresh_count = 0
        for sid, amfi_code, curr_wcat, curr_wsub in all_with_code:
            if amfi_code in amfi_map:
                new_cat, new_sub = amfi_map[amfi_code]
                if new_cat != curr_wcat or new_sub != curr_wsub:
                    cur.execute("""
                        UPDATE schemes SET website_category=%s, website_sub_category=%s, updated_at=NOW()
                        WHERE scheme_id=%s
                    """, (new_cat, new_sub, sid))
                    refresh_count += 1
        conn.commit()
        print(f"[OK] Refreshed {refresh_count} category mappings from AMFI feed")

    cur.close()
    conn.close()
    print("\n[DONE] Enrichment complete!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enrich scheme categories from AMFI and name inference")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing to DB")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
