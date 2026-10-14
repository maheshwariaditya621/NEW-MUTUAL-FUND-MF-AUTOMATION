import re
from typing import Optional, Tuple
from src.db.connection import get_connection
from src.config import logger

# Categories based on user UI and standard MF classifications
CAT_EQUITY = "Equity Funds"
CAT_DEBT = "Debt Funds"
CAT_HYBRID = "Hybrid Funds"
CAT_ARBITRAGE = "Arbitrage Funds"
CAT_TAX_SAVING = "Tax-Saving Funds (ELSS)"
CAT_INDEX = "Index Funds"
CAT_THEMATIC = "Thematic Funds"

def get_website_category(scheme_name: str, amfi_broad: Optional[str] = None, amfi_scheme: Optional[str] = None) -> Tuple[str, str]:
    """
    Determines the website category and sub-category for a scheme.
    Logic:
    1. Check for Index/ETF/Thematic keywords first (cross-broad-category)
    2. Fallback to AMFI category mapping
    3. Final fallback to Name-based heuristics
    """
    name_upper = scheme_name.upper().replace("\n", " ").strip()
    
    # 1. Specialized Checks (Index / ETF)
    if any(x in name_upper for x in ["INDEX", "NIFTY", "SENSEX", "INDX", "NIFTY50", "NEXT 50", "ALPHA", "EQUAL WEIGHT", " IDX"]):
        if " ETF" in name_upper or "ETF " in name_upper or name_upper.endswith("ETF"):
            return CAT_INDEX, "ETF"
        return CAT_INDEX, "Index Fund"
    if " ETF" in name_upper or "ETF " in name_upper or name_upper.endswith("ETF") or "EXCHANGE TRADED" in name_upper:
        return CAT_INDEX, "ETF"
    
    # ELSS Check
    if "ELSS" in name_upper or "TAX SAVER" in name_upper or "TAX SAVG" in name_upper or "TAX-SAVING" in name_upper:
        return CAT_TAX_SAVING, "ELSS"

    # Arbitrage Check — must come BEFORE Hybrid to avoid mis-classification
    if "ARBITRAGE" in name_upper:
        return CAT_ARBITRAGE, "Arbitrage Fund"

    # Thematic / Sectoral Check
    if amfi_scheme and ("Sectoral" in amfi_scheme or "Thematic" in amfi_scheme):
        return CAT_THEMATIC, "Sectoral/Thematic"
    
    # 2. AMFI Broad Category Fallback
    if amfi_broad:
        broad = amfi_broad.upper()
        if "EQUITY" in broad:
            sub = "Other Equity"
            if amfi_scheme:
                if "Large & Mid" in amfi_scheme or "Large and Mid" in amfi_scheme: sub = "Large & Mid Cap"
                elif "Large Cap" in amfi_scheme: sub = "Large Cap"
                elif "Mid Cap" in amfi_scheme: sub = "Mid Cap"
                elif "Small Cap" in amfi_scheme: sub = "Small Cap"
                elif "Flexi Cap" in amfi_scheme: sub = "Flexi Cap"
                elif "Multi Cap" in amfi_scheme: sub = "Multi Cap"
                elif "Dividend Yield" in amfi_scheme: sub = "Dividend Yield"
                elif "Focused" in amfi_scheme: sub = "Focused Fund"
                elif "Value" in amfi_scheme or "Contra" in amfi_scheme: sub = "Value/Contra"
                elif "ELSS" in amfi_scheme: return CAT_TAX_SAVING, "ELSS"
            return CAT_EQUITY, sub
        
        if "DEBT" in broad:
            sub = amfi_scheme if amfi_scheme else "Debt"
            return CAT_DEBT, sub
        
        if "HYBRID" in broad:
            if amfi_scheme and "Arbitrage" in amfi_scheme:
                return CAT_ARBITRAGE, "Arbitrage Fund"
            sub = amfi_scheme if amfi_scheme else "Hybrid"
            return CAT_HYBRID, sub

    # 3. Name-based Heuristics (Final Fallback)
    
    # Debt Heuristics
    if any(x in name_upper for x in [
        "BOND", "DEBT", "LIQUID", "TREASURY", "GILT", "FIXED TERM", "FTP", 
        "MATURITY", "FMP", "FIXED MATURITY", "CAPITAL PROTECTION", "INTERVAL",
        "MONTHLY INCOME", "MIP", "OVERNIGHT", "ULTRASHORT", "ULTRA SHORT",
        "LOW DURATION", "SHORT TERM", "MONEY MARKET", "CORPORATE BOND", "FLOATER",
        "CREDIT RISK", "BANKING & PSU", "10Y GILT", "DURATION", "MEDIUM TERM"
    ]):
        if "LIQUID" in name_upper: return CAT_DEBT, "Liquid Fund"
        if "OVERNIGHT" in name_upper: return CAT_DEBT, "Overnight Fund"
        if "MONEY MARKET" in name_upper: return CAT_DEBT, "Money Market Fund"
        if "CORPORATE BOND" in name_upper: return CAT_DEBT, "Corporate Bond Fund"
        if "CREDIT RISK" in name_upper: return CAT_DEBT, "Credit Risk Fund"
        if "GILT" in name_upper: return CAT_DEBT, "Gilt Fund"
        if "FLOATER" in name_upper: return CAT_DEBT, "Floater Fund"
        return CAT_DEBT, "Debt"

    # Hybrid & Solution Oriented (ARBITRAGE handled above)
    if any(x in name_upper for x in [
        "HYBRID", "BALANCED", "AGGRESSIVE", "CONSERVATIVE",
        "DYNAMIC ASSET", "EQUITY SAVINGS", "RETIREMENT", "CHILDREN",
        "PENSION", "MULTI ASSET", "ASSET ALLOCATOR", "BAL BHAVISHYA", "BALANCED ADVANTAGE", "CHILD"
    ]):
        if "BALANCED ADVANTAGE" in name_upper or "DYNAMIC ASSET" in name_upper:
            return CAT_HYBRID, "Dynamic Asset Allocation or Balanced Advantage"
        if "MULTI ASSET" in name_upper:
            return CAT_HYBRID, "Multi Asset Allocation"
        if "EQUITY SAVINGS" in name_upper:
            return CAT_HYBRID, "Equity Savings"
        if "AGGRESSIVE" in name_upper:
            return CAT_HYBRID, "Aggressive Hybrid Fund"
        if "CONSERVATIVE" in name_upper:
            return CAT_HYBRID, "Conservative Hybrid Fund"
        return CAT_HYBRID, "Hybrid"

    # Thematic / Sectoral
    if any(x in name_upper for x in [
        "INFRASTRUCTURE", "INFRA", "BANKING", "FINANCIAL", "HEALTHCARE", 
        "PHARMA", "TECHNOLOGY", "IT FUND", "DIGITAL", "CONSUMPTION", 
        "FMCG", "ENERGY", "POWER", "COMMODITIES", "MANUFACTURE", "MANUFACTURING",
        "TRANSPORT", "LOGISTICS", "SERVICES", "INDIA GENEXT", "SPECIAL SITUATION",
        "BUSINESS CYCLE", "MNC", "ESG", "QUANT", "MOMENTUM", "PASSIVE", "SMART BETA",
        "DEFENCE", "AUTO", "REALTY", "HOUSING", "INNOVATION", "GLOBAL", "INTERNATIONAL",
        "US SPECIFIC", "US SPEC", "NASDAQ", "TAIWAN", "GOLD", "SILVER", "COMMODITY"
    ]):
        if any(x in name_upper for x in ["GLOBAL", "INTERNATIONAL", "US SPECIFIC", "US SPEC", "NASDAQ", "TAIWAN"]):
            return CAT_THEMATIC, "International/FOF"
        return CAT_THEMATIC, "Sectoral/Thematic"

    # Equity Heuristics
    if any(x in name_upper for x in ["LARGE & MID", "LARGE AND MID"]):
        return CAT_EQUITY, "Large & Mid Cap"
    if any(x in name_upper for x in ["LARGE CAP", "BLUECHIP", "TOP 100", "TOP 200", "LARGECAP"]):
        return CAT_EQUITY, "Large Cap"
    if any(x in name_upper for x in ["MID CAP", "MIDCAP"]):
        return CAT_EQUITY, "Mid Cap"
    if any(x in name_upper for x in ["SMALL CAP", "SMALLCAP"]):
        return CAT_EQUITY, "Small Cap"
    if any(x in name_upper for x in ["FLEXI CAP", "FLEXICAP"]):
        return CAT_EQUITY, "Flexi Cap"
    if any(x in name_upper for x in ["MULTI CAP", "MULTICAP"]):
        return CAT_EQUITY, "Multi Cap"
    if any(x in name_upper for x in ["VALUE", "CONTRA", "STRATEGY"]):
        return CAT_EQUITY, "Value/Contra"
    if "DIVIDEND" in name_upper:
        return CAT_EQUITY, "Dividend Yield"
    if "FOCUSED" in name_upper:
        return CAT_EQUITY, "Focused Fund"
    
    # Generic Equity fallback
    if any(x in name_upper for x in ["EQUITY", "OPPORTUNITIES", "GROWTH", "FUND", "SCHEME", "SELECT", "TARGET", "DIVERSIFIED", "FOF", "DISCOVERY"]):
        return CAT_EQUITY, "Other Equity"

    return "Other", "Uncategorized"

def update_categories(dry_run: bool = False):
    conn = get_connection()
    cur = conn.cursor()
    
    # Fetch all schemes and their AMFI categories (if mapped)
    query = """
        SELECT s.scheme_id, s.scheme_name, c.broad_category, c.scheme_category
        FROM schemes s
        LEFT JOIN scheme_category_master c ON s.amfi_code = c.amfi_code
    """
    cur.execute(query)
    schemes = cur.fetchall()
    
    logger.info(f"Processing {len(schemes)} schemes for categorization...")
    
    updates = []
    for s_id, s_name, amfi_broad, amfi_scheme in schemes:
        cat, sub = get_website_category(s_name, amfi_broad, amfi_scheme)
        updates.append((cat, sub, s_id))
        
        if dry_run and len(updates) <= 20:
             logger.info(f"[DRY RUN] {s_name} -> {cat} | {sub}")

    if not dry_run:
        logger.info(f"Applying updates to {len(updates)} schemes...")
        update_query = """
            UPDATE schemes 
            SET website_category = %s, website_sub_category = %s, updated_at = NOW()
            WHERE scheme_id = %s
        """
        from psycopg2.extras import execute_batch
        execute_batch(cur, update_query, updates)
        conn.commit()
        logger.info("Website categories updated successfully.")
    else:
        logger.info(f"[DRY RUN] Would update {len(updates)} schemes.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    import sys
    dry = "--dry-run" in sys.argv
    update_categories(dry_run=dry)
