from typing import List, Dict, Any
import time
from src.config import logger
from src.db import (
    upsert_scheme, upsert_company_master, create_snapshot, 
    insert_holdings, check_snapshot_exists, get_isin_details,
    get_canonical_sector, upsert_isin_master, upsert_company
)

class PortfolioLoader:
    """
    Handles loading of extracted portfolio data into PostgreSQL.
    Optimized for performance:
    - Memory cache for ISIN -> company_id
    - One transaction per scheme
    """
    
    _company_cache = {} # Static cache across instances

    @staticmethod
    def load_holdings(amc_id: int, period_id: int, holdings: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Loads a list of holdings into the database.
        Returns metadata about the load (rows inserted, etc).
        """
        if not holdings:
            return {"rows_inserted": 0, "schemes_count": 0}

        # Group holdings by scheme
        schemes_data = {}
        for h in holdings:
            s_key = (h['scheme_name'], h['plan_type'], h['option_type'])
            if s_key not in schemes_data:
                schemes_data[s_key] = {
                    "info": h, # Sample for metadata
                    "items": []
                }
            schemes_data[s_key]["items"].append(h)

        total_inserted = 0
        
        for s_key, data in schemes_data.items():
            try:
                # 1. Upsert Scheme (Granular)
                scheme_id = upsert_scheme(
                    amc_id=amc_id,
                    scheme_name=data["info"]["scheme_name"],
                    plan_type=data["info"]["plan_type"],
                    option_type=data["info"]["option_type"],
                    is_reinvest=data["info"].get("is_reinvest", False)
                )

                # 2. Idempotency Check
                if check_snapshot_exists(scheme_id, period_id):
                    logger.info(f"Snapshot already exists for {s_key[0]} ({s_key[1]}/{s_key[2]}). Skipping.")
                    continue

                # 3. Resolve Company IDs with Caching & Dedup/Merge Logic
                merged_holdings_map = {} # isin -> holding_dict
                
                for h in data["items"]:
                    isin = h['isin']
                    
                    # Dedup/Merge Logic
                    if isin in merged_holdings_map:
                        existing = merged_holdings_map[isin]
                        # Check if exact duplicate
                        if existing['quantity'] == h['quantity'] and existing['market_value_inr'] == h['market_value_inr']:
                            logger.warning(f"Duplicate holding dropped for {isin} in {s_key[0]}")
                            continue
                        else:
                            # Merge (Sum)
                            logger.info(f"Merging split holding for {isin} in {s_key[0]}")
                            existing['quantity'] += h['quantity']
                            existing['market_value_inr'] += h['market_value_inr']
                            existing['percent_to_nav'] = float(existing['percent_to_nav']) + float(h['percent_to_nav'])
                            continue

                    merged_holdings_map[isin] = h.copy() # Copy to avoid mutating original

                    if isin not in PortfolioLoader._company_cache:
                        isin_meta = get_isin_details(isin)
                        canonical_name = isin_meta['canonical_name'] if isin_meta else h['company_name']
                        
                        # Added: Ensure ISIN exists in isin_master to satisfy FK constraint
                        upsert_isin_master(
                            isin=isin,
                            canonical_name=canonical_name,
                            sector=isin_meta['sector'] if isin_meta else h.get('sector'),
                            industry=isin_meta.get('industry') if isin_meta else h.get('industry')
                        )

                        # 2. Canonical Sector Resolution
                        raw_sector = isin_meta['sector'] if isin_meta else h.get('sector', 'Unknown')
                        canonical_sector = get_canonical_sector(raw_sector)

                        # Use upsert_company to satisfy the FK in equity_holdings (which points to 'companies' table)
                        company_id = upsert_company(
                            isin=isin,
                            company_name=canonical_name,
                            sector=canonical_sector,
                            industry=isin_meta.get('industry') if isin_meta else h.get('industry')
                        )
                        PortfolioLoader._company_cache[isin] = company_id
                
                # Build final db_holdings from merged map
                db_holdings = []
                for isin, h in merged_holdings_map.items():
                    db_holdings.append({
                        "company_id": PortfolioLoader._company_cache[isin],
                        "quantity": h['quantity'],
                        "market_value_inr": h['market_value_inr'],
                        "percent_of_nav": h['percent_to_nav']
                    })

                # 4. Atomic Load per Scheme
                total_value = sum(h['market_value_inr'] for h in db_holdings)
                total_nav_percent = sum(float(h['percent_of_nav']) for h in db_holdings)

                # %NAV Guard: Check if sum is within 95-105% (Standard Equity Range)
                if not (95.0 <= total_nav_percent <= 105.0):
                    logger.warning(
                        f"[%NAV GUARD] Scheme '{s_key[0]}' has unusual total equity exposure: {total_nav_percent:.2f}% "
                        f"(Expected 95-105%). Verify if significant Cash/Debt was excluded."
                    )

                snapshot_id = create_snapshot(
                    scheme_id=scheme_id,
                    period_id=period_id,
                    total_holdings=len(db_holdings),
                    total_value_inr=total_value,
                    holdings_count=len(db_holdings)
                )

                insert_holdings(snapshot_id, db_holdings)
                total_inserted += len(db_holdings)

            except Exception as e:
                logger.error(f"Failed to load scheme {s_key}: {e}")
                continue

        return {
            "rows_inserted": total_inserted,
            "schemes_count": len(schemes_data)
        }
