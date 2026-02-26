from typing import List, Dict, Any
import time
from src.config import logger
from src.db import (
    upsert_scheme, upsert_company_master, create_snapshot, 
    insert_holdings, check_snapshot_exists, get_isin_details,
    get_canonical_sector, upsert_isin_master, upsert_company,
    find_entity_by_name, find_entity_by_symbol, fuzzy_search_entity,
    create_corporate_entity, log_resolution_audit,
    find_potential_scheme_renames, record_pending_merge, record_notification, get_cursor
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
    def _resolve_security_entity(isin: str, company_name: str, period_id: int) -> int:
        """
        Production-grade security resolution engine.
        Order: ISIN -> Exact Name -> Symbol -> Fuzzy -> Create.
        """
        # 1. Direct ISIN Match (Fast Path)
        isin_meta = get_isin_details(isin)
        if isin_meta and isin_meta.get('entity_id'):
            log_resolution_audit(isin, company_name, isin_meta['entity_id'], "TIER 1 (ISIN)")
            return isin_meta['entity_id']

        # 2. Exact Name Match
        entity_id = find_entity_by_name(company_name)
        if entity_id:
            log_resolution_audit(isin, company_name, entity_id, "TIER 2 (EXACT NAME)")
            return entity_id

        # 3. Symbol Match (Usually not in AMC files, but can be derived if available)
        # symbol = derive_symbol(isin, company_name)
        # entity_id = find_entity_by_symbol(symbol)
        
        # 4. Fuzzy Match Fallback (Audit Logged)
        entity_id = fuzzy_search_entity(company_name, threshold=0.95)
        if entity_id:
            log_resolution_audit(isin, company_name, entity_id, "TIER 4 (FUZZY)")
            return entity_id

        # 5. Create new logical entity
        entity_id = create_corporate_entity(company_name)
        log_resolution_audit(isin, company_name, entity_id, "TIER 5 (NEW ENTITY)")
        return entity_id

    @staticmethod
    def _calculate_overlap_with_holdings(old_scheme_id: int, incoming_holdings: List[Dict[str, Any]]) -> Dict[str, float]:
        """
        Calculates ISIN and weight overlap between an existing scheme's latest snapshot
        and the incoming raw holdings data.
        """
        cursor = get_cursor()
        # 1. Get latest snapshot for old scheme
        cursor.execute("SELECT snapshot_id FROM scheme_snapshots WHERE scheme_id = %s ORDER BY period_id DESC LIMIT 1", (old_scheme_id,))
        res = cursor.fetchone()
        if not res: return {"isin_pc": 0, "weight_pc": 0}
        
        old_snap_id = res[0]
        
        # 2. Fetch old holdings
        cursor.execute("""
            SELECT c.isin, eh.percent_of_nav 
            FROM equity_holdings eh
            JOIN companies c ON eh.company_id = c.company_id
            WHERE eh.snapshot_id = %s
        """, (old_snap_id,))
        old_h = {row[0]: float(row[1]) for row in cursor.fetchall() if row[0]}
        
        # 3. Process incoming holdings
        new_h = {h['isin']: float(h['percent_of_nav']) for h in incoming_holdings if h.get('isin')}
        
        if not old_h or not new_h:
            return {"isin_pc": 0, "weight_pc": 0}
            
        isins_old = set(old_h.keys())
        isins_new = set(new_h.keys())
        common = isins_old.intersection(isins_new)
        
        isin_match_pc = (len(common) / max(len(isins_old), len(isins_new))) * 100
        
        # Weight match (within 10% relative tolerance)
        weight_matches = 0
        for isin in common:
            w1, w2 = old_h[isin], new_h[isin]
            if w1 > 0 and 0.9 * w1 <= w2 <= 1.1 * w1:
                weight_matches += 1
        
        weight_match_pc = (weight_matches / len(common)) * 100 if common else 0
        
        return {"isin_pc": isin_match_pc, "weight_pc": weight_match_pc, "common_count": len(common)}

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
                # 1. Resolve/Upsert Scheme (Granular)
                # Before upsert, check if name is brand new for this AMC
                cursor = get_cursor()
                cursor.execute("SELECT scheme_id FROM schemes WHERE amc_id = %s AND scheme_name = %s", (amc_id, data["info"]["scheme_name"].upper().strip()))
                exists = cursor.fetchone()
                
                scheme_id = upsert_scheme(
                    amc_id=amc_id,
                    scheme_name=data["info"]["scheme_name"],
                    plan_type=data["info"]["plan_type"],
                    option_type=data["info"]["option_type"],
                    is_reinvest=data["info"].get("is_reinvest", False)
                )

                if not exists:
                    # Brand new scheme name detected! Run 4-Layer Resolution Quarantine
                    match = find_potential_scheme_renames(
                        amc_id=amc_id,
                        new_name=data["info"]["scheme_name"],
                        plan=data["info"]["plan_type"],
                        opt=data["info"]["option_type"],
                        re=data["info"].get("is_reinvest", False),
                        period_id=period_id
                    )
                    
                    if match:
                        # Perform deep portfolio check
                        overlap = PortfolioLoader._calculate_overlap_with_holdings(match['old_id'], data["items"])
                        
                        # If ISIN match > 80%, quarantine and flag
                        if overlap['isin_pc'] >= 80:
                            record_pending_merge(
                                amc_id=amc_id,
                                new_name=data["info"]["scheme_name"],
                                old_id=match['old_id'],
                                plan=data["info"]["plan_type"],
                                opt=data["info"]["option_type"],
                                re=data["info"].get("is_reinvest", False),
                                score=overlap['isin_pc']/100.0,
                                method='4_LAYER_DETECTION',
                                metadata={
                                    "text_score": match['text_score'],
                                    "isin_score": overlap['isin_pc'],
                                    "weight_score": overlap['weight_pc'],
                                    "common_isins": overlap['common_count'],
                                    "old_name": match['old_name']
                                }
                            )
                            record_notification(
                                level='WARNING',
                                category='MERGE',
                                content=f"Potential rename detected: '{data['info']['scheme_name']}' matches '{match['old_name']}' ({overlap['isin_pc']:.1f}%). Quarantined for Admin approval."
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
                            existing['percent_of_nav'] = float(existing['percent_of_nav']) + float(h['percent_of_nav'])
                            continue

                    merged_holdings_map[isin] = h.copy() # Copy to avoid mutating original

                    if isin not in PortfolioLoader._company_cache:
                        # NEW Security Resolution Engine
                        entity_id = PortfolioLoader._resolve_security_entity(isin, h['company_name'], period_id)
                        
                        isin_meta = get_isin_details(isin)
                        
                        if isin_meta and isin_meta['canonical_name'] and isin_meta['canonical_name'] != 'N/A':
                            canonical_name = isin_meta['canonical_name']
                        else:
                            canonical_name = h['company_name']
                        
                        # Upsert ISIN master with fresh data and mapping to entity
                        upsert_isin_master(
                            isin=isin,
                            canonical_name=canonical_name,
                            sector=isin_meta['sector'] if isin_meta else h.get('sector'),
                            industry=isin_meta.get('industry') if isin_meta else h.get('industry'),
                            entity_id=entity_id,
                            period_id=period_id
                        )

                        # 2. Canonical Sector Resolution
                        raw_sector = isin_meta['sector'] if isin_meta else h.get('sector', 'Unknown')
                        canonical_sector = get_canonical_sector(raw_sector)

                        # Use upsert_company with entity_id link
                        company_id = upsert_company(
                            isin=isin,
                            company_name=canonical_name,
                            sector=canonical_sector,
                            industry=isin_meta.get('industry') if isin_meta else h.get('industry'),
                            # We should ideally pass entity_id here too if upsert_company supports it
                             # nse_symbol/bse_code can be passed if we had them
                        )
                        # Ensure companies table has parity with entity_id (optional if upsert_company doesn't have it yet)
                        # For now, company_id is our internal record ID.
                        
                        PortfolioLoader._company_cache[isin] = company_id
                
                # Build final db_holdings from merged map
                db_holdings = []
                for isin, h in merged_holdings_map.items():
                    db_holdings.append({
                        "company_id": PortfolioLoader._company_cache[isin],
                        "quantity": h['quantity'],
                        "market_value_inr": h['market_value_inr'],
                        "percent_of_nav": h['percent_of_nav']
                    })

                # 4. Atomic Load per Scheme
                if not db_holdings:
                    logger.info(f"Scheme '{s_key[0]}' has no valid equity holdings. Skipping DB store.")
                    continue

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
                # Log and re-raise to trigger transaction rollback
                logger.error(f"Critical failure loading scheme {s_key}: {e}")
                raise

        return {
            "rows_inserted": total_inserted,
            "schemes_count": len(schemes_data)
        }
