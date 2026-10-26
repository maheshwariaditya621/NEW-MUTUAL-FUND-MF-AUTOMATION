"""
Bulk and Block Deals Orchestration Engine.

Handles:
1. Daily multi-exchange data collection (NSE & BSE).
2. Resumable, idempotent historical backfilling (up to 12 months in configurable batches).
3. Conflict-safe database upserts and execution auditing via market_deal_ingestion_runs.
4. Operational sync state checkpoints.
5. Command-line interface with dry-run support.
"""

import argparse
import sys
import time
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional

from src.config import logger
from src.db.connection import get_connection
from src.deals.models import NormalizedDeal, IngestionResult
from src.deals.normalizer import DealNormalizer
from src.deals.sources.nse_source import NSESource
from src.deals.sources.bse_source import BSESource
from src.deals.sources.csv_source import CSVDealSource


class DealEngine:
    """Core synchronization and backfill engine for Bulk & Block Deals."""

    def __init__(self):
        self.nse_source = NSESource()
        self.bse_source = BSESource()

    def sync_recent_deals(
        self,
        lookback_days: int = 5,
        deal_type: str = "ALL",
        dry_run: bool = False
    ) -> List[IngestionResult]:
        """
        Synchronizes recent deals across enabled exchanges.
        lookback_days=5 catches trades executed across weekends/holidays and late broker reconciliations.
        """
        to_date = date.today()
        from_date = to_date - timedelta(days=lookback_days)
        logger.info(f"Starting Deal Sync: {from_date} to {to_date} (deal_type={deal_type}, dry_run={dry_run})")

        results: List[IngestionResult] = []

        # 1. Fetch NSE
        if self.nse_source.is_enabled:
            res_nse = self._execute_source_run(
                source=self.nse_source,
                exchange="NSE",
                deal_type=deal_type,
                from_date=from_date,
                to_date=to_date,
                dry_run=dry_run
            )
            results.append(res_nse)

        # 2. Fetch BSE
        if self.bse_source.is_enabled:
            res_bse = self._execute_source_run(
                source=self.bse_source,
                exchange="BSE",
                deal_type=deal_type,
                from_date=from_date,
                to_date=to_date,
                dry_run=dry_run
            )
            results.append(res_bse)

        return results

    def backfill_history(
        self,
        months: int = 12,
        batch_days: int = 30,
        deal_type: str = "ALL",
        dry_run: bool = False
    ) -> List[IngestionResult]:
        """
        Resumable historical backfill over specified months in bounded chunk batches.
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=months * 30)
        logger.info(f"Starting Historical Backfill: {start_date} to {end_date} in {batch_days}-day batches (dry_run={dry_run})")

        all_results: List[IngestionResult] = []
        curr_from = start_date

        while curr_from <= end_date:
            curr_to = min(curr_from + timedelta(days=batch_days - 1), end_date)
            logger.info(f"Backfill batch window: {curr_from} to {curr_to}")

            # Run NSE
            if self.nse_source.is_enabled:
                r_nse = self._execute_source_run(
                    source=self.nse_source,
                    exchange="NSE",
                    deal_type=deal_type,
                    from_date=curr_from,
                    to_date=curr_to,
                    run_mode="BACKFILL",
                    dry_run=dry_run
                )
                all_results.append(r_nse)

            # Run BSE
            if self.bse_source.is_enabled:
                r_bse = self._execute_source_run(
                    source=self.bse_source,
                    exchange="BSE",
                    deal_type=deal_type,
                    from_date=curr_from,
                    to_date=curr_to,
                    run_mode="BACKFILL",
                    dry_run=dry_run
                )
                all_results.append(r_bse)

            curr_from = curr_to + timedelta(days=1)
            time.sleep(1.0)  # Safe rate limit pause between historical batches

        return all_results

    def import_manual_csv(
        self,
        csv_content: str,
        filename: str = "upload.csv",
        forced_deal_type: Optional[str] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Validates and imports manual CSV file with dry-run support.
        """
        valid_deals, rejected_rows = CSVDealSource.parse_and_validate(
            csv_content=csv_content,
            filename=filename,
            forced_deal_type=forced_deal_type
        )

        if dry_run or not valid_deals:
            return {
                "dry_run": dry_run,
                "valid_records_count": len(valid_deals),
                "rejected_records_count": len(rejected_rows),
                "rejected_details": rejected_rows[:20],  # sample up to 20 errors
                "sample_preview": [d.__dict__ for d in valid_deals[:5]]
            }

        # Persist valid deals
        result = self._persist_deals(
            deals=valid_deals,
            exchange="MIXED",
            deal_type=forced_deal_type or "ALL",
            from_date=min(d.trade_date for d in valid_deals),
            to_date=max(d.trade_date for d in valid_deals),
            run_mode="MANUAL_CSV"
        )

        return {
            "dry_run": False,
            "valid_records_count": len(valid_deals),
            "inserted": result.records_inserted,
            "skipped": result.records_skipped,
            "rejected_records_count": len(rejected_rows),
            "rejected_details": rejected_rows[:20]
        }

    def _execute_source_run(
        self,
        source: Any,
        exchange: str,
        deal_type: str,
        from_date: date,
        to_date: date,
        run_mode: str = "AUTOMATED",
        dry_run: bool = False
    ) -> IngestionResult:
        """Executes a single source collection run and records metrics."""
        t_start = time.time()
        logger.info(f"Querying {source.source_name} deals between {from_date} and {to_date}")

        try:
            deals = source.fetch_deals(from_date=from_date, to_date=to_date, deal_type=deal_type)
        except Exception as e:
            logger.error(f"Failed to fetch from {source.source_name}: {e}")
            return IngestionResult(
                source_name=source.source_name,
                deal_type=deal_type,
                exchange=exchange,
                records_fetched=0,
                error_summary=str(e),
                duration_seconds=round(time.time() - t_start, 2)
            )

        if dry_run:
            logger.info(f"[DRY-RUN] {source.source_name} fetched {len(deals)} deals.")
            return IngestionResult(
                source_name=source.source_name,
                deal_type=deal_type,
                exchange=exchange,
                records_fetched=len(deals),
                records_skipped=len(deals),
                duration_seconds=round(time.time() - t_start, 2)
            )

        # Persist into DB
        result = self._persist_deals(
            deals=deals,
            exchange=exchange,
            deal_type=deal_type,
            from_date=from_date,
            to_date=to_date,
            run_mode=run_mode
        )
        result.duration_seconds = round(time.time() - t_start, 2)
        return result

    def _persist_deals(
        self,
        deals: List[NormalizedDeal],
        exchange: str,
        deal_type: str,
        from_date: date,
        to_date: date,
        run_mode: str = "AUTOMATED"
    ) -> IngestionResult:
        """Persists deals idempotently into PostgreSQL within a safe transaction."""
        conn = get_connection()
        res = IngestionResult(
            source_name=f"{exchange}_{deal_type}",
            deal_type=deal_type,
            exchange=exchange,
            records_fetched=len(deals)
        )

        run_id = None
        try:
            with conn.cursor() as cur:
                # 1. Create Ingestion Run Record
                cur.execute(
                    """
                    INSERT INTO market_deal_ingestion_runs (
                        run_mode, exchange, deal_type, from_date, to_date, status, records_fetched
                    ) VALUES (%s, %s, %s, %s, %s, 'RUNNING', %s)
                    RETURNING run_id
                    """,
                    (run_mode, exchange, deal_type, from_date, to_date, len(deals))
                )
                run_id = cur.fetchone()[0]

                # 2. Batch Upsert Deals
                insert_sql = """
                    INSERT INTO market_deals (
                        deal_type, exchange, trade_date, company_id, isin, symbol, security_name,
                        client_name, investor_category, action, quantity, trade_price, trade_value_inr,
                        pct_equity_traded, source_fingerprint, source_record_id, source_name, ingestion_mode
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (source_fingerprint) DO UPDATE SET
                        company_id = COALESCE(EXCLUDED.company_id, market_deals.company_id),
                        isin = COALESCE(EXCLUDED.isin, market_deals.isin),
                        investor_category = EXCLUDED.investor_category,
                        pct_equity_traded = COALESCE(EXCLUDED.pct_equity_traded, market_deals.pct_equity_traded),
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING (xmax = 0) AS is_inserted
                """

                for d in deals:
                    params = (
                        d.deal_type, d.exchange, d.trade_date, d.company_id, d.isin, d.symbol, d.security_name,
                        d.client_name, d.investor_category, d.action, d.quantity, d.trade_price, d.trade_value_inr,
                        d.pct_equity_traded, d.source_fingerprint, d.source_record_id, d.source_name, d.ingestion_mode
                    )
                    cur.execute(insert_sql, params)
                    is_inserted = cur.fetchone()[0]
                    if is_inserted:
                        res.records_inserted += 1
                    else:
                        res.records_updated += 1

                # 3. Update Sync State Checkpoint
                sync_key = f"{exchange}_{deal_type}_DEALS" if exchange != "MIXED" else "MANUAL_CSV_IMPORT"
                cur.execute(
                    """
                    INSERT INTO deal_sync_state (
                        source_key, last_attempted_at, last_successful_at, status,
                        earliest_date_covered, latest_date_covered, total_deals_count
                    ) VALUES (
                        %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'OK', %s, %s, %s
                    )
                    ON CONFLICT (source_key) DO UPDATE SET
                        last_attempted_at = CURRENT_TIMESTAMP,
                        last_successful_at = CURRENT_TIMESTAMP,
                        status = 'OK',
                        earliest_date_covered = LEAST(deal_sync_state.earliest_date_covered, EXCLUDED.earliest_date_covered),
                        latest_date_covered = GREATEST(deal_sync_state.latest_date_covered, EXCLUDED.latest_date_covered),
                        total_deals_count = (SELECT COUNT(*) FROM market_deals WHERE exchange = %s OR %s = 'MIXED'),
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (sync_key, from_date, to_date, res.records_inserted + res.records_updated, exchange, exchange)
                )

                # 4. Finalize Ingestion Run Record
                cur.execute(
                    """
                    UPDATE market_deal_ingestion_runs
                    SET end_time = CURRENT_TIMESTAMP,
                        status = 'SUCCESS',
                        records_inserted = %s,
                        records_updated = %s,
                        records_skipped = %s,
                        records_rejected = %s
                    WHERE run_id = %s
                    """,
                    (res.records_inserted, res.records_updated, res.records_skipped, res.records_rejected, run_id)
                )

                conn.commit()
                logger.info(f"Persisted {len(deals)} deals: {res.records_inserted} inserted, {res.records_updated} updated.")

        except Exception as e:
            conn.rollback()
            logger.error(f"Error persisting deals: {e}")
            res.error_summary = str(e)
            if run_id:
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE market_deal_ingestion_runs SET end_time = CURRENT_TIMESTAMP, status = 'FAILED', error_summary = %s WHERE run_id = %s",
                            (str(e), run_id)
                        )
                        conn.commit()
                except Exception:
                    pass

        return res


def main():
    """CLI Entrypoint."""
    parser = argparse.ArgumentParser(description="Bulk and Block Deals Automation Engine")
    parser.add_argument("--mode", choices=["sync", "backfill", "status"], default="sync", help="Operation mode")
    parser.add_argument("--days", type=int, default=5, help="Lookback days for sync mode")
    parser.add_argument("--months", type=int, default=12, help="Months to backfill")
    parser.add_argument("--type", choices=["BULK", "BLOCK", "ALL"], default="ALL", help="Deal type")
    parser.add_argument("--dry-run", action="store_true", help="Dry run without writing to database")
    args = parser.parse_args()

    engine = DealEngine()

    if args.mode == "sync":
        print(f"Executing deal sync (lookback={args.days} days, type={args.type}, dry_run={args.dry_run})...")
        results = engine.sync_recent_deals(lookback_days=args.days, deal_type=args.type, dry_run=args.dry_run)
        for r in results:
            print(f"[{r.source_name}] Fetched: {r.records_fetched}, Inserted: {r.records_inserted}, Updated: {r.records_updated}, Errors: {r.error_summary}")

    elif args.mode == "backfill":
        print(f"Executing historical backfill ({args.months} months, type={args.type}, dry_run={args.dry_run})...")
        results = engine.backfill_history(months=args.months, deal_type=args.type, dry_run=args.dry_run)
        total_inserted = sum(r.records_inserted for r in results)
        print(f"Backfill finished. Total deals inserted: {total_inserted}")

    elif args.mode == "status":
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT source_key, status, last_successful_at, earliest_date_covered, latest_date_covered, total_deals_count FROM deal_sync_state")
            rows = cur.fetchall()
            print("--- Deal Sync State ---")
            for r in rows:
                print(f"{r[0]}: status={r[1]}, last_success={r[2]}, range={r[3]} to {r[4]}, total={r[5]}")


if __name__ == "__main__":
    main()
