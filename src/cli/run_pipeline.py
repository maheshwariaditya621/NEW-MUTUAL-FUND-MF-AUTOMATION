"""
CLI entrypoint for data ingestion pipeline.

Provides command-line interface for testing and running the pipeline.
"""

import argparse
import sys
from decimal import Decimal

from src.loaders import EquityHoldingsLoader
from src.validators import ValidationError
from src.alerts import alerter
from src.utils import format_period
from src.config import logger


def main():
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="Mutual Fund Portfolio Analytics - Data Ingestion Pipeline"
    )
    
    parser.add_argument("--amc", required=True, help="AMC name")
    parser.add_argument("--scheme", required=True, help="Scheme name")
    parser.add_argument("--plan", required=True, choices=["Direct", "Regular"], help="Plan type")
    parser.add_argument("--option", required=True, choices=["Growth", "Dividend", "IDCW"], help="Option type")
    parser.add_argument("--year", required=True, type=int, help="Year (YYYY)")
    parser.add_argument("--month", required=True, type=int, help="Month (1-12)")
    parser.add_argument("--file", help="Source file path (for future use)")
    parser.add_argument("--test", action="store_true", help="Run with test data")
    
    args = parser.parse_args()
    
    # Format period for display
    period_str = format_period(args.year, args.month)
    
    logger.info("=" * 60)
    logger.info("MUTUAL FUND PORTFOLIO ANALYTICS - DATA INGESTION PIPELINE")
    logger.info("=" * 60)
    logger.info(f"AMC: {args.amc}")
    logger.info(f"Scheme: {args.scheme}")
    logger.info(f"Plan: {args.plan}")
    logger.info(f"Option: {args.option}")
    logger.info(f"Period: {period_str}")
    logger.info("=" * 60)
    
    # For now, use test data if --test flag is provided
    if args.test:
        logger.info("Using test data")
        holdings_data = create_test_data()
    else:
        logger.error("No extractor implemented yet. Use --test flag for testing.")
        sys.exit(1)
    
    # Load data
    try:
        loader = EquityHoldingsLoader()
        
        snapshot_id = loader.load_scheme_month(
            amc_name=args.amc,
            scheme_name=args.scheme,
            plan_type=args.plan,
            option_type=args.option,
            year=args.year,
            month=args.month,
            holdings_data=holdings_data
        )
        
        logger.info("=" * 60)
        logger.success(f"✅ SUCCESS: Data loaded successfully (snapshot_id={snapshot_id})")
        logger.info("=" * 60)
        
        # Send success notification
        alerter.send_success_notification(
            amc=args.amc,
            scheme=f"{args.scheme} - {args.plan} - {args.option}",
            period=period_str,
            snapshot_id=snapshot_id,
            holdings_count=len(holdings_data)
        )
        
    except ValidationError as e:
        logger.error(f"Validation failed: {e}")
        
        # Send validation error alert
        alerter.send_validation_error(
            amc=args.amc,
            scheme=f"{args.scheme} - {args.plan} - {args.option}",
            period=period_str,
            error_details=str(e),
            file_name=args.file
        )
        
        logger.info("=" * 60)
        logger.error("❌ FAILED: Scheme-month skipped due to validation error")
        logger.info("=" * 60)
        
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        
        # Send rollback alert
        alerter.send_rollback_alert(
            amc=args.amc,
            scheme=f"{args.scheme} - {args.plan} - {args.option}",
            period=period_str,
            error_message=str(e)
        )
        
        logger.info("=" * 60)
        logger.error("❌ FAILED: Transaction rolled back")
        logger.info("=" * 60)
        
        sys.exit(1)


def create_test_data():
    """
    Create test holdings data for testing.
    
    Returns:
        List of test holdings
    """
    return [
        {
            'isin': 'INE002A01018',
            'company_name': 'Reliance Industries Limited',
            'quantity': 125000,
            'market_value_inr': Decimal('306250000.00'),
            'percent_of_nav': Decimal('3.06'),
            'exchange_symbol': 'RELIANCE',
            'sector': 'Energy',
            'industry': 'Oil & Gas',
        },
        {
            'isin': 'INE040A01034',
            'company_name': 'HDFC Bank Limited',
            'quantity': 180000,
            'market_value_inr': Decimal('285000000.00'),
            'percent_of_nav': Decimal('2.85'),
            'exchange_symbol': 'HDFCBANK',
            'sector': 'Financial Services',
            'industry': 'Banking',
        },
        {
            'isin': 'INE009A01021',
            'company_name': 'Infosys Limited',
            'quantity': 150000,
            'market_value_inr': Decimal('240000000.00'),
            'percent_of_nav': Decimal('2.40'),
            'exchange_symbol': 'INFY',
            'sector': 'Information Technology',
            'industry': 'IT Services',
        },
    ]


if __name__ == "__main__":
    main()
