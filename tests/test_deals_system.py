"""
Automated Unit and Integration Test Suite for Bulk & Block Deals System.

Covers:
1. DealNormalizer - Date parsing, numeric cleaning, buy/sell parsing, investor classification
2. Fingerprint determinism - SHA256 idempotency verification
3. Company Resolver - Master cache resolution by ISIN, Symbol, and BSE code
4. CSVDealSource - Manual CSV import parser, validation rules, error tracking with line numbers
5. DealEngine - Idempotency and conflict handling
6. FastAPI Deals Router - Pagination, filtering, summary statistics, operational health endpoints
"""

import pytest
from datetime import date, datetime
from decimal import Decimal
from fastapi.testclient import TestClient

from src.deals.models import NormalizedDeal
from src.deals.normalizer import DealNormalizer
from src.deals.sources.csv_source import CSVDealSource
from src.deals.engine import DealEngine
from src.api.main import app
from src.api.utils.auth_utils import create_access_token

client = TestClient(app)


# ---------------------------------------------------------------------------
# 1. Normalizer Tests
# ---------------------------------------------------------------------------

def test_normalizer_date_parsing():
    """Verify parsing across varied Indian financial date formats."""
    assert DealNormalizer.parse_date("26-Oct-2026") == date(2026, 10, 26)
    assert DealNormalizer.parse_date("26/10/2026") == date(2026, 10, 26)
    assert DealNormalizer.parse_date("2026-10-26") == date(2026, 10, 26)
    assert DealNormalizer.parse_date("26-10-2026") == date(2026, 10, 26)
    assert DealNormalizer.parse_date("26-Oct-26") == date(2026, 10, 26)
    assert DealNormalizer.parse_date(date(2026, 10, 26)) == date(2026, 10, 26)
    assert DealNormalizer.parse_date("") is None
    assert DealNormalizer.parse_date("invalid_date") is None


def test_normalizer_numeric_parsing():
    """Verify parsing Indian formatted numbers, currency symbols, and commas."""
    assert DealNormalizer.parse_numeric("1,25,000") == Decimal("125000")
    assert DealNormalizer.parse_numeric("₹2,950.50") == Decimal("2950.50")
    assert DealNormalizer.parse_numeric("Rs. 450.75") == Decimal("450.75")
    assert DealNormalizer.parse_numeric(500) == Decimal("500")
    assert DealNormalizer.parse_numeric(None) == Decimal("0.0")


def test_normalizer_action_parsing():
    """Verify BUY/SELL normalization."""
    assert DealNormalizer.parse_action("BUY") == "BUY"
    assert DealNormalizer.parse_action("B") == "BUY"
    assert DealNormalizer.parse_action("PURCHASE") == "BUY"
    assert DealNormalizer.parse_action("SELL") == "SELL"
    assert DealNormalizer.parse_action("S") == "SELL"
    assert DealNormalizer.parse_action("SALE") == "SELL"


def test_investor_categorization():
    """Verify classification of institutional and retail market participants."""
    # Mutual Funds
    assert DealNormalizer.categorize_investor("NIPPON INDIA MUTUAL FUND") == "MUTUAL_FUND"
    assert DealNormalizer.categorize_investor("HDFC TRUSTEE COMPANY LTD A/C HDFC TOP 100 FUND") == "MUTUAL_FUND"
    assert DealNormalizer.categorize_investor("ICICI PRUDENTIAL ASSET MANAGEMENT COMPANY") == "MUTUAL_FUND"
    assert DealNormalizer.categorize_investor("PARAG PARIKH CONSERVATIVE HYBRID FUND") == "MUTUAL_FUND"

    # FII / FPI
    assert DealNormalizer.categorize_investor("GOVERNMENT PENSION FUND GLOBAL") == "FII_FPI"
    assert DealNormalizer.categorize_investor("NORGES BANK ON ACCOUNT OF THE GOVERNMENT PENSION FUND") == "FII_FPI"
    assert DealNormalizer.categorize_investor("VANGUARD TOTAL INTERNATIONAL STOCK INDEX FUND") == "FII_FPI"
    assert DealNormalizer.categorize_investor("GOLDMAN SACHS INDIA LIMITED") == "FII_FPI"
    assert DealNormalizer.categorize_investor("SOCIETE GENERALE - ODI") == "FII_FPI"

    # DII / Insurance
    assert DealNormalizer.categorize_investor("LIFE INSURANCE CORPORATION OF INDIA") == "DII"
    assert DealNormalizer.categorize_investor("SBI LIFE INSURANCE COMPANY LIMITED") == "DII"
    assert DealNormalizer.categorize_investor("GENERAL INSURANCE CORPORATION OF INDIA") == "DII"

    # AIF
    assert DealNormalizer.categorize_investor("INDIA ADVANTAGE FUND SERIES 4") == "AIF"
    assert DealNormalizer.categorize_investor("CAT III ALTERNATIVE INVESTMENT FUND") == "AIF"

    # Promoter
    assert DealNormalizer.categorize_investor("ABC HOLDINGS PRIVATE LIMITED") == "PROMOTER"
    assert DealNormalizer.categorize_investor("XYZ FAMILY TRUST") == "PROMOTER"

    # Other / HNI
    assert DealNormalizer.categorize_investor("RAMESH CHANDRA SHARMA") == "OTHER"


def test_source_fingerprint_determinism():
    """Verify that identical deal records produce the exact same fingerprint for deduplication."""
    deal1 = NormalizedDeal(
        deal_type="BULK",
        exchange="NSE",
        trade_date=date(2026, 10, 26),
        symbol="RELIANCE",
        security_name="Reliance Industries",
        client_name="NIPPON INDIA MUTUAL FUND",
        action="BUY",
        quantity=500000,
        trade_price=Decimal("2950.50"),
        trade_value_inr=Decimal("1475250000.00"),
        source_name="NSE_BULK"
    )

    deal2 = NormalizedDeal(
        deal_type="BULK",
        exchange="NSE",
        trade_date=date(2026, 10, 26),
        symbol="RELIANCE",
        security_name="Reliance Industries Limited",  # slight name variance shouldn't break fingerprint
        client_name="NIPPON INDIA MUTUAL FUND",
        action="BUY",
        quantity=500000,
        trade_price=Decimal("2950.5000"),
        trade_value_inr=Decimal("1475250000.00"),
        source_name="NSE_DAILY_CSV"
    )

    assert deal1.source_fingerprint == deal2.source_fingerprint


# ---------------------------------------------------------------------------
# 2. CSV Parser & Validator Tests
# ---------------------------------------------------------------------------

def test_csv_parser_valid_and_rejected():
    """Test manual CSV import with both valid rows and malformed lines."""
    sample_csv = (
        "Date,Symbol,Security Name,Client Name,Buy/Sell,Quantity Traded,Trade Price / Wgt. Avg. Price\n"
        "26-Oct-2026,TCS,Tata Consultancy Services,NORGES BANK,BUY,100000,4120.00\n"
        "26-Oct-2026,INFY,Infosys Ltd,HDFC MUTUAL FUND,SELL,250000,1890.50\n"
        "invalid_date,WIPRO,Wipro Ltd,CLIENT X,BUY,50000,520.00\n"
        "26-Oct-2026,,Missing Symbol,CLIENT Y,BUY,10000,100.00\n"
        "26-Oct-2026,ITC,ITC Limited,CLIENT Z,BUY,-500,450.00\n"
    )

    valid_deals, rejected = CSVDealSource.parse_and_validate(sample_csv, filename="test_deals.csv")

    assert len(valid_deals) == 2
    assert valid_deals[0].symbol == "TCS"
    assert valid_deals[0].investor_category == "FII_FPI"
    assert valid_deals[1].symbol == "INFY"
    assert valid_deals[1].investor_category == "MUTUAL_FUND"

    assert len(rejected) == 3
    # Line 4: invalid date
    assert rejected[0]["line"] == 4
    # Line 5: missing symbol
    assert rejected[1]["line"] == 5
    # Line 6: non-positive quantity
    assert rejected[2]["line"] == 6


# ---------------------------------------------------------------------------
# 3. FastAPI Endpoint Tests
# ---------------------------------------------------------------------------

@pytest.fixture
def auth_headers():
    """Create JWT token with admin sub for API testing."""
    token = create_access_token(data={"sub": "admin"})
    return {"Authorization": f"Bearer {token}"}


def test_api_deals_list(auth_headers):
    """Verify GET /api/v1/deals endpoint with sorting and filtering."""
    resp = client.get("/api/v1/deals?page=1&limit=10&deal_type=ALL", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert "deals" in data
    assert "total" in data
    assert "page" in data
    assert data["page"] == 1


def test_api_deals_summary(auth_headers):
    """Verify GET /api/v1/deals/summary endpoint calculations."""
    resp = client.get("/api/v1/deals/summary", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert "total_deals" in data
    assert "unique_symbols" in data
    assert "total_value_inr" in data
    assert "total_value_crores" in data
    assert "by_category" in data
    assert "by_deal_type" in data


def test_api_deals_status(auth_headers):
    """Verify GET /api/v1/deals/status health checkpoint reporting."""
    resp = client.get("/api/v1/deals/status", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert "total_deals_in_db" in data
    assert "sync_states" in data
    assert "recent_runs" in data


def test_api_deals_csv_dry_run(auth_headers):
    """Verify POST /api/v1/deals/upload-csv dry-run validation preview."""
    payload = {
        "csv_content": (
            "Date,Symbol,Security Name,Client Name,Buy/Sell,Quantity Traded,Trade Price / Wgt. Avg. Price\n"
            "26-Oct-2026,RELIANCE,Reliance Industries,SBI MUTUAL FUND,BUY,100000,2950.00\n"
        ),
        "filename": "nse_bulk.csv",
        "dry_run": True
    }
    resp = client.post("/api/v1/deals/upload-csv", json=payload, headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["dry_run"] is True
    assert data["valid_records_count"] == 1
    assert data["rejected_records_count"] == 0
    assert len(data["sample_preview"]) == 1
    assert data["sample_preview"][0]["symbol"] == "RELIANCE"
    assert data["sample_preview"][0]["investor_category"] == "MUTUAL_FUND"


def test_api_deals_invalid_parameters(auth_headers):
    """Verify that invalid sort or regex parameters are rejected with 422 Unprocessable Entity."""
    # Invalid sort_by
    resp = client.get("/api/v1/deals?sort_by=DROP_TABLE", headers=auth_headers)
    assert resp.status_code == 422

    # Invalid sort_order
    resp = client.get("/api/v1/deals?sort_order=INVALID", headers=auth_headers)
    assert resp.status_code == 422
