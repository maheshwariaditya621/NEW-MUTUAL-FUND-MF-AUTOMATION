"""
Unit test for empty / corrupt response protection.
"""

from unittest.mock import patch, MagicMock
from src.extractors.orchestrator import ExtractionOrchestrator
from src.scheduler.master_pipeline import MasterPipelineRunner
from src.db.ledger_repository import is_amc_period_successful

def test_empty_holdings_protection():
    orchestrator = ExtractionOrchestrator()

    # Mock extractor returning 0 holdings
    mock_extractor = MagicMock()
    mock_extractor.version = "v1"
    mock_extractor.amc_name = "CANARA ROBECO MUTUAL FUND"
    mock_extractor.extract.return_value = [] # 0 rows!

    with patch("src.extractors.orchestrator.ExtractorFactory.get_extractor", return_value=mock_extractor):
        with patch.object(orchestrator, "compute_file_hash", return_value="dummyhash123"):
            with patch("pathlib.Path.exists", return_value=True):
                res = orchestrator.process_amc_month(
                    amc_slug="canara",
                    year=2026,
                    month=7,
                    dry_run=False
                )
                print(f"Empty extraction result: {res}")
                assert res["status"] == "failed", f"Expected failed status, got {res['status']}"
                assert res["reason"] == "zero_holdings_extracted", f"Expected zero_holdings_extracted, got {res.get('reason')}"
                print("✓ Empty response guard successfully blocked database write!")

if __name__ == "__main__":
    test_empty_holdings_protection()
