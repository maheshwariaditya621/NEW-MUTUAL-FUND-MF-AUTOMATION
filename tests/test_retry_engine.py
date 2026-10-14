"""
Unit test for transient failure retry engine and backoff.
"""

import time
from unittest.mock import patch, MagicMock
from src.scheduler.master_pipeline import MasterPipelineRunner, is_transient_error
from src.db.ledger_repository import is_amc_period_successful

def test_transient_detection():
    assert is_transient_error("HTTPSConnectionPool: Read timed out. (read timeout=60)") is True
    assert is_transient_error("HTTP 429 Too Many Requests") is True
    assert is_transient_error("503 Service Unavailable") is True
    assert is_transient_error("Connection reset by peer") is True
    assert is_transient_error("404 Not Found") is False
    assert is_transient_error("ParserError: Invalid header format") is False
    print("✓ Transient error classification passed.")

def test_runner_retry_behavior():
    runner = MasterPipelineRunner(
        year=2026,
        month=7,
        amc_slugs=["test_mock_amc"],
        concurrency=1,
        max_retries=2,
        backoff_delays=[1, 2]
    )

    attempt_counter = {"count": 0}

    def mock_run_pipeline(*args, **kwargs):
        attempt_counter["count"] += 1
        if attempt_counter["count"] < 3:
            return {
                "status": "failed",
                "steps": {
                    "download": {"status": "failed", "error": "Connection timed out after 30s"}
                }
            }
        return {
            "status": "success",
            "steps": {
                "download": {"status": "success", "files_downloaded": 5},
                "merge": {"status": "success"},
                "extract": {"status": "success", "rows_read": 150, "rows_inserted": 150, "amc_id": 1, "period_id": 1}
            }
        }

    with patch("src.scheduler.master_pipeline.PipelineOrchestrator") as MockOrch:
        instance = MockOrch.return_value
        instance.run_pipeline.side_effect = mock_run_pipeline

        summary = runner.run()
        print(f"Summary: {summary}")
        assert attempt_counter["count"] == 3, f"Expected 3 attempts, got {attempt_counter['count']}"
        assert summary["success_count"] == 1, "Expected run to succeed after 2 retries"
        print("✓ Retry loop with backoff successfully recovered after 2 transient failures!")

if __name__ == "__main__":
    test_transient_detection()
    test_runner_retry_behavior()
