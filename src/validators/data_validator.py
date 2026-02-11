from typing import List, Dict, Any, Tuple
from src.config import logger
from src.alerts.telegram_notifier import get_notifier

class DataValidator:
    """
    Validates extracted portfolio data for:
    - Total Market Value checksum
    - Missing required columns
    - Significant data mismatches (>1%)
    """

    def __init__(self, amc: str):
        self.amc = amc
        self.notifier = get_notifier()

    def validate_totals(self, extracted_holdings: List[Dict[str, Any]], expected_total: float) -> Tuple[bool, float]:
        """
        Compares the sum of extracted market values vs the expected total from Excel.
        Returns (is_valid, deviation_percent).
        """
        extracted_total = sum(h['market_value_inr'] for h in extracted_holdings)
        
        if expected_total == 0:
            return True, 0.0

        deviation = abs(extracted_total - expected_total)
        deviation_percent = (deviation / expected_total) * 100

        if deviation_percent > 1.0:
            logger.error(f"CRITICAL: Data mismatch for {self.amc}. Extracted: {extracted_total}, Expected: {expected_total}. Deviation: {deviation_percent:.2f}%")
            self.notifier.notify_error(
                amc=self.amc,
                year=0, month=0, # Contextualized in caller
                error=f"Total Value Mismatch: {deviation_percent:.2f}% (Extracted: {extracted_total}, Expected: {expected_total})"
            )
            return False, deviation_percent

        logger.info(f"Totals validated for {self.amc}. Deviation: {deviation_percent:.4f}%")
        return True, deviation_percent

    def validate_mandatory_fields(self, holdings: List[Dict[str, Any]]) -> bool:
        """Checks if all required fields are present and non-empty."""
        mandatory = ["isin", "company_name", "quantity", "market_value_inr"]
        
        for h in holdings:
            for field in mandatory:
                if not h.get(field):
                    logger.warning(f"Missing mandatory field '{field}' in holding: {h.get('isin')}")
                    # We don't necessarily fail the whole batch, but we log the issue
        
        return True
