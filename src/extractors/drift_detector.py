import hashlib
import json
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from src.config import logger
from src.alerts.telegram_notifier import get_notifier

class DriftDetector:
    """
    Detects changes in AMC Excel structures (Schema Drift).
    Stores fingerprints of sorted column names.
    """

    def __init__(self, fingerprint_dir: str = "data/config/fingerprints"):
        self.fingerprint_dir = Path(fingerprint_dir)
        self.fingerprint_dir.mkdir(parents=True, exist_ok=True)
        self.notifier = get_notifier()

    def _get_path(self, amc_slug: str, version: str) -> Path:
        return self.fingerprint_dir / f"{amc_slug.lower()}_{version}.json"

    def check_drift(self, amc_slug: str, version: str, current_columns: List[str]) -> Tuple[bool, str]:
        """
        Compares current columns with knows fingerprint.
        Returns (drift_detected, fingerprint).
        """
        sorted_cols = sorted([str(c).upper() for c in current_columns])
        fingerprint = hashlib.sha256(json.dumps(sorted_cols).encode()).hexdigest()
        
        path = self._get_path(amc_slug, version)
        
        if not path.exists():
            logger.info(f"First-time footprint for {amc_slug} {version}. Saving.")
            with open(path, "w") as f:
                json.dump({"columns": sorted_cols, "hash": fingerprint}, f)
            return False, fingerprint

        with open(path, "r") as f:
            known = json.load(f)

        if known["hash"] != fingerprint:
            logger.error(f"SCHEMA DRIFT DETECTED for {amc_slug} {version}!")
            # ... logging ...
            return True, fingerprint

        return False, fingerprint
