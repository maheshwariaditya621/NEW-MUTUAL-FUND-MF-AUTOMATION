import os
import shutil
import pathlib
from typing import List, Dict, Any
from datetime import datetime
from src.config import logger

class AdminFileService:
    """Service to manage raw and merged files from the admin panel."""

    def __init__(self, base_dir: str = "data"):
        self.base_dir = os.path.abspath(base_dir)
        self.raw_dir = os.path.join(self.base_dir, "raw")
        self.merged_dir = os.path.join(self.base_dir, "output", "merged excels")

    def _safe_path(self, path: str) -> bool:
        """Ensure the path is within the data directory."""
        abs_path = os.path.abspath(path)
        return abs_path.startswith(self.base_dir)

    def get_inventory(self, cur) -> List[Dict[str, Any]]:
        """
        Scans filesystem and cross-references with DB snapshots.
        Returns a list of AMC/Period combinations and their file status.
        """
        inventory = {}

        # 1. Scan DB for 'Loaded' status
        cur.execute("""
            SELECT a.amc_slug, a.amc_name, p.year, p.month, COUNT(s.snapshot_id)
            FROM snapshots s
            JOIN schemes sc ON s.scheme_id = sc.scheme_id
            JOIN amcs a ON sc.amc_id = a.amc_id
            JOIN periods p ON s.period_id = p.period_id
            GROUP BY a.amc_slug, a.amc_name, p.year, p.month
        """)
        for slug, name, year, month, count in cur.fetchall():
            key = (slug, year, month)
            inventory[key] = {
                "amc_slug": slug,
                "amc_name": name,
                "year": year,
                "month": month,
                "raw_present": False,
                "merged_present": False,
                "db_loaded": count > 0,
                "snapshots_count": count
            }

        # 2. Scan Raw Files
        if os.path.exists(self.raw_dir):
            for amc_slug in os.listdir(self.raw_dir):
                amc_path = os.path.join(self.raw_dir, amc_slug)
                if not os.path.isdir(amc_path): continue
                for year_str in os.listdir(amc_path):
                    year_path = os.path.join(amc_path, year_str)
                    if not os.path.isdir(year_path): continue
                    for month_str in os.listdir(year_path):
                        try:
                            year, month = int(year_str), int(month_str)
                            key = (amc_slug, year, month)
                            if key not in inventory:
                                inventory[key] = self._empty_item(amc_slug, year, month)
                            inventory[key]["raw_present"] = True
                        except ValueError:
                            continue

        # 3. Scan Merged Files
        if os.path.exists(self.merged_dir):
            for amc_slug in os.listdir(self.merged_dir):
                amc_path = os.path.join(self.merged_dir, amc_slug)
                if not os.path.isdir(amc_path): continue
                for year_str in os.listdir(amc_path):
                    year_path = os.path.join(amc_path, year_str)
                    if not os.path.isdir(year_path): continue
                    for filename in os.listdir(year_path):
                        # CONSOLIDATED_HDFC_2025_11.xlsx
                        if filename.startswith("CONSOLIDATED_") and filename.endswith(".xlsx"):
                            try:
                                parts = filename.replace(".xlsx", "").split("_")
                                year, month = int(parts[-2]), int(parts[-1])
                                key = (amc_slug, year, month)
                                if key not in inventory:
                                    inventory[key] = self._empty_item(amc_slug, year, month)
                                inventory[key]["merged_present"] = True
                            except (ValueError, IndexError):
                                continue

        return sorted(inventory.values(), key=lambda x: (x['year'], x['month'], x['amc_slug']), reverse=True)

    def _empty_item(self, slug, year, month):
        return {
            "amc_slug": slug,
            "amc_name": slug.upper(),
            "year": year,
            "month": month,
            "raw_present": False,
            "merged_present": False,
            "db_loaded": False,
            "snapshots_count": 0
        }

    def delete_files(self, amc_slug: str, year: int, month: int, category: str) -> bool:
        """Delete files/folders for a specific AMC and month."""
        target_path = None
        if category == "raw":
            target_path = os.path.join(self.raw_dir, amc_slug, str(year), str(month))
        elif category == "merged":
            filename = f"CONSOLIDATED_{amc_slug.upper()}_{year}_{month:02d}.xlsx"
            target_path = os.path.join(self.merged_dir, amc_slug, str(year), filename)
            # Try without padding if first fails
            if not os.path.exists(target_path):
                filename_alt = f"CONSOLIDATED_{amc_slug.upper()}_{year}_{month}.xlsx"
                target_path = os.path.join(self.merged_dir, amc_slug, str(year), filename_alt)

        if not target_path or not os.path.exists(target_path):
            return False

        if not self._safe_path(target_path):
            raise PermissionError(f"Attempted to delete path outside data directory: {target_path}")

        try:
            if os.path.isdir(target_path):
                shutil.rmtree(target_path)
            else:
                os.remove(target_path)
            logger.info(f"Admin deleted {category} files for {amc_slug} {year}-{month}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete {target_path}: {e}")
            return False

    def get_storage_stats(self) -> Dict[str, Any]:
        """Calculate total disk usage for raw and merged data."""
        def get_size(path):
            total_size = 0
            if not os.path.exists(path): return 0
            for dirpath, dirnames, filenames in os.walk(path):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    total_size += os.path.getsize(fp)
            return total_size

        raw_size = get_size(self.raw_dir)
        merged_size = get_size(self.merged_dir)
        
        return {
            "raw_size_mb": round(raw_size / (1024 * 1024), 2),
            "merged_size_mb": round(merged_size / (1024 * 1024), 2),
            "total_size_mb": round((raw_size + merged_size) / (1024 * 1024), 2)
        }

admin_file_service = AdminFileService()
