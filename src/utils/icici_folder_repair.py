"""
ICICI Folder Repair Utility

Purpose:
- Fix folder placement for ICICI ZIP files
- Move files to correct YYYY_MM folders based on filename parsing
- Remove invalid _SUCCESS.json markers
- Recreate markers only for valid folders

Safety:
- NO downloads
- NO deletions of ZIP files
- Only moves files to correct locations
"""

import re
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from src.config import logger


class ICICIFolderRepair:
    """Repair ICICI folder structure by moving files to correct month folders."""
    
    BASE_DIR = Path("data/raw/icici")
    CORRUPT_DIR = Path("data/raw/icici/_corrupt")
    
    MONTH_NAMES = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12
    }
    
    def __init__(self, dry_run: bool = False):
        """
        Initialize repair utility.
        
        Args:
            dry_run: If True, only log actions without making changes
        """
        self.dry_run = dry_run
        self.stats = {
            "folders_scanned": 0,
            "files_found": 0,
            "files_moved": 0,
            "success_markers_removed": 0,
            "success_markers_created": 0,
            "errors": 0
        }
    
    def parse_month_year_from_filename(self, filename: str) -> Optional[Tuple[int, int]]:
        """
        Extract (year, month) from ICICI ZIP filename.
        
        Expected patterns:
        - "Monthly Portfolio Disclosure December 2025.zip"
        - "Portfolio Disclosure November 2024.zip"
        
        Args:
            filename: ZIP filename
            
        Returns:
            Tuple of (year, month) or None if parsing fails
        """
        filename_lower = filename.lower()
        
        # Extract year (4 digits)
        year_match = re.search(r'\b(20\d{2})\b', filename)
        if not year_match:
            return None
        year = int(year_match.group(1))
        
        # Extract month name
        month = None
        for month_name, month_num in self.MONTH_NAMES.items():
            if month_name in filename_lower:
                month = month_num
                break
        
        if month is None:
            return None
        
        return (year, month)
    
    def scan_folder(self, folder_path: Path) -> List[Tuple[Path, int, int]]:
        """
        Scan a folder and identify files with their correct month/year.
        
        Args:
            folder_path: Path to folder to scan
            
        Returns:
            List of (file_path, year, month) tuples
        """
        files_info = []
        
        if not folder_path.exists():
            return files_info
        
        for file_path in folder_path.glob("*.zip"):
            parsed = self.parse_month_year_from_filename(file_path.name)
            if parsed:
                year, month = parsed
                files_info.append((file_path, year, month))
            else:
                logger.warning(f"⚠️ Could not parse month/year from: {file_path.name}")
        
        return files_info
    
    def repair_folder(self, folder_path: Path, expected_year: int, expected_month: int) -> bool:
        """
        Repair a single folder by moving misplaced files.
        
        Args:
            folder_path: Path to folder to repair
            expected_year: Expected year for this folder
            expected_month: Expected month for this folder
            
        Returns:
            True if folder is valid after repair, False otherwise
        """
        logger.info(f"🔧 Repairing folder: {folder_path.name}")
        
        files_info = self.scan_folder(folder_path)
        self.stats["files_found"] += len(files_info)
        
        if not files_info:
            logger.info(f"  📭 No ZIP files found")
            return False
        
        # Check for cross-month contamination
        correct_files = []
        wrong_files = []
        
        for file_path, year, month in files_info:
            if year == expected_year and month == expected_month:
                correct_files.append(file_path)
            else:
                wrong_files.append((file_path, year, month))
        
        # Move wrong files to their correct folders
        for file_path, year, month in wrong_files:
            target_folder = self.BASE_DIR / f"{year}_{month:02d}"
            
            logger.warning(f"  ❌ Wrong month: {file_path.name} belongs to {year}-{month:02d}, not {expected_year}-{expected_month:02d}")
            
            if not self.dry_run:
                target_folder.mkdir(parents=True, exist_ok=True)
                target_path = target_folder / file_path.name
                
                # If target already exists, skip (don't overwrite)
                if target_path.exists():
                    logger.warning(f"    ⚠️ Target already exists: {target_path}")
                else:
                    shutil.move(str(file_path), str(target_path))
                    logger.info(f"    ✅ Moved to: {target_folder.name}/")
                    self.stats["files_moved"] += 1
            else:
                logger.info(f"    [DRY RUN] Would move to: {year}_{month:02d}/")
        
        # Remove _SUCCESS.json if folder had wrong files
        success_marker = folder_path / "_SUCCESS.json"
        if wrong_files and success_marker.exists():
            logger.warning(f"  🗑️ Removing invalid _SUCCESS.json (folder had cross-month files)")
            if not self.dry_run:
                success_marker.unlink()
                self.stats["success_markers_removed"] += 1
            else:
                logger.info(f"    [DRY RUN] Would remove _SUCCESS.json")
        
        # Return True if folder has correct files remaining
        return len(correct_files) > 0
    
    def create_success_marker(self, folder_path: Path, year: int, month: int):
        """
        Create _SUCCESS.json for a valid folder.
        
        Args:
            folder_path: Path to folder
            year: Year
            month: Month
        """
        success_marker = folder_path / "_SUCCESS.json"
        
        if success_marker.exists():
            return  # Already has marker
        
        # Count ZIP files
        zip_files = list(folder_path.glob("*.zip"))
        if not zip_files:
            return  # No files, don't create marker
        
        logger.info(f"  ✅ Creating _SUCCESS.json for {year}-{month:02d} ({len(zip_files)} file(s))")
        
        if not self.dry_run:
            with open(success_marker, "w") as f:
                json.dump({
                    "amc": "ICICI",
                    "year": year,
                    "month": month,
                    "files_downloaded": len(zip_files),
                    "timestamp": datetime.utcnow().isoformat(),
                    "repaired": True
                }, f, indent=2)
            self.stats["success_markers_created"] += 1
        else:
            logger.info(f"    [DRY RUN] Would create _SUCCESS.json")
    
    def run_repair(self):
        """Run full repair process on all ICICI folders."""
        logger.info("=" * 70)
        logger.info("🔧 ICICI FOLDER REPAIR UTILITY")
        if self.dry_run:
            logger.info("MODE: DRY RUN (no changes will be made)")
        logger.info("=" * 70)
        
        if not self.BASE_DIR.exists():
            logger.error(f"❌ Base directory not found: {self.BASE_DIR}")
            return
        
        # Get all YYYY_MM folders
        folders = sorted([
            f for f in self.BASE_DIR.iterdir()
            if f.is_dir() and re.match(r'^\d{4}_\d{2}$', f.name)
        ])
        
        logger.info(f"📁 Found {len(folders)} month folders")
        
        # Repair each folder
        for folder in folders:
            self.stats["folders_scanned"] += 1
            
            # Parse expected year/month from folder name
            match = re.match(r'^(\d{4})_(\d{2})$', folder.name)
            if not match:
                continue
            
            expected_year = int(match.group(1))
            expected_month = int(match.group(2))
            
            try:
                is_valid = self.repair_folder(folder, expected_year, expected_month)
                
                # Create success marker if folder is valid and doesn't have one
                if is_valid:
                    self.create_success_marker(folder, expected_year, expected_month)
            
            except Exception as e:
                logger.error(f"❌ Error repairing {folder.name}: {e}")
                self.stats["errors"] += 1
        
        # Summary
        logger.info("=" * 70)
        logger.info("REPAIR SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Folders scanned: {self.stats['folders_scanned']}")
        logger.info(f"Files found: {self.stats['files_found']}")
        logger.info(f"Files moved: {self.stats['files_moved']}")
        logger.info(f"Success markers removed: {self.stats['success_markers_removed']}")
        logger.info(f"Success markers created: {self.stats['success_markers_created']}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info("=" * 70)
        
        if self.dry_run:
            logger.info("ℹ️  DRY RUN complete - no changes were made")
            logger.info("ℹ️  Run without --dry-run to apply changes")
        else:
            logger.info("✅ Repair complete")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="ICICI Folder Repair Utility")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without applying them")
    args = parser.parse_args()
    
    repair = ICICIFolderRepair(dry_run=args.dry_run)
    repair.run_repair()
