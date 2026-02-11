"""
Base downloader abstract class.

Defines the interface for AMC-specific downloaders.
"""

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any

from src.config import logger


class BaseDownloader(ABC):
    """
    Abstract base class for AMC-specific downloaders.
    
    Each AMC will have its own downloader class that inherits from this base class
    and implements the download method.
    
    Downloaders are responsible for:
    - Downloading monthly portfolio files from AMC websites
    - Saving files to canonical raw data folder
    - Returning metadata about the download
    
    Downloaders are NOT responsible for:
    - Parsing Excel files
    - Validating data
    - Normalizing data
    - Database operations
    """
    
    def __init__(self, amc_name: str):
        """
        Initialize downloader.
        
        Args:
            amc_name: Canonical AMC name
        """
        self.amc_name = amc_name
    
    @abstractmethod
    def download(self, year: int, month: int) -> Dict[str, Any]:
        """
        Download monthly portfolio file for specified period.
        
        Args:
            year: 4-digit year (e.g., 2025)
            month: Month number (1-12)
            
        Returns:
            Dictionary with keys:
                - amc: str (AMC name)
                - year: int
                - month: int
                - file_path: str (path to downloaded file) [if success]
                - status: str ("success" or "failed")
                - reason: str (failure reason) [if failed]
        """
        pass
    
    def ensure_directory(self, path: str) -> None:
        """
        Ensure directory exists, create if missing.
        
        Args:
            path: Directory path
        """
        Path(path).mkdir(parents=True, exist_ok=True)
        logger.debug(f"Directory ensured: {path}")
    
    def log_start(self, year: int, month: int, target_folder: str) -> None:
        """
        Log download start.
        
        Args:
            year: Year
            month: Month
            target_folder: Target folder path
        """
        month_names = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]
        period_str = f"{month_names[month - 1]} {year}"
        
        logger.info("=" * 60)
        logger.info(f"{self.amc_name.upper()} DOWNLOADER STARTED")
        logger.info(f"Period: {period_str}")
        logger.info(f"Target folder: {target_folder}")
        logger.info("=" * 60)
    
    def log_success(self, file_path: str) -> None:
        """
        Log download success.
        
        Args:
            file_path: Path to downloaded file
        """
        logger.success(f"File downloaded successfully")
        logger.success(f"Saved as: {file_path}")
        logger.info("=" * 60)
        logger.success(f"✅ {self.amc_name} download completed")
        logger.info("=" * 60)
    
    def log_failure(self, reason: str) -> None:
        """
        Log download failure.
        
        Args:
            reason: Failure reason
        """
        logger.error("Download failed")
        logger.error(f"Reason: {reason}")
        logger.info("=" * 60)
        logger.error(f"❌ {self.amc_name} download failed")
        logger.info("=" * 60)
    
    def get_target_folder(self, amc_slug: str, year: int, month: int) -> str:
        """
        Get target folder path for downloaded file.
        
        Args:
            amc_slug: AMC slug (e.g., "hdfc")
            year: Year
            month: Month
            
        Returns:
            Target folder path
        """
        return os.path.join("data", "raw", amc_slug, f"{year}_{month:02d}")
    
    def get_target_file_path(self, amc_slug: str, year: int, month: int, filename: str) -> str:
        """
        Get target file path for downloaded file.
        
        Args:
            amc_slug: AMC slug (e.g., "hdfc")
            year: Year
            month: Month
            filename: Filename (e.g., "hdfc_portfolio.xlsx")
            
        Returns:
            Target file path
        """
        folder = self.get_target_folder(amc_slug, year, month)
        return os.path.join(folder, filename)

    def consolidate_downloads(self, year: int, month: int) -> None:
        """
        Consolidate all downloaded files for the AMC/period into a single file.
        This is typically called after a successful multi-file download.
        """
        from src.utils import consolidate_amc_downloads
        
        amc_slug = getattr(self, "AMC_NAME", self.amc_name.lower().replace(" ", "_"))
        logger.info(f"Consolidating downloads for {self.amc_name} ({year}-{month:02d})...")
        
        consolidated_path = consolidate_amc_downloads(amc_slug, year, month)
        if consolidated_path:
            logger.success(f"Consolidation complete: {consolidated_path}")
        else:
            logger.warning(f"Consolidation skipped or failed for {self.amc_name}")
