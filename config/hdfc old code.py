"""
HDFC Mutual Fund downloader.

Downloads monthly portfolio Excel files from HDFC Mutual Fund using official API.
"""

import os
import argparse
import requests
from typing import Dict, Any, List

from src.downloaders.base_downloader import BaseDownloader
from src.config import logger


class HDFCDownloader(BaseDownloader):
    """
    HDFC Mutual Fund downloader using official API.
    
    Uses HDFC's official disclosure API to download portfolio files.
    No browser automation required.
    """
    
    # HDFC API endpoint
    API_URL = "https://cms.hdfcfund.com/en/hdfc/api/v2/disclosures/monthforportfolio"
    
    # Month mapping
    MONTH_NAMES = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }
    
    def __init__(self):
        """Initialize HDFC downloader."""
        super().__init__("HDFC Mutual Fund")
    
    def download(self, year: int, month: int) -> Dict[str, Any]:
        """
        Download HDFC monthly portfolio files using API.
        
        Args:
            year: Calendar year (e.g., 2025)
            month: Month number (1-12)
            
        Returns:
            Download metadata dictionary
        """
        # Get target folder
        target_folder = self.get_target_folder("hdfc", year, month)
        
        # Log start
        self.log_start(year, month, target_folder)
        
        # Ensure directory exists
        self.ensure_directory(target_folder)
        
        try:
            # Convert to financial year
            financial_year = self._get_financial_year(year, month)
            logger.info(f"Financial year: {financial_year}-{financial_year + 1}")
            
            # Call API
            logger.info("Calling HDFC API")
            files = self._call_api(financial_year, month)
            
            if not files:
                raise Exception("No files found in API response")
            
            logger.info(f"Found {len(files)} file(s)")
            
            # Download each file
            downloaded_files = []
            for i, file_info in enumerate(files, 1):
                logger.info(f"Downloading file {i}/{len(files)}: {file_info['filename']}")
                
                file_path = self._download_file(
                    file_info['url'],
                    file_info['filename'],
                    target_folder
                )
                
                downloaded_files.append(file_path)
                logger.success(f"Saved: {file_path}")
            
            # Log success
            logger.info("=" * 60)
            logger.success(f"✅ {self.amc_name} download completed")
            logger.success(f"Downloaded {len(downloaded_files)} file(s)")
            logger.info("=" * 60)
            
            return {
                "amc": self.amc_name,
                "year": year,
                "month": month,
                "files_downloaded": len(downloaded_files),
                "files": downloaded_files,
                "status": "success"
            }
            
        except Exception as e:
            # Log failure
            reason = str(e)
            self.log_failure(reason)
            
            return {
                "amc": self.amc_name,
                "year": year,
                "month": month,
                "status": "failed",
                "reason": reason
            }
    
    def _get_financial_year(self, year: int, month: int) -> int:
        """
        Convert calendar year/month to financial year.
        
        Financial year in India: April to March
        - Jan-Mar → financial year = year - 1
        - Apr-Dec → financial year = year
        
        Args:
            year: Calendar year
            month: Month (1-12)
            
        Returns:
            Financial year start year
            
        Examples:
            >>> _get_financial_year(2025, 1)  # Jan 2025
            2024  # FY 2024-25
            >>> _get_financial_year(2025, 4)  # Apr 2025
            2025  # FY 2025-26
        """
        if month <= 3:  # Jan, Feb, Mar
            return year - 1
        else:  # Apr to Dec
            return year
    
    def _call_api(self, financial_year: int, month: int) -> List[Dict[str, str]]:
        """
        Call HDFC API to get portfolio files.
        
        Args:
            financial_year: Financial year start year
            month: Month (1-12)
            
        Returns:
            List of file info dictionaries with 'filename' and 'url'
            
        Raises:
            Exception: If API call fails or returns invalid data
        """
        # Prepare request with comprehensive headers
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Origin": "https://www.hdfcfund.com",
            "Referer": "https://www.hdfcfund.com/",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site"
        }
        
        payload = {
            "year": financial_year,
            "month": month,
            "type": "monthly"
        }
        
        # Make API call
        logger.debug(f"API payload: year={financial_year}, month={month}, type=monthly")
        
        try:
            response = requests.post(
                self.API_URL,
                headers=headers,
                data=payload,
                timeout=30
            )
            
            # Check HTTP status
            if response.status_code == 403:
                raise Exception(
                    "API access forbidden (403). "
                    "This may be due to rate limiting, IP blocking, or API changes. "
                    "Please verify the API endpoint is still active."
                )
            elif response.status_code == 404:
                raise Exception(
                    f"No data found for {self.MONTH_NAMES[month]} {financial_year} (FY {financial_year}-{financial_year+1}). "
                    "Data may not be available yet for this period."
                )
            elif response.status_code != 200:
                raise Exception(f"API returned status {response.status_code}: {response.text[:200]}")
            
            # Parse JSON
            try:
                data = response.json()
            except ValueError:
                raise Exception(f"API returned invalid JSON: {response.text[:200]}")
            
            logger.debug(f"API response received")
            
            # Validate response structure
            if "data" not in data:
                raise Exception("Invalid API response: missing 'data' field")
            
            if "files" not in data["data"]:
                raise Exception("Invalid API response: missing 'data.files' field")
            
            files_data = data["data"]["files"]
            
            if not isinstance(files_data, list):
                raise Exception("Invalid API response: 'data.files' is not a list")
            
            if len(files_data) == 0:
                raise Exception(
                    f"No files available for {self.MONTH_NAMES[month]} {financial_year}. "
                    "Data may not have been published yet."
                )
            
            # Extract file information
            files = []
            for file_item in files_data:
                if "file" not in file_item:
                    logger.warning(f"Skipping item without 'file' field: {file_item.get('title', 'Unknown')}")
                    continue
                
                file_obj = file_item["file"]
                
                if "url" not in file_obj or "filename" not in file_obj:
                    logger.warning(f"Skipping file without url/filename")
                    continue
                
                files.append({
                    "filename": file_obj["filename"],
                    "url": file_obj["url"],
                    "title": file_item.get("title", "")
                })
            
            if len(files) == 0:
                raise Exception("No valid files found in API response")
            
            return files
            
        except requests.RequestException as e:
            raise Exception(f"API request failed: {str(e)}")

    
    def _download_file(self, url: str, filename: str, target_folder: str) -> str:
        """
        Download file from URL.
        
        Args:
            url: File URL
            filename: Original filename
            target_folder: Target folder path
            
        Returns:
            Path to downloaded file
            
        Raises:
            Exception: If download fails
        """
        # Construct full path
        file_path = os.path.join(target_folder, filename)
        
        try:
            # Download file
            response = requests.get(url, timeout=60, stream=True)
            
            if response.status_code != 200:
                raise Exception(f"Download failed with status {response.status_code}")
            
            # Save file
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # Validate file
            if not os.path.exists(file_path):
                raise Exception("File not saved")
            
            file_size = os.path.getsize(file_path)
            if file_size < 1024:  # Less than 1KB
                raise Exception(f"Downloaded file is too small ({file_size} bytes)")
            
            logger.debug(f"File size: {file_size:,} bytes")
            
            return file_path
            
        except requests.RequestException as e:
            raise Exception(f"Download failed: {str(e)}")
        except IOError as e:
            raise Exception(f"Failed to save file: {str(e)}")


def main():
    """CLI entrypoint for HDFC downloader."""
    parser = argparse.ArgumentParser(
        description="HDFC Mutual Fund Portfolio Downloader (API-based)"
    )
    
    parser.add_argument("--year", required=True, type=int, help="Calendar year (YYYY)")
    parser.add_argument("--month", required=True, type=int, help="Month (1-12)")
    
    args = parser.parse_args()
    
    # Validate month
    if args.month < 1 or args.month > 12:
        logger.error("Invalid month. Must be between 1 and 12.")
        return
    
    # Create downloader and download
    downloader = HDFCDownloader()
    result = downloader.download(args.year, args.month)
    
    # Print result
    if result['status'] == 'success':
        logger.info(f"✅ Success: Downloaded {result['files_downloaded']} file(s)")
    else:
        logger.error(f"❌ Failed: {result['reason']}")


if __name__ == "__main__":
    main()
