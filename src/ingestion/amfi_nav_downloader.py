import requests
from pathlib import Path
from src.config import logger
from datetime import datetime

class AMFINavDownloader:
    """
    Downloads NAV data from AMFI (Association of Mutual Funds in India).
    Default URL: https://www.amfiindia.com/spages/NAVAll.txt
    """
    
    URL = "https://www.amfiindia.com/spages/NAVAll.txt"

    def __init__(self, download_dir: str = "data/raw/amfi"):
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def download_latest_nav_file(self) -> Path:
        """
        Downloads the latest NAV file from AMFI.
        Returns the path to the downloaded file.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        target_path = self.download_dir / f"amfi_nav_{timestamp}.txt"
        
        logger.info(f"Downloading latest AMFI NAV data from {self.URL}...")
        
        try:
            response = requests.get(self.URL, timeout=30)
            response.raise_for_status()
            
            with open(target_path, "w", encoding="utf-8") as f:
                f.write(response.text)
                
            logger.info(f"Successfully downloaded AMFI NAV data to {target_path}")
            return target_path
            
        except Exception as e:
            logger.error(f"Failed to download AMFI NAV data: {e}")
            raise

if __name__ == "__main__":
    downloader = AMFINavDownloader()
    downloader.download_latest_nav_file()
