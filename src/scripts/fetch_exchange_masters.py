
import requests
import os
from pathlib import Path
from src.config import logger

def fetch_nse_master():
    """Download the official NSE Equity Master (EQUITY_L.csv)."""
    url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    output_dir = Path("data/raw/exchange_masters")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "nse_equity_l.csv"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    }
    
    try:
        logger.info(f"Downloading NSE Master from {url}")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        with open(output_path, "wb") as f:
            f.write(response.content)
        
        logger.info(f"Successfully saved NSE Master to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Failed to fetch NSE Master: {e}")
        return None

def fetch_bse_master():
    """
    Download the BSE Scrip Master from a reliable secondary source (IIFL).
    The official BSE link often 404s or blocks direct scripts.
    """
    url = "http://content.indiainfoline.com/IIFLTT/Scripmaster.csv"
    output_dir = Path("data/raw/exchange_masters")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "bse_scrip_master.csv"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    }
    
    try:
        logger.info(f"Downloading BSE Master (via IIFL) from {url}")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        with open(output_path, "wb") as f:
            f.write(response.content)
            
        logger.info(f"Successfully saved BSE Master to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Failed to fetch BSE Master: {e}")
        return None

if __name__ == "__main__":
    nse_file = fetch_nse_master()
    bse_file = fetch_bse_master()
    
    if nse_file and bse_file:
        print("\nAll masters fetched successfully.")
    else:
        print("\nOne or more masters failed to fetch.")
