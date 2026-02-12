from src.ingestion.amfi_nav_downloader import AMFINavDownloader
from src.ingestion.amfi_nav_parser import AMFINavParser
from src.db import upsert_nav_entries, close_connection
from src.config import logger

def main():
    """
    Orchestrates the AMFI NAV ingestion pipeline.
    """
    try:
        # 1. Download
        downloader = AMFINavDownloader()
        file_path = downloader.download_latest_nav_file()
        
        # 2. Parse
        parser = AMFINavParser()
        nav_data = parser.parse_file(file_path)
        
        if not nav_data:
            logger.warning("No NAV data parsed. Exiting.")
            return

        # 3. Ingest
        logger.info(f"Starting database ingestion of {len(nav_data)} NAV entries...")
        upsert_nav_entries(nav_data)
        logger.info("AMFI NAV ingestion completed successfully.")
        
    except Exception as e:
        logger.error(f"AMFI NAV ingestion failed: {e}")
    finally:
        close_connection()

if __name__ == "__main__":
    main()
