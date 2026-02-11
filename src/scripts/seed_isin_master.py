import csv
import argparse
from pathlib import Path
from src.config import logger
from src.db.connection import TransactionContext
from src.db.repositories import upsert_isin_master

def seed_from_csv(file_path: str):
    """
    Seeds the isin_master table from a CSV file.
    Expected columns: isin, canonical_name, nse_symbol, bse_code, sector, industry
    """
    path = Path(file_path)
    if not path.exists():
        logger.error(f"File not found: {file_path}")
        return

    logger.info(f"Seeding isin_master from {file_path}")
    
    count = 0
    with TransactionContext():
        with open(path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                upsert_isin_master(
                    isin=row['isin'],
                    canonical_name=row['canonical_name'],
                    nse_symbol=row.get('nse_symbol'),
                    bse_code=row.get('bse_code'),
                    sector=row.get('sector'),
                    industry=row.get('industry')
                )
                count += 1
                if count % 100 == 0:
                    logger.info(f"Processed {count} ISINs...")

    logger.info(f"Successfully seeded {count} ISINs into isin_master.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed ISIN Master from CSV")
    parser.add_argument("--file", required=True, help="Path to the master CSV file")
    args = parser.parse_args()
    seed_from_csv(args.file)
