import csv
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
from src.config import logger

class AMFINavParser:
    """
    Parses AMFI NAVAll.txt files into structured lists of dictionaries.
    """

    @staticmethod
    def parse_file(file_path: Path) -> List[Dict[str, Any]]:
        """
        Parses the semicolon-delimited AMFI file.
        Skips headers and blank lines.
        """
        results = []
        logger.info(f"Parsing AMFI NAV file: {file_path}")
        
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                # Use csv reader with semicolon delimiter
                reader = csv.reader(f, delimiter=';')
                
                header_skipped = False
                col_map = {}
                
                for row in reader:
                    if not row:
                        continue
                    
                    # 1. Detect Header
                    if not header_skipped and "Scheme Code" in row[0]:
                        header_skipped = True
                        # Map columns based on headers
                        for i, col in enumerate(row):
                            c = col.strip().lower()
                            if "scheme code" in c: col_map['scheme_code'] = i
                            if "scheme name" in c: col_map['scheme_name'] = i
                            if "isin growth" in c: col_map['isin_growth'] = i
                            if "isin div reinvestment" in c: col_map['isin_div_reinv'] = i
                            if "net asset value" in c: col_map['nav'] = i
                            if "date" in c: col_map['date'] = i
                        continue
                    
                    if not header_skipped:
                        continue
                        
                    # 2. Extract Data
                    scheme_code = row[col_map['scheme_code']].strip()
                    if not scheme_code.isdigit():
                        continue
                    
                    try:
                        raw_date = row[col_map['date']].strip()
                        nav_date = datetime.strptime(raw_date, "%d-%b-%Y").date()
                        
                        raw_nav = row[col_map['nav']].strip()
                        nav_value = float(raw_nav) if raw_nav.replace('.', '', 1).isdigit() else 0.0
                        
                        scheme_name = row[col_map['scheme_name']].strip()
                        
                        # Extract Attributes
                        plan_type = "Regular"
                        if "DIRECT" in scheme_name.upper():
                            plan_type = "Direct"
                            
                        option_type = "Growth"
                        if "IDCW" in scheme_name.upper() or "DIVIDEND" in scheme_name.upper():
                            option_type = "IDCW"
                            
                        is_reinvest = False
                        if "REINVEST" in scheme_name.upper():
                            is_reinvest = True
                        
                        results.append({
                            "scheme_code": scheme_code,
                            "isin_growth": row[col_map['isin_growth']].strip() if row[col_map['isin_growth']].strip() != '-' else None,
                            "isin_div_reinv": row[col_map['isin_div_reinv']].strip() if row[col_map.get('isin_div_reinv', -1)].strip() != '-' else None,
                            "scheme_name": scheme_name,
                            "plan_type": plan_type,
                            "option_type": option_type,
                            "is_reinvest": is_reinvest,
                            "nav_value": nav_value,
                            "nav_date": nav_date
                        })
                    except Exception as e:
                        # Log but continue for other rows
                        logger.debug(f"Skipping malformed row {row}: {e}")
                        continue
                        
            logger.info(f"Successfully parsed {len(results)} NAV entries from {file_path}")
            return results
            
        except Exception as e:
            logger.error(f"Failed to parse AMFI NAV file: {e}")
            raise

if __name__ == "__main__":
    # Test with a local file if exists
    test_file = list(Path("data/raw/amfi").glob("amfi_nav_*.txt"))
    if test_file:
        data = AMFINavParser.parse_file(test_file[-1])
        print(f"Sample: {data[0] if data else 'No data'}")
