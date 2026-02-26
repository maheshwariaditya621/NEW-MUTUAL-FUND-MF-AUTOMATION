import json
import os
import requests
from typing import Dict, Optional, Tuple
from src.config import logger
from src.db.connection import get_connection

class SymbolMapper:
    """
    Combines Database ISINs with Angel One Instrument List.
    Maps an ISIN -> exchange_symbol (DB) -> tradingsymbol (Angel).
    Returns (tradingsymbol, symboltoken, exchange).
    """
    def __init__(self):
        self.INSTRUMENT_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        self.LOCAL_FILE = os.path.join("data", "angel_instruments.json")
        self._isin_map: Dict[str, dict] = {}
        
    def _download_instruments(self) -> bool:
        """Downloads the instrument list if missing or old."""
        try:
            os.makedirs(os.path.dirname(self.LOCAL_FILE), exist_ok=True)
            logger.info("Downloading latest Angel One instrument list...")
            
            response = requests.get(self.INSTRUMENT_URL, timeout=30)
            response.raise_for_status()
            
            with open(self.LOCAL_FILE, "wb") as f:
                f.write(response.content)
            
            logger.info("Successfully downloaded instrument list.")
            return True
        except Exception as e:
            logger.error(f"Failed to download Angel One instruments: {e}")
            return False

    def _fetch_db_mappings(self) -> Dict[str, str]:
        """Returns Dict[isin, exchange_symbol] from the database."""
        db_map = {}
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("SELECT isin, exchange_symbol FROM companies WHERE exchange_symbol IS NOT NULL")
            rows = cur.fetchall()
            for isin, symbol in rows:
                db_map[isin] = symbol
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"Error fetching DB ISIN mappings: {e}")
            
        return db_map

    def _load_data(self):
        """Builds the final ISIN -> Angel Info map."""
        if not os.path.exists(self.LOCAL_FILE):
            if not self._download_instruments():
                return
                
        try:
            # 1. Load Angel One JSON and build `name` -> `token_info`
            with open(self.LOCAL_FILE, 'r', encoding='utf-8') as f:
                angel_data = json.load(f)
                
            angel_map = {}
            for item in angel_data:
                name = item.get("name")
                symbol = item.get("symbol")
                token = item.get("token")
                exch_seg = item.get("exch_seg")
                
                if exch_seg in ["NSE", "BSE"]:
                    info = {
                        "symbol": symbol,
                        "token": token,
                        "exchange": exch_seg
                    }
                    
                    # Prefer NSE for name indexing
                    if name:
                        if name not in angel_map or exch_seg == "NSE":
                            angel_map[name] = info
                            
                    # Index by token (Crucial for BSE codes like '500285' from DB)
                    if token:
                        if token not in angel_map:
                            angel_map[token] = info
                            
                    # Index by exact symbol
                    if symbol:
                        if symbol not in angel_map:
                            angel_map[symbol] = info
            
            # 2. Match Database ISINs to Angel One symbols
            db_isin_map = self._fetch_db_mappings()
            
            self._isin_map.clear()
            for isin, exchange_symbol in db_isin_map.items():
                angel_info = angel_map.get(exchange_symbol)
                if angel_info:
                    self._isin_map[isin] = angel_info
            
            logger.info(f"Successfully mapped {len(self._isin_map)} ISINs to Angel One tokens via DB exchange_symbols.")
            
        except Exception as e:
            logger.error(f"Error loading and mapping instrument data: {e}")

    def get_symbol_info(self, isin: str) -> Optional[Tuple[str, str, str]]:
        """
        Returns (tradingsymbol, symboltoken, exchange) for a given ISIN.
        """
        if not self._isin_map:
            self._load_data()
            
        if not self._isin_map:
            return None
            
        data = self._isin_map.get(isin)
        if data:
            return data["symbol"], data["token"], data["exchange"]
            
        return None

symbol_mapper = SymbolMapper()
