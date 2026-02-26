import time
from typing import Dict, List, Optional
import pyotp
from SmartApi import SmartConnect
from src.config.settings import (
    ANGEL_API_KEY, ANGEL_CLIENT_CODE, ANGEL_PASSWORD, ANGEL_TOTP_SECRET
)
from src.config import logger

class PricingService:
    def __init__(self):
        self.api_key = ANGEL_API_KEY
        self.client_code = ANGEL_CLIENT_CODE
        self.password = ANGEL_PASSWORD
        self.totp_secret = ANGEL_TOTP_SECRET
        
        self.smartApi = None
        self.feed_token = None
        
        # Simple memory cache: { "tradingsymbol": {"ltp": 150.5, "timestamp": 1690000000} }
        self._ltp_cache: Dict[str, Dict] = {}
        self.CACHE_TTL_SECONDS = 300  # 5 minutes cache to avoid rate limits
        
    def _is_configured(self) -> bool:
        return all([self.api_key, self.client_code, self.password, self.totp_secret])

    def _login(self) -> bool:
        if not self._is_configured():
            logger.warning("Angel One credentials incomplete. Pricing service disabled.")
            return False
            
        try:
            self.smartApi = SmartConnect(api_key=self.api_key)
            
            # Generate dynamic TOTP
            totp = pyotp.TOTP(self.totp_secret).now()
            
            data = self.smartApi.generateSession(self.client_code, self.password, totp)
            
            if data['status']:
                self.feed_token = self.smartApi.getfeedToken()
                logger.info("Successfully logged into Angel One SmartAPI")
                return True
            else:
                logger.error(f"Angel One login failed: {data.get('message')}")
                return False
                
        except Exception as e:
            logger.error(f"Error connecting to Angel One: {e}")
            return False

    def get_ltp(self, exchange: str, tradingsymbol: str, symboltoken: str) -> Optional[float]:
        """
        Fetch the Last Traded Price (LTP) for a specific symbol.
        Uses a 5-minute memory cache to prevent rate-limiting.
        """
        if not tradingsymbol or not symboltoken:
            return None
            
        # Check cache
        cached_entry = self._ltp_cache.get(tradingsymbol)
        if cached_entry:
            if time.time() - cached_entry['timestamp'] < self.CACHE_TTL_SECONDS:
                return cached_entry['ltp']

        # Ensure login
        if not self.smartApi or not self.feed_token:
            if not self._login():
                return None

        try:
            # SmartAPI expects exchange and tradingsymbol
            # Format: ltpData(exchange: str, tradingsymbol: str, symboltoken: str)
            response = self.smartApi.ltpData(exchange, tradingsymbol, symboltoken)
            
            if response and response.get('status') and response.get('data'):
                ltp = response['data'].get('ltp')
                if ltp is not None:
                    # Update cache
                    self._ltp_cache[tradingsymbol] = {
                        "ltp": float(ltp),
                        "timestamp": time.time()
                    }
                    return float(ltp)
            else:
                logger.warning(f"Failed to fetch LTP for {tradingsymbol}: {response.get('message')}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching LTP for {tradingsymbol}: {e}")
            
            # If session expired, reset it
            if "Session Expired" in str(e) or "Token is invalid" in str(e):
                self.smartApi = None
                
            return None

    def get_live_market_cap(self, isin: str, db_mcap: Optional[float], shares_outstanding: Optional[int]) -> Optional[float]:
        """
        Calculates live Market Cap dynamically: Live LTP * Shares Outstanding.
        Falls back to the last known database Market Cap if fetching fails.
        """
        # We need both ISIN to fetch price and Shares to calculate
        if not isin or not shares_outstanding:
            return float(db_mcap) if db_mcap else None
            
        from src.services.symbol_mapper import symbol_mapper
        info = symbol_mapper.get_symbol_info(isin)
        if info:
            symbol, token, exchange = info
            ltp = self.get_ltp(exchange, symbol, token)
            if ltp:
                # Market Cap = LTP * Shares
                return float(ltp) * float(shares_outstanding)
                
        return float(db_mcap) if db_mcap else None

    def prefetch_ltps(self, isins: List[str]):
        """
        Prefetches LTPs for a list of ISINs concurrently into the cache.
        Uses Angel One's getMarketData bulk endpoint to fetch up to 40 tokens per API call,
        resolving bottleneck delays instantly.
        """
        if not isins:
            return
            
        from src.services.symbol_mapper import symbol_mapper
        from collections import defaultdict
        
        # Group tokens by exchange
        exchange_tokens = defaultdict(list)
        symbol_map = {} # To map token string back to symbol for caching
        
        for isin in isins:
            info = symbol_mapper.get_symbol_info(isin)
            if info:
                symbol, token, exchange = info
                cached = self._ltp_cache.get(symbol)
                # Only queue if missing or expired cache
                if not cached or time.time() - cached['timestamp'] > self.CACHE_TTL_SECONDS:
                    exchange_tokens[exchange].append(token)
                    symbol_map[token] = symbol
                    
        if not exchange_tokens:
            return
            
        if not self.smartApi or not self.feed_token:
            if not self._login():
                return
                
        # Batch requests into chunks of 40 tokens per exchange to stay safe
        BATCH_SIZE = 40
        for exch, tokens in exchange_tokens.items():
            for i in range(0, len(tokens), BATCH_SIZE):
                chunk = tokens[i:i+BATCH_SIZE]
                try:
                    res = self.smartApi.getMarketData('LTP', {exch: chunk})
                    if res and res.get('status') and res.get('data'):
                        fetched = res['data'].get('fetched', [])
                        for item in fetched:
                            sym = item.get('tradingSymbol')
                            ltp = item.get('ltp')
                            if sym and ltp is not None:
                                self._ltp_cache[sym] = {"ltp": float(ltp), "timestamp": time.time()}
                except Exception as e:
                    logger.error(f"Bulk prefetch failed for {exch} batch {i}: {e}")

pricing_service = PricingService()
