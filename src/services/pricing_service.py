import threading
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
        self._login_lock = threading.Lock()
        
        # Simple memory cache: { "tradingsymbol": {"ltp": 150.5, "timestamp": 1690000000} }
        self._ltp_cache: Dict[str, Dict] = {}
        self.CACHE_TTL_SECONDS = 30  
        self.LOG_THROTTLE_SECONDS = 60 # Throttle repetitive error logs
        self._last_error_time = 0
        
    def _is_configured(self) -> bool:
        return all([self.api_key, self.client_code, self.password, self.totp_secret])

    def _login(self) -> bool:
        """
        Thread-safe login to Angel One.
        """
        if not self._is_configured():
            logger.warning("Angel One credentials incomplete. Pricing service disabled.")
            return False
            
        with self._login_lock:
            # Double-check if another thread already logged in while we were waiting for the lock
            if self.smartApi and self.feed_token:
                return True

            try:
                logger.info("Initiating Angel One login...")
                api_client = SmartConnect(api_key=self.api_key)
                
                # Generate dynamic TOTP
                totp = pyotp.TOTP(self.totp_secret).now()
                
                data = api_client.generateSession(self.client_code, self.password, totp)
                
                if data and data.get('status'):
                    self.smartApi = api_client
                    self.feed_token = api_client.getfeedToken()
                    logger.info("Successfully logged into Angel One SmartAPI")
                    return True
                else:
                    msg = data.get('message') if data else "No response data"
                    logger.error(f"Angel One login failed: {msg}")
                    return False
                    
            except Exception as e:
                if time.time() - self._last_error_time > self.LOG_THROTTLE_SECONDS:
                    logger.error(f"Error connecting to Angel One: {e}")
                    self._last_error_time = time.time()
                return False

    def get_ltp(self, exchange: str, tradingsymbol: str, symboltoken: str, sync: bool = True) -> Optional[float]:
        """
        Fetch the Last Traded Price (LTP) for a specific symbol.
        Uses a 30-second memory cache to prevent rate-limiting.
        
        If sync=False, returns None immediately if not in cache (non-blocking).
        """
        if not tradingsymbol or not symboltoken:
            return None
            
        # Check cache
        cached_entry = self._ltp_cache.get(tradingsymbol)
        if cached_entry:
            if time.time() - cached_entry['timestamp'] < self.CACHE_TTL_SECONDS:
                return cached_entry['ltp']

        # If non-blocking and not in cache, return None immediately
        if not sync:
            return None

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
                msg = response.get('message', '')
                logger.warning(f"Failed to fetch LTP for {tradingsymbol}: {msg}")
                
                # If token is invalid or session expired in response data
                if "Invalid Token" in msg or "AG8001" in str(response.get('errorCode', '')):
                    logger.info("Forcing Angel One re-login due to invalid token in response")
                    self.smartApi = None
                    self.feed_token = None
                    
                return None
                
        except Exception as e:
            logger.error(f"Error fetching LTP for {tradingsymbol}: {e}")
            
            # If session expired, reset it
            if "Session Expired" in str(e) or "Token is invalid" in str(e):
                self.smartApi = None
                
            return None

    def get_live_market_cap(self, isin: str, db_mcap: Optional[float], shares_outstanding: Optional[int], sync: bool = True) -> Optional[float]:
        """
        Calculates live Market Cap dynamically: Live LTP * Shares Outstanding.
        Falls back to the last known database Market Cap if fetching fails.
        
        If sync=False, it won't block for a network price fetch.
        """
        # We need both ISIN to fetch price and Shares to calculate
        if not isin or not shares_outstanding:
            return float(db_mcap) if db_mcap else None
            
        from src.services.symbol_mapper import symbol_mapper
        info = symbol_mapper.get_symbol_info(isin)
        if info:
            symbol, token, exchange = info
            ltp = self.get_ltp(exchange, symbol, token, sync=sync)
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
                
        # Batch requests into chunks of 20 tokens (reduced from 40 for stability)
        BATCH_SIZE = 20
        for exch, tokens in exchange_tokens.items():
            for i in range(0, len(tokens), BATCH_SIZE):
                chunk = tokens[i:i+BATCH_SIZE]
                try:
                    # Small sleep to prevent hitting rate limits during bulk
                    if i > 0:
                        time.sleep(0.1)
                        
                    res = self.smartApi.getMarketData('LTP', {exch: chunk})
                    if res and res.get('status') and res.get('data'):
                        fetched = res['data'].get('fetched', [])
                        for item in fetched:
                            sym = item.get('tradingSymbol')
                            ltp = item.get('ltp')
                            if sym and ltp is not None:
                                self._ltp_cache[sym] = {"ltp": float(ltp), "timestamp": time.time()}
                                
                    elif res and not res.get('status'):
                        msg = res.get('message', '')
                        logger.warning(f"Bulk fetch partially failed for {exch}: {msg}")
                        
                        # Handle AG8001 Invalid Token in bulk response
                        if "Invalid Token" in msg or "AG8001" in str(res.get('errorCode', '')):
                            logger.info(f"Forcing Angel One re-login due to invalid token in bulk response for {exch}")
                            self.smartApi = None
                            self.feed_token = None
                            return # Stop current bulk to prevent multiple re-login attempts in one loop
                        
                except Exception as e:
                    # Handle Connection Aborted / Reset
                    if "Connection aborted" in str(e) or "10054" in str(e):
                        logger.error(f"Network error during bulk prefetch for {exch}: {e}")
                        self.smartApi = None # Force re-login on next attempt
                        return # Stop current bulk to prevent cascade failures
                    
                    if time.time() - self._last_error_time > self.LOG_THROTTLE_SECONDS:
                        logger.error(f"Bulk prefetch failed for {exch} batch {i}: {e}")
                        self._last_error_time = time.time()

pricing_service = PricingService()
