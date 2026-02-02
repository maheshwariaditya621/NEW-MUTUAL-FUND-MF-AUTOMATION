"""
Telegram client for sending messages.

Low-level HTTP client - no business logic.
"""

import requests
import time
from typing import Optional
from src.config import logger

# Import config with fallback
try:
    from src.config.telegram_config import (
        TELEGRAM_BOT_TOKEN,
        TELEGRAM_CHAT_ID,
        TELEGRAM_ENABLED
    )
except ImportError:
    TELEGRAM_BOT_TOKEN = None
    TELEGRAM_CHAT_ID = None
    TELEGRAM_ENABLED = False
    logger.warning("Telegram config not found - alerts disabled")

# Retry configuration (matches downloader retry logic)
MAX_RETRIES = 2  # Total 3 attempts (1 initial + 2 retries)
RETRY_BACKOFF = [5, 10]  # Retry 1: 5s, Retry 2: 10s


class TelegramClient:
    """Low-level Telegram API client."""
    
    def __init__(self):
        """Initialize Telegram client."""
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.enabled = TELEGRAM_ENABLED and self.bot_token and self.chat_id
        
        if not self.enabled:
            if not TELEGRAM_ENABLED:
                logger.debug("Telegram alerts disabled (TELEGRAM_ENABLED=False)")
            elif not self.bot_token or not self.chat_id:
                logger.debug("Telegram alerts disabled (credentials not configured)")
    
    def send_message(self, text: str, parse_mode: str = "Markdown") -> bool:
        """
        Send a message to Telegram with retry logic.
        
        Args:
            text: Message text
            parse_mode: Parse mode (Markdown or HTML)
            
        Returns:
            True if sent successfully, False otherwise
        """
        if not self.enabled:
            return False
        
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode
        }
        
        # Retry loop
        for attempt in range(MAX_RETRIES + 1):
            try:
                response = requests.post(url, json=payload, timeout=10)
                response.raise_for_status()
                logger.debug("Telegram message sent successfully")
                return True
            
            except (requests.Timeout, requests.ConnectionError) as e:
                # Retry on timeout and connection errors
                if attempt < MAX_RETRIES:
                    backoff = RETRY_BACKOFF[attempt]
                    logger.warning(
                        f"Telegram send failed (attempt {attempt + 1}/{MAX_RETRIES + 1}): {type(e).__name__}. "
                        f"Retrying in {backoff}s..."
                    )
                    time.sleep(backoff)
                else:
                    logger.error(
                        f"Failed to send Telegram message after {MAX_RETRIES + 1} attempts: {str(e)}"
                    )
                    return False
            
            except requests.HTTPError as e:
                # Don't retry on HTTP errors (4xx, 5xx from Telegram API)
                logger.error(f"Telegram API error (non-retryable): {str(e)}")
                return False
            
            except Exception as e:
                # Catch-all for unexpected errors
                logger.error(f"Unexpected error sending Telegram message: {str(e)}")
                return False
        
        return False
