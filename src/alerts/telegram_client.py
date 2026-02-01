"""
Telegram client for sending messages.

Low-level HTTP client - no business logic.
"""

import requests
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
        Send a message to Telegram.
        
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
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            logger.debug("Telegram message sent successfully")
            return True
        
        except requests.RequestException as e:
            logger.error(f"Failed to send Telegram message: {str(e)}")
            return False
