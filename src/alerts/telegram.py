"""
Telegram alert system.

Sends alerts for validation failures, data integrity warnings, and errors.
"""

import requests
from typing import Optional

from src.config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, ENVIRONMENT, logger


class TelegramAlerter:
    """
    Telegram alert sender.
    
    Sends formatted alerts to Telegram for monitoring and debugging.
    """
    
    def __init__(self):
        """Initialize Telegram alerter."""
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.enabled = bool(self.bot_token and self.chat_id)
        
        if not self.enabled:
            logger.debug("Telegram alerts disabled (credentials not configured)")
    
    def _send_message(self, message: str) -> bool:
        """
        Send message to Telegram.
        
        Args:
            message: Message text
            
        Returns:
            True if sent successfully, False otherwise
        """
        if not self.enabled:
            logger.warning("Telegram not configured. Alert skipped.")
            logger.debug(f"Alert message: {message}")
            return False
        
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "HTML"
            }
            
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.debug("Telegram alert sent successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send Telegram alert: {e}")
            return False
    
    def send_skip_alert(
        self,
        amc: str,
        scheme: str,
        period: str,
        reason: str,
        file_name: Optional[str] = None
    ) -> None:
        """
        Send alert when a scheme-month is skipped.
        
        Args:
            amc: AMC name
            scheme: Scheme name
            period: Period string (e.g., "January 2025")
            reason: Reason for skipping
            file_name: Optional source file name
        """
        message = f"""
🚨 <b>ERROR: Scheme-Month SKIPPED</b>

<b>AMC:</b> {amc}
<b>Scheme:</b> {scheme}
<b>Period:</b> {period}
<b>Reason:</b> {reason}
"""
        
        if file_name:
            message += f"<b>File:</b> {file_name}\n"
        
        message += "\n<b>Action:</b> Manual review required"
        
        self._send_message(message)
        logger.info(f"Skip alert sent for {amc} - {scheme} ({period})")
    
    def send_validation_error(
        self,
        amc: str,
        scheme: str,
        period: str,
        error_details: str,
        file_name: Optional[str] = None
    ) -> None:
        """
        Send alert for validation error.
        
        Args:
            amc: AMC name
            scheme: Scheme name
            period: Period string
            error_details: Error details
            file_name: Optional source file name
        """
        message = f"""
❌ <b>VALIDATION ERROR</b>

<b>AMC:</b> {amc}
<b>Scheme:</b> {scheme}
<b>Period:</b> {period}
<b>Error:</b> {error_details}
"""
        
        if file_name:
            message += f"<b>File:</b> {file_name}\n"
        
        message += "\n<b>Action:</b> Fix data and retry"
        
        self._send_message(message)
        logger.info(f"Validation error alert sent for {amc} - {scheme} ({period})")
    
    def send_integrity_warning(
        self,
        amc: str,
        scheme: str,
        period: str,
        warning_message: str,
        file_name: Optional[str] = None
    ) -> None:
        """
        Send alert for data integrity warning.
        
        Args:
            amc: AMC name
            scheme: Scheme name
            period: Period string
            warning_message: Warning message
            file_name: Optional source file name
        """
        message = f"""
⚠️ <b>DATA INTEGRITY WARNING</b>

<b>AMC:</b> {amc}
<b>Scheme:</b> {scheme}
<b>Period:</b> {period}
<b>Warning:</b> {warning_message}
"""
        
        if file_name:
            message += f"<b>File:</b> {file_name}\n"
        
        message += "\n<b>Action:</b> Review recommended"
        
        self._send_message(message)
        logger.info(f"Integrity warning alert sent for {amc} - {scheme} ({period})")
    
    def send_rollback_alert(
        self,
        amc: str,
        scheme: str,
        period: str,
        error_message: str
    ) -> None:
        """
        Send alert when transaction is rolled back.
        
        Args:
            amc: AMC name
            scheme: Scheme name
            period: Period string
            error_message: Error that caused rollback
        """
        message = f"""
🔄 <b>TRANSACTION ROLLED BACK</b>

<b>AMC:</b> {amc}
<b>Scheme:</b> {scheme}
<b>Period:</b> {period}
<b>Error:</b> {error_message}

<b>Status:</b> No data was saved (transaction rolled back safely)
<b>Action:</b> Investigate error and retry
"""
        
        self._send_message(message)
        logger.info(f"Rollback alert sent for {amc} - {scheme} ({period})")
    
    def send_success_notification(
        self,
        amc: str,
        scheme: str,
        period: str,
        snapshot_id: int,
        holdings_count: int
    ) -> None:
        """
        Send success notification (optional, for monitoring).
        
        Args:
            amc: AMC name
            scheme: Scheme name
            period: Period string
            snapshot_id: Created snapshot ID
            holdings_count: Number of holdings loaded
        """
        if ENVIRONMENT != "prod":
            return  # Only send success notifications in production
        
        message = f"""
✅ <b>SUCCESS: Data Loaded</b>

<b>AMC:</b> {amc}
<b>Scheme:</b> {scheme}
<b>Period:</b> {period}
<b>Snapshot ID:</b> {snapshot_id}
<b>Holdings:</b> {holdings_count}
"""
        
        self._send_message(message)


# Global alerter instance
alerter = TelegramAlerter()
