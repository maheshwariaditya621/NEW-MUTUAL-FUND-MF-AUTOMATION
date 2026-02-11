"""
Telegram notifier - decides when to send alerts.

Event-based architecture - no tight coupling to downloader.
"""

from typing import Dict, Any, Optional
from src.alerts.telegram_client import TelegramClient
from src.alerts.telegram_templates import TelegramTemplates
from src.config import logger

# Import config with fallback
try:
    from src.config.telegram_config import ALERTS_ENABLED
except ImportError:
    ALERTS_ENABLED = {
        "SUCCESS": True,
        "WARNING": True,
        "ERROR": True,
        "NOT_PUBLISHED": True,
        "SCHEDULER": True,
    }


class TelegramNotifier:
    """
    Event-based Telegram notifier.
    
    Decides when to send alerts based on configuration.
    """
    
    def __init__(self):
        """Initialize notifier."""
        self.client = TelegramClient()
        self.templates = TelegramTemplates()
        self.alerts_enabled = ALERTS_ENABLED
    
    def _should_send(self, alert_type: str) -> bool:
        """
        Check if alert type should be sent.
        
        Args:
            alert_type: Alert type (SUCCESS, WARNING, ERROR, etc.)
            
        Returns:
            True if should send, False otherwise
        """
        if not self.client.enabled:
            return False
        
        return self.alerts_enabled.get(alert_type, False)
    
    def notify_success(self, amc: str, year: int, month: int, 
                      files_downloaded: int, duration: float = 0) -> bool:
        """
        Send SUCCESS alert.
        
        Args:
            amc: AMC name
            year: Year
            month: Month
            files_downloaded: Number of files downloaded
            duration: Duration in seconds
            
        Returns:
            True if sent, False otherwise
        """
        if not self._should_send("SUCCESS"):
            return False
        
        data = {
            "amc": amc,
            "year": year,
            "month": month,
            "files_downloaded": files_downloaded,
            "duration": duration
        }
        
        message = self.templates.success(data)
        return self.client.send_message(message)
    
    def notify_warning(self, amc: str, year: int, month: int,
                      warning_type: str, message: str) -> bool:
        """
        Send WARNING alert.
        
        Args:
            amc: AMC name
            year: Year
            month: Month
            warning_type: Type of warning
            message: Warning message
            
        Returns:
            True if sent, False otherwise
        """
        if not self._should_send("WARNING"):
            return False
        
        data = {
            "amc": amc,
            "year": year,
            "month": month,
            "warning_type": warning_type,
            "message": message
        }
        
        msg = self.templates.warning(data)
        return self.client.send_message(msg)
    
    def notify_error(self, amc: str, year: int, month: int,
                    error_type: str, reason: str) -> bool:
        """
        Send ERROR alert.
        
        Args:
            amc: AMC name
            year: Year
            month: Month
            error_type: Type of error
            reason: Error reason
            
        Returns:
            True if sent, False otherwise
        """
        if not self._should_send("ERROR"):
            return False
        
        data = {
            "amc": amc,
            "year": year,
            "month": month,
            "error_type": error_type,
            "reason": reason
        }
        
        message = self.templates.error(data)
        return self.client.send_message(message)
    
    def notify_not_published(self, amc: str, year: int, month: int) -> bool:
        """
        Send NOT_PUBLISHED alert.
        
        Args:
            amc: AMC name
            year: Year
            month: Month
            
        Returns:
            True if sent, False otherwise
        """
        if not self._should_send("NOT_PUBLISHED"):
            return False
        
        data = {
            "amc": amc,
            "year": year,
            "month": month
        }
        
        message = self.templates.not_published(data)
        return self.client.send_message(message)
    
    def notify_scheduler(self, event: str, message: str, 
                        stats: Optional[Dict[str, int]] = None) -> bool:
        """
        Send SCHEDULER alert.
        
        Args:
            event: Event name
            message: Event message
            stats: Optional statistics dict
            
        Returns:
            True if sent, False otherwise
        """
        if not self._should_send("SCHEDULER"):
            return False
        
        data = {
            "event": event,
            "message": message,
            "stats": stats or {}
        }
        
        msg = self.templates.scheduler(data)
        return self.client.send_message(msg)

    def notify_merge_success(self, amc: str, year: int, month: int, 
                            output_file: str) -> bool:
        """
        Send EXCEL_MERGE_SUCCESS alert.
        
        Args:
            amc: AMC name
            year: Year
            month: Month
            output_file: Path to merged file
            
        Returns:
            True if sent, False otherwise
        """
        if not self._should_send("SUCCESS"):
            return False
        
        data = {
            "amc": amc,
            "year": year,
            "month": month,
            "output_file": output_file
        }
        
        message = self.templates.merge_success(data)
        return self.client.send_message(message)

    def notify_merge_error(self, amc: str, year: int, month: int,
                          error: str) -> bool:
        """
        Send EXCEL_MERGE_ERROR alert.
        
        Args:
            amc: AMC name
            year: Year
            month: Month
            error: Error message
            
        Returns:
            True if sent, False otherwise
        """
        if not self._should_send("ERROR"):
            return False
        
        data = {
            "amc": amc,
            "year": year,
            "month": month,
            "error": error
        }
        
        message = self.templates.merge_error(data)
        return self.client.send_message(message)


# Global singleton instance
_notifier = None


def get_notifier() -> TelegramNotifier:
    """
    Get global notifier instance.
    
    Returns:
        TelegramNotifier instance
    """
    global _notifier
    if _notifier is None:
        _notifier = TelegramNotifier()
    return _notifier
