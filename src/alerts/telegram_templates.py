"""
Telegram message templates.

Formats alert messages - no business logic.
"""

from typing import Dict, Any


class TelegramTemplates:
    """Message templates for Telegram alerts."""
    
    @staticmethod
    def success(data: Dict[str, Any]) -> str:
        """
        Format SUCCESS alert.
        
        Args:
            data: {amc, year, month, files_downloaded, duration}
            
        Returns:
            Formatted message
        """
        amc = data.get("amc", "Unknown")
        year = data.get("year")
        month = data.get("month")
        files = data.get("files_downloaded", 0)
        duration = data.get("duration", 0)
        
        return f"""✅ *{amc} Download Complete*

📅 Period: {year}-{month:02d}
📁 Files: {files}
⏱ Duration: {duration:.1f}s

Status: SUCCESS"""
    
    @staticmethod
    def warning(data: Dict[str, Any]) -> str:
        """
        Format WARNING alert.
        
        Args:
            data: {amc, year, month, warning_type, message}
            
        Returns:
            Formatted message
        """
        amc = data.get("amc", "Unknown")
        year = data.get("year")
        month = data.get("month")
        warning_type = data.get("warning_type", "Warning")
        message = data.get("message", "")
        
        return f"""⚠️ *{amc} Warning*

📅 Period: {year}-{month:02d}
🔔 Type: {warning_type}

{message}

Status: WARNING"""
    
    @staticmethod
    def error(data: Dict[str, Any]) -> str:
        """
        Format ERROR alert.
        
        Args:
            data: {amc, year, month, error_type, reason}
            
        Returns:
            Formatted message
        """
        amc = data.get("amc", "Unknown")
        year = data.get("year")
        month = data.get("month")
        error_type = data.get("error_type", "Error")
        reason = data.get("reason", "Unknown error")
        
        return f"""❌ *{amc} Download Failed*

📅 Period: {year}-{month:02d}
🚨 Type: {error_type}

Reason: {reason}

Status: FAILED"""
    
    @staticmethod
    def not_published(data: Dict[str, Any]) -> str:
        """
        Format NOT_PUBLISHED alert.
        
        Args:
            data: {amc, year, month}
            
        Returns:
            Formatted message
        """
        amc = data.get("amc", "Unknown")
        year = data.get("year")
        month = data.get("month")
        
        return f"""📭 *{amc} Data Not Available*

📅 Period: {year}-{month:02d}

The AMC has not yet published data for this month.
Will retry on next scheduled run.

Status: NOT PUBLISHED"""
    
    @staticmethod
    def scheduler(data: Dict[str, Any]) -> str:
        """
        Format SCHEDULER alert.
        
        Args:
            data: {event, message, stats}
            
        Returns:
            Formatted message
        """
        event = data.get("event", "Scheduler Event")
        message = data.get("message", "")
        stats = data.get("stats", {})
        
        msg = f"""🤖 *Scheduler: {event}*

{message}"""
        
        if stats:
            msg += "\n\n📊 Summary:"
            if "downloaded" in stats:
                msg += f"\n✅ Downloaded: {stats['downloaded']}"
            if "skipped" in stats:
                msg += f"\n⏭ Skipped: {stats['skipped']}"
            if "failed" in stats:
                msg += f"\n❌ Failed: {stats['failed']}"
        
        return msg
