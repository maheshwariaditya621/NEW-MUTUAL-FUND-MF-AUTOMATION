"""
Centralized Logging System
===========================
Beautified, colorized logging for the entire application.

Usage:
    from logging.logger import get_logger
    
    logger = get_logger(__name__)
    logger.info("Starting process")
    logger.success("Process completed successfully")
    logger.warning("Non-critical issue detected")
    logger.error("Critical failure occurred")
"""

import logging
import sys
from datetime import datetime
from typing import Optional

# Try to import colorama for cross-platform color support
try:
    from colorama import init, Fore, Style
    init(autoreset=True)
    COLORAMA_AVAILABLE = True
except ImportError:
    COLORAMA_AVAILABLE = False
    # Fallback: no colors
    class Fore:
        BLUE = ""
        GREEN = ""
        YELLOW = ""
        RED = ""
        CYAN = ""
        RESET = ""
    
    class Style:
        BRIGHT = ""
        RESET_ALL = ""


class ColoredFormatter(logging.Formatter):
    """
    Custom formatter that adds colors and beautification to log messages.
    """
    
    # Color mapping for log levels
    COLORS = {
        'DEBUG': Fore.CYAN,
        'INFO': Fore.BLUE,
        'SUCCESS': Fore.GREEN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.RED + Style.BRIGHT,
    }
    
    # Emoji/symbols for log levels (optional, can be disabled)
    SYMBOLS = {
        'DEBUG': '🔍',
        'INFO': '🔵',
        'SUCCESS': '✅',
        'WARNING': '⚠️',
        'ERROR': '❌',
        'CRITICAL': '🚨',
    }
    
    def __init__(self, use_symbols: bool = True):
        """
        Initialize the colored formatter.
        
        Args:
            use_symbols: Whether to include emoji symbols in output
        """
        self.use_symbols = use_symbols
        super().__init__()
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format the log record with colors and structure.
        
        Format: [TIMESTAMP] [LEVEL] [MODULE] Message
        Example: [2026-02-01 10:45:12] [INFO] [ingestion.pipeline] Starting process
        """
        # Get timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Get log level color
        level_name = record.levelname
        color = self.COLORS.get(level_name, '')
        
        # Get symbol (if enabled)
        symbol = self.SYMBOLS.get(level_name, '') if self.use_symbols else ''
        
        # Format module name (shorten if too long)
        module_name = record.name
        if len(module_name) > 30:
            module_name = '...' + module_name[-27:]
        
        # Build the formatted message
        if COLORAMA_AVAILABLE and self.use_symbols:
            formatted = (
                f"[{timestamp}] "
                f"{color}[{level_name}]{Style.RESET_ALL} "
                f"{symbol} "
                f"[{module_name}] "
                f"{record.getMessage()}"
            )
        elif COLORAMA_AVAILABLE:
            formatted = (
                f"[{timestamp}] "
                f"{color}[{level_name}]{Style.RESET_ALL} "
                f"[{module_name}] "
                f"{record.getMessage()}"
            )
        else:
            # Fallback without colors
            formatted = (
                f"[{timestamp}] "
                f"[{level_name}] "
                f"[{module_name}] "
                f"{record.getMessage()}"
            )
        
        # Add exception info if present
        if record.exc_info:
            formatted += "\n" + self.formatException(record.exc_info)
        
        return formatted


# Add custom SUCCESS log level
SUCCESS_LEVEL = 25  # Between INFO (20) and WARNING (30)
logging.addLevelName(SUCCESS_LEVEL, 'SUCCESS')


def success(self, message: str, *args, **kwargs):
    """
    Log a success message.
    
    Args:
        message: The success message to log
    """
    if self.isEnabledFor(SUCCESS_LEVEL):
        self._log(SUCCESS_LEVEL, message, args, **kwargs)


# Add success method to Logger class
logging.Logger.success = success


def get_logger(
    name: str,
    level: int = logging.INFO,
    use_symbols: bool = True,
    log_to_file: Optional[str] = None
) -> logging.Logger:
    """
    Get a configured logger instance.
    
    Args:
        name: Name of the logger (usually __name__)
        level: Logging level (default: INFO)
        use_symbols: Whether to include emoji symbols (default: True)
        log_to_file: Optional file path to also log to a file
    
    Returns:
        Configured logger instance
    
    Example:
        logger = get_logger(__name__)
        logger.info("This is an info message")
        logger.success("This is a success message")
        logger.warning("This is a warning")
        logger.error("This is an error")
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Remove existing handlers to avoid duplicates
    logger.handlers = []
    
    # Console handler (always enabled)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(ColoredFormatter(use_symbols=use_symbols))
    logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_to_file:
        file_handler = logging.FileHandler(log_to_file, encoding='utf-8')
        file_handler.setLevel(level)
        # File logs don't need colors/symbols
        file_formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger


# Example usage and testing
if __name__ == "__main__":
    # Create a test logger
    test_logger = get_logger("test.module", use_symbols=True)
    
    print("\n" + "="*80)
    print("LOGGING SYSTEM DEMO - Example Terminal Output")
    print("="*80 + "\n")
    
    test_logger.debug("This is a debug message (usually hidden)")
    test_logger.info("Starting monthly portfolio ingestion for HDFC MF")
    test_logger.success("Successfully extracted 1,245 rows from Excel")
    test_logger.warning("3 rows have missing ISIN codes, flagged for review")
    test_logger.error("Database connection failed: timeout exceeded")
    
    print("\n" + "="*80)
    print("END OF DEMO")
    print("="*80 + "\n")
