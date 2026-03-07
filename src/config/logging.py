"""
Centralized logging configuration.

Provides color-coded, human-readable logging for the entire application.
"""

import logging
import sys
from datetime import datetime
from colorama import init, Fore, Style

# Initialize colorama for Windows color support
init(autoreset=True)

# Fix for Windows UnicodeEncodeError in console
if sys.platform == "win32":
    try:
        # Reconfigure stdout/stderr to use UTF-8
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(encoding='utf-8')
        if hasattr(sys.stderr, 'reconfigure'):
            sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        # Fallback if reconfiguration fails
        pass

# Custom log levels
SUCCESS = 25  # Between INFO (20) and WARNING (30)
ROLLBACK = 35  # Between WARNING (30) and ERROR (40)

logging.addLevelName(SUCCESS, "SUCCESS")
logging.addLevelName(ROLLBACK, "ROLLBACK")


class ColoredFormatter(logging.Formatter):
    """Custom formatter with color-coded log levels."""

    COLORS = {
        'DEBUG': Fore.CYAN,
        'INFO': Fore.BLUE,
        'SUCCESS': Fore.GREEN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'ROLLBACK': Fore.MAGENTA,
        'CRITICAL': Fore.RED + Style.BRIGHT,
    }

    def format(self, record):
        # Add color to level name
        levelname = record.levelname
        if levelname in self.COLORS:
            record.levelname = f"{self.COLORS[levelname]}{levelname}{Style.RESET_ALL}"
        
        # Format timestamp
        record.asctime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return super().format(record)


def setup_logger(name: str = "mf_analytics") -> logging.Logger:
    """
    Set up and return a configured logger.
    
    Args:
        name: Logger name
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.DEBUG)
    
    # Console handler with color formatting
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    
    formatter = ColoredFormatter(
        fmt='[%(asctime)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    
    # Add custom log methods
    def success(self, message, *args, **kwargs):
        if self.isEnabledFor(SUCCESS):
            self._log(SUCCESS, message, args, **kwargs)
    
    def rollback(self, message, *args, **kwargs):
        if self.isEnabledFor(ROLLBACK):
            self._log(ROLLBACK, message, args, **kwargs)
    
    logging.Logger.success = success
    logging.Logger.rollback = rollback
    
    return logger


# Global logger instance
logger = setup_logger()
