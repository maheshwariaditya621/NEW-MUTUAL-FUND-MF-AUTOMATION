"""
Configuration Management System
================================
Environment-based configuration with NO hardcoded secrets.

Supports dev and prod environments using .env files.
"""

import os
from pathlib import Path
from typing import Optional

# Try to import python-dotenv
try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False
    print("WARNING: python-dotenv not installed. Install with: pip install python-dotenv")


class Config:
    """
    Application configuration loaded from environment variables.
    
    Usage:
        from config.settings import config
        
        print(config.ENVIRONMENT)
        print(config.DB_HOST)
    """
    
    def __init__(self):
        """Initialize configuration from environment variables."""
        # Load .env file if available
        if DOTENV_AVAILABLE:
            env_file = Path(__file__).parent.parent / '.env'
            if env_file.exists():
                load_dotenv(env_file)
        
        # ============================================================
        # ENVIRONMENT
        # ============================================================
        self.ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
        
        # ============================================================
        # POSTGRESQL CONFIGURATION (not connected yet)
        # ============================================================
        self.DB_HOST = os.getenv("DB_HOST", "localhost")
        self.DB_PORT = int(os.getenv("DB_PORT", "5432"))
        self.DB_NAME = os.getenv("DB_NAME", "mutual_fund_db")
        self.DB_USER = os.getenv("DB_USER", "postgres")
        self.DB_PASSWORD = os.getenv("DB_PASSWORD", "")
        
        # Connection pool settings
        self.DB_POOL_MIN_SIZE = int(os.getenv("DB_POOL_MIN_SIZE", "2"))
        self.DB_POOL_MAX_SIZE = int(os.getenv("DB_POOL_MAX_SIZE", "10"))
        
        # ============================================================
        # TELEGRAM CONFIGURATION (not connected yet)
        # ============================================================
        self.TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
        self.TELEGRAM_ENABLED = os.getenv("TELEGRAM_ENABLED", "false").lower() == "true"
        
        # ============================================================
        # FILE PATHS
        # ============================================================
        # Base directory (project root)
        self.BASE_DIR = Path(__file__).parent.parent
        
        # Data directories
        self.DATA_DIR = Path(os.getenv("DATA_DIR", str(self.BASE_DIR / "data")))
        self.INPUT_DIR = Path(os.getenv("INPUT_DIR", str(self.DATA_DIR / "input")))
        self.OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", str(self.DATA_DIR / "output")))
        self.LOGS_DIR = Path(os.getenv("LOGS_DIR", str(self.BASE_DIR / "logs")))
        
        # Create directories if they don't exist
        self.INPUT_DIR.mkdir(parents=True, exist_ok=True)
        self.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        self.LOGS_DIR.mkdir(parents=True, exist_ok=True)
        
        # ============================================================
        # LOGGING CONFIGURATION
        # ============================================================
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
        self.LOG_TO_FILE = os.getenv("LOG_TO_FILE", "true").lower() == "true"
        self.LOG_FILE_PATH = self.LOGS_DIR / "pipeline.log"
        self.LOG_USE_SYMBOLS = os.getenv("LOG_USE_SYMBOLS", "true").lower() == "true"
        
        # ============================================================
        # PIPELINE CONFIGURATION
        # ============================================================
        self.BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
        self.MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
        self.RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "5"))
        
        # ============================================================
        # VALIDATION RULES
        # ============================================================
        self.STRICT_VALIDATION = os.getenv("STRICT_VALIDATION", "true").lower() == "true"
        self.ALLOW_PARTIAL_LOADS = os.getenv("ALLOW_PARTIAL_LOADS", "false").lower() == "true"
    
    def get_db_connection_string(self) -> str:
        """
        Get PostgreSQL connection string.
        
        Returns:
            Connection string in format: postgresql://user:password@host:port/database
        """
        return (
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@"
            f"{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )
    
    def validate(self) -> list[str]:
        """
        Validate configuration and return list of errors.
        
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Check required database credentials
        if not self.DB_PASSWORD and self.ENVIRONMENT == "prod":
            errors.append("DB_PASSWORD is required in production environment")
        
        # Check Telegram configuration if enabled
        if self.TELEGRAM_ENABLED:
            if not self.TELEGRAM_BOT_TOKEN:
                errors.append("TELEGRAM_BOT_TOKEN is required when Telegram is enabled")
            if not self.TELEGRAM_CHAT_ID:
                errors.append("TELEGRAM_CHAT_ID is required when Telegram is enabled")
        
        return errors
    
    def __repr__(self) -> str:
        """String representation (hides sensitive data)."""
        return (
            f"Config(\n"
            f"  ENVIRONMENT={self.ENVIRONMENT}\n"
            f"  DB_HOST={self.DB_HOST}\n"
            f"  DB_PORT={self.DB_PORT}\n"
            f"  DB_NAME={self.DB_NAME}\n"
            f"  DB_USER={self.DB_USER}\n"
            f"  DB_PASSWORD={'***' if self.DB_PASSWORD else '(not set)'}\n"
            f"  TELEGRAM_ENABLED={self.TELEGRAM_ENABLED}\n"
            f"  INPUT_DIR={self.INPUT_DIR}\n"
            f"  OUTPUT_DIR={self.OUTPUT_DIR}\n"
            f"  LOGS_DIR={self.LOGS_DIR}\n"
            f")"
        )


# Global configuration instance
config = Config()


# Validate configuration on import
_errors = config.validate()
if _errors:
    print("⚠️  Configuration validation warnings:")
    for error in _errors:
        print(f"  - {error}")


if __name__ == "__main__":
    print("\n" + "="*80)
    print("CONFIGURATION SYSTEM DEMO")
    print("="*80 + "\n")
    
    print(config)
    
    print("\n" + "="*80)
    print("Validation Errors:")
    print("="*80)
    errors = config.validate()
    if errors:
        for error in errors:
            print(f"  ❌ {error}")
    else:
        print("  ✅ No validation errors")
    
    print("\n")
