"""
Configuration settings for the Mutual Fund Portfolio Analytics Platform.

Loads environment variables and provides centralized configuration.

NOTE: This module does NOT validate credentials at import time.
Validation happens in respective modules:
- Database validation: src/db/connection.py (when connecting)
- Telegram validation: src/alerts/telegram.py (when sending alerts)
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent

# Environment
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")  # dev or prod

# Database Configuration (optional at import time)
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "mf_analytics")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Babyaditya#007")

# Telegram Configuration (optional at import time)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Admin Console Configuration
ADMIN_SECRET_KEY = os.getenv("ADMIN_SECRET_KEY", "admin123")

# JWT Authentication Configuration
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "7e46c70a889f6edfcff0480fac5847ea8415a08f8d3feda3b3d2b700a61b922e")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "480"))

# ============================================================
# ANGEL ONE SMARTAPI CONFIGURATION
# ============================================================
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY", "")
ANGEL_CLIENT_CODE = os.getenv("ANGEL_CLIENT_CODE", "")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD", "")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET", "")
