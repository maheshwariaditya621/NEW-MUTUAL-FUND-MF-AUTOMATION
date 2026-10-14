"""
Telegram configuration for alerts.

Set your credentials in environment variables or update here.
"""

import os

# ============================================================================
# TELEGRAM CREDENTIALS
# ============================================================================
# Get these from @BotFather on Telegram or environment variables
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8647498516:AAFXHfMga4Men0HzL65ikQF6BbK0JfS2xyY")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "1308792142")

# ============================================================================
# ALERT TYPE FEATURE FLAGS
# ============================================================================
# Enable/disable specific alert types

ALERTS_ENABLED = {
    "SUCCESS": True,          # New month downloaded successfully
    "WARNING": True,          # File count anomaly, corruption recovery
    "ERROR": True,            # Any failure (API, network, etc.)
    "NOT_PUBLISHED": True,   # Month not yet published by AMC
    "SCHEDULER": True,       # Scheduler lifecycle events
}

# ============================================================================
# GLOBAL TELEGRAM TOGGLE
# ============================================================================
# Master switch - set to False to disable all Telegram alerts
TELEGRAM_ENABLED = True
