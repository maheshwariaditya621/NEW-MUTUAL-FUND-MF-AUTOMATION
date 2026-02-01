"""
Telegram configuration for alerts.

Set your credentials in environment variables or update here.
"""

# ============================================================================
# TELEGRAM CREDENTIALS
# ============================================================================
# Get these from @BotFather on Telegram
TELEGRAM_BOT_TOKEN = "8508088144:AAHvwtlu3R1seJJtleM3OPRTZFXv2-Sdq7o"  # Replace with your bot token
TELEGRAM_CHAT_ID = "1308792142"      # Replace with your chat ID

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
