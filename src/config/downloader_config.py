"""
Configuration settings for HDFC downloader.
"""

# ============================================================================
# DRY RUN MODE
# ============================================================================
# When True, no API calls or downloads are made (testing only)
DRY_RUN = False

# ============================================================================
# FILE COUNT THRESHOLDS
# ============================================================================
# Expected file count per month (for sanity checking)
FILE_COUNT_MIN = 80
FILE_COUNT_MAX = 120

# ============================================================================
# RETRY CONFIGURATION
# ============================================================================
# Maximum number of retries for failed downloads
MAX_RETRIES = 3

# Backoff delays in seconds for each retry
RETRY_BACKOFF = [5, 15, 30]  # Retry 1: 5s, Retry 2: 15s, Retry 3: 30s

# Browser Headless mode
HEADLESS = False
