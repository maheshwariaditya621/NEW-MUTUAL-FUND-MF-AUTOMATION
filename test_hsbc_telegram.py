"""
Test HSBC Telegram Notifications
"""

from src.downloaders.hsbc_downloader import HSBCDownloader
from src.alerts.telegram_notifier import get_notifier
import shutil
from pathlib import Path

print("=" * 70)
print("HSBC TELEGRAM NOTIFICATION TEST")
print("=" * 70)
print()

# Setup: Delete Nov 2024 _SUCCESS.json to force re-download
success_file = Path("data/raw/hsbc/2024_11/_SUCCESS.json")
if success_file.exists():
    print(f"Removing {success_file} to force re-download...")
    success_file.unlink()
    print("✅ Removed")
else:
    print("⚠️  _SUCCESS.json already removed")

print()
print("=" * 70)
print("TEST: Download with Telegram Notification")
print("=" * 70)
print()

# Download Nov 2024 (should trigger Telegram notification)
downloader = HSBCDownloader()
result = downloader.download(year=2024, month=11)

print()
print("=" * 70)
print("DOWNLOAD RESULT")
print("=" * 70)
print(f"Status: {result['status']}")
print(f"Files downloaded: {result.get('files_downloaded', 0)}")
print(f"Duration: {result.get('duration', 0):.2f}s")
print()

if result['status'] == 'success':
    print("✅ Download successful")
    print()
    print("📱 CHECK TELEGRAM:")
    print("   - Should have received notification")
    print("   - Event: HSBC Download Success")
    print(f"   - Files: {result.get('files_downloaded', 0)}")
    print(f"   - Month: 2024-11")
else:
    print(f"❌ Download failed: {result.get('reason', 'Unknown')}")

print()
print("=" * 70)
print("MANUAL VERIFICATION REQUIRED")
print("=" * 70)
print("1. Check your Telegram bot for the notification")
print("2. Verify message contains:")
print("   - AMC: HSBC")
print("   - Month: 2024-11")
print("   - File count")
print("   - Duration")
print("=" * 70)
