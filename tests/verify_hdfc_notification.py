import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.excel_merger import consolidate_amc_downloads

@patch("src.alerts.telegram_client.TelegramClient.send_message")
@patch("src.utils.excel_merger.merge_project_excels")
@patch("pathlib.Path.exists")
def test_notifications(mock_exists, mock_merge, mock_send):
    # Setup mocks
    mock_exists.return_value = False
    mock_merge.return_value = Path("test_output.xlsx")
    mock_send.return_value = True

    print("--- Testing HDFC (Should notify) ---")
    consolidate_amc_downloads("hdfc", 2025, 12)
    if mock_send.called:
        print("✅ SUCCESS: Telegram notification called for HDFC")
        # Check if message contains HDFC
        last_msg = mock_send.call_args[0][0]
        if "HDFC" in last_msg:
            print("✅ SUCCESS: Notification message contains 'HDFC'")
        else:
            print(f"❌ FAILED: Notification message does not contain 'HDFC'. Msg: {last_msg}")
    else:
        print("❌ FAILED: Telegram notification NOT called for HDFC")

    mock_send.reset_mock()

    print("\n--- Testing ICICI (Should NOT notify) ---")
    consolidate_amc_downloads("icici", 2025, 12)
    if not mock_send.called:
        print("✅ SUCCESS: Telegram notification NOT called for ICICI")
    else:
        print("❌ FAILED: Telegram notification WAS called for ICICI")

if __name__ == "__main__":
    test_notifications()
