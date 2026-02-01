"""
Test script for Telegram alerting system.

Tests all alert types: SUCCESS, WARNING, ERROR, NOT_PUBLISHED, SCHEDULER
"""

from src.alerts.telegram_notifier import get_notifier
from src.config import logger


def test_telegram_config():
    """Test 1: Verify Telegram configuration."""
    logger.info("=" * 60)
    logger.info("TEST 1: Telegram Configuration")
    logger.info("=" * 60)
    
    notifier = get_notifier()
    
    logger.info(f"Telegram enabled: {notifier.client.enabled}")
    logger.info(f"Bot token configured: {bool(notifier.client.bot_token)}")
    logger.info(f"Chat ID configured: {bool(notifier.client.chat_id)}")
    logger.info(f"Alert types enabled: {notifier.alerts_enabled}")
    
    if not notifier.client.enabled:
        logger.error("❌ Telegram is not enabled!")
        logger.error("Check credentials in src/config/telegram_config.py")
        return False
    
    logger.success("✅ Telegram configuration OK")
    return True


def test_success_alert():
    """Test 2: Send SUCCESS alert."""
    logger.info("=" * 60)
    logger.info("TEST 2: SUCCESS Alert")
    logger.info("=" * 60)
    
    notifier = get_notifier()
    
    result = notifier.notify_success(
        amc="HDFC",
        year=2025,
        month=2,
        files_downloaded=104,
        duration=15.23
    )
    
    if result:
        logger.success("✅ SUCCESS alert sent")
    else:
        logger.warning("⚠️ SUCCESS alert not sent (may be disabled)")
    
    return result


def test_warning_alert():
    """Test 3: Send WARNING alert."""
    logger.info("=" * 60)
    logger.info("TEST 3: WARNING Alert")
    logger.info("=" * 60)
    
    notifier = get_notifier()
    
    # Test file count warning
    result = notifier.notify_warning(
        amc="HDFC",
        year=2025,
        month=2,
        warning_type="Low File Count",
        message="Only 65 files downloaded (expected 80+)"
    )
    
    if result:
        logger.success("✅ WARNING alert sent")
    else:
        logger.warning("⚠️ WARNING alert not sent (may be disabled)")
    
    return result


def test_error_alert():
    """Test 4: Send ERROR alert."""
    logger.info("=" * 60)
    logger.info("TEST 4: ERROR Alert")
    logger.info("=" * 60)
    
    notifier = get_notifier()
    
    result = notifier.notify_error(
        amc="HDFC",
        year=2025,
        month=3,
        error_type="API Error",
        reason="HTTP 503 - Service Unavailable"
    )
    
    if result:
        logger.success("✅ ERROR alert sent")
    else:
        logger.warning("⚠️ ERROR alert not sent (may be disabled)")
    
    return result


def test_not_published_alert():
    """Test 5: Send NOT_PUBLISHED alert."""
    logger.info("=" * 60)
    logger.info("TEST 5: NOT_PUBLISHED Alert")
    logger.info("=" * 60)
    
    notifier = get_notifier()
    
    result = notifier.notify_not_published(
        amc="HDFC",
        year=2026,
        month=2
    )
    
    if result:
        logger.success("✅ NOT_PUBLISHED alert sent")
    else:
        logger.warning("⚠️ NOT_PUBLISHED alert not sent (may be disabled)")
    
    return result


def test_scheduler_alert():
    """Test 6: Send SCHEDULER alert."""
    logger.info("=" * 60)
    logger.info("TEST 6: SCHEDULER Alert")
    logger.info("=" * 60)
    
    notifier = get_notifier()
    
    result = notifier.notify_scheduler(
        event="Test Run Complete",
        message="Testing scheduler alerts",
        stats={
            "downloaded": 2,
            "skipped": 5,
            "failed": 1
        }
    )
    
    if result:
        logger.success("✅ SCHEDULER alert sent")
    else:
        logger.warning("⚠️ SCHEDULER alert not sent (may be disabled)")
    
    return result


def run_all_tests():
    """Run all Telegram alert tests."""
    logger.info("=" * 60)
    logger.info("TELEGRAM ALERTING SYSTEM - TEST SUITE")
    logger.info("=" * 60)
    logger.info("")
    
    results = {}
    
    # Test 1: Configuration
    results["config"] = test_telegram_config()
    
    if not results["config"]:
        logger.error("=" * 60)
        logger.error("TESTS ABORTED - Telegram not configured")
        logger.error("=" * 60)
        return
    
    logger.info("")
    input("Press Enter to test SUCCESS alert...")
    results["success"] = test_success_alert()
    
    logger.info("")
    input("Press Enter to test WARNING alert...")
    results["warning"] = test_warning_alert()
    
    logger.info("")
    input("Press Enter to test ERROR alert...")
    results["error"] = test_error_alert()
    
    logger.info("")
    input("Press Enter to test NOT_PUBLISHED alert...")
    results["not_published"] = test_not_published_alert()
    
    logger.info("")
    input("Press Enter to test SCHEDULER alert...")
    results["scheduler"] = test_scheduler_alert()
    
    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)
    
    for test_name, result in results.items():
        status = "✅ SENT" if result else "⚠️ NOT SENT"
        logger.info(f"{test_name.upper()}: {status}")
    
    logger.info("=" * 60)
    logger.info("Check your Telegram to verify messages received!")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_all_tests()
