# Alerts Module

## Responsibility
Send Telegram notifications for pipeline events.

## Why This Exists
Real-time alerts ensure you know immediately when:
- ✅ Data ingestion succeeds
- ❌ Pipeline fails
- ⚠️ Data quality issues detected

## What It Does
- Sends formatted messages to Telegram
- Includes relevant context (AMC name, date, error details)
- Supports different alert levels (INFO, WARNING, ERROR)

## What It Does NOT Do
- Does NOT log to files (that's `logging/`)
- Does NOT handle pipeline logic (that's `ingestion/`)

## Future Components
- `telegram_notifier.py` - Telegram bot integration
- `message_formatter.py` - Message formatting utilities
