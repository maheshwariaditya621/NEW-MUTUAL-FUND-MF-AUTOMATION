# Configuration Module

## Responsibility
Environment-based configuration management with NO hardcoded secrets.

## Why This Exists
Different environments (dev, prod) need different configurations:
- Dev uses local PostgreSQL
- Prod uses production PostgreSQL
- Secrets must NEVER be committed to Git

## How It Works
1. Configuration is loaded from environment variables
2. Environment variables are set in `.env` files (NOT committed to Git)
3. `.env.example` provides a template (committed to Git)

## Usage

```python
from config.settings import config

# Access configuration
print(config.ENVIRONMENT)  # "dev" or "prod"
print(config.DB_HOST)      # "localhost" or production host
print(config.INPUT_DIR)    # Path to input directory

# Get database connection string
conn_string = config.get_db_connection_string()

# Validate configuration
errors = config.validate()
if errors:
    print("Configuration errors:", errors)
```

## Environment Variables

### Required Variables
- `ENVIRONMENT` - Environment name (dev/prod)
- `DB_PASSWORD` - PostgreSQL password (required in prod)

### Optional Variables (with defaults)
- `DB_HOST` - PostgreSQL host (default: localhost)
- `DB_PORT` - PostgreSQL port (default: 5432)
- `DB_NAME` - Database name (default: mutual_fund_db)
- `DB_USER` - Database user (default: postgres)
- `TELEGRAM_BOT_TOKEN` - Telegram bot token
- `TELEGRAM_CHAT_ID` - Telegram chat ID
- `TELEGRAM_ENABLED` - Enable Telegram alerts (default: false)
- `INPUT_DIR` - Input files directory
- `OUTPUT_DIR` - Output files directory
- `LOGS_DIR` - Log files directory
- `LOG_LEVEL` - Logging level (default: INFO)
- `STRICT_VALIDATION` - Enable strict validation (default: true)

## Setup Instructions

### 1. Copy the example file
```bash
cp .env.example .env
```

### 2. Edit `.env` with your values
```bash
# Open .env in your editor
notepad .env  # Windows
```

### 3. Never commit `.env` to Git
The `.gitignore` file already excludes `.env` files.

## Example `.env` File

See `.env.example` for a complete template.

## Testing Configuration

Run the settings module directly:
```bash
python config/settings.py
```

This will show your current configuration and any validation errors.
