# Project Foundation - Folder Structure & Explanations

## Complete Folder Tree

```
d:/CODING/NEW MUTUAL FUND MF AUTOMATION/
│
├── .env.example              # Environment variables template (copy to .env)
├── .gitignore                # Git exclusions (secrets, data, logs)
├── README.md                 # Main project documentation
├── requirements.txt          # Python dependencies
├── demo.py                   # Foundation demonstration script
│
├── ingestion/                # 🔄 Pipeline Orchestration
│   ├── __init__.py
│   └── README.md
│
├── extractors/               # 📊 AMC-Specific Excel Parsers
│   ├── __init__.py
│   └── README.md
│
├── standardisation/          # 🔧 Data Transformation Rules
│   ├── __init__.py
│   └── README.md
│
├── validation/               # ✅ Quality Validation Rules
│   ├── __init__.py
│   └── README.md
│
├── loaders/                  # 💾 Database Loading Logic
│   ├── __init__.py
│   └── README.md
│
├── database/                 # 🗄️ Schema & Migrations
│   ├── __init__.py
│   └── README.md
│
├── config/                   # ⚙️ Environment Configuration
│   ├── __init__.py
│   ├── settings.py           # Configuration management system
│   └── README.md
│
├── log_system/               # 📝 Centralized Logging System
│   ├── __init__.py
│   ├── logger.py             # Colorized logging implementation
│   └── README.md
│
├── alerts/                   # 📢 Telegram Notifications
│   ├── __init__.py
│   └── README.md
│
├── analytics/                # 📈 Reporting & Insights
│   ├── __init__.py
│   └── README.md
│
├── streamlit_app/            # 🖥️ Web-Based UI
│   ├── __init__.py
│   └── README.md
│
├── scripts/                  # 🛠️ Utility Scripts
│   ├── __init__.py
│   └── README.md
│
├── tests/                    # 🧪 Unit & Integration Tests
│   ├── __init__.py
│   └── README.md
│
├── docs/                     # 📚 Documentation
│   ├── README.md
│   └── PREVIOUS_PROJECT.md   # Legacy project documentation
│
├── data/                     # 📁 Data Files (auto-created)
│   ├── input/                # Input Excel files
│   └── output/               # Output files
│
└── logs/                     # 📋 Log Files (auto-created)
```

---

## Module Responsibilities

### 🔄 ingestion/
**Orchestrates the entire data ingestion pipeline**
- Coordinates: Excel → Extraction → Standardization → Validation → Loading
- Handles error recovery and retry logic
- Sends alerts on success/failure
- Logs all pipeline activities

### 📊 extractors/
**AMC-specific Excel extraction logic**
- Each AMC has unique Excel formats
- Dedicated extractors for each AMC (HDFC, ICICI, Axis, etc.)
- Extracts raw data into common intermediate format
- Handles AMC-specific quirks and edge cases

### 🔧 standardisation/
**Transforms raw data into consistent format**
- Renames columns to standard names
- Converts data types (strings to dates, numbers)
- Normalizes text (trim whitespace, fix casing)
- Handles missing/null values consistently

### ✅ validation/
**Enforces strict data quality rules**
- **CRITICAL RULE**: NO partial or dirty data enters database
- Checks required fields (ISIN, scheme name, etc.)
- Validates data formats and business rules
- All-or-nothing: entire batch rejected if validation fails

### 💾 loaders/
**Loads validated data into PostgreSQL**
- Manages database connections
- Handles bulk inserts (efficient loading)
- Manages transactions (all-or-nothing loading)
- Handles duplicate detection and updates

### 🗄️ database/
**Database schema and connection management**
- Defines database schema (tables, indexes, constraints)
- Manages database migrations (schema version control)
- Provides connection pooling
- Contains database utility functions

### ⚙️ config/
**Environment-based configuration**
- Supports dev and prod environments
- NO hardcoded secrets (uses .env files)
- Prepares for PostgreSQL, Telegram, file paths
- Built-in validation

### 📝 log_system/
**Centralized, beautified logging**
- Colorized terminal output
- Custom SUCCESS log level
- Timestamps and module names
- Human-readable for non-coders

### 📢 alerts/
**Telegram-based notifications**
- Sends alerts for pipeline events
- Includes relevant context (AMC name, date, errors)
- Supports different alert levels

### 📈 analytics/
**Data analytics and reporting**
- Queries PostgreSQL for insights
- Generates summary statistics
- Exports reports in various formats
- Supports custom analytics queries

### 🖥️ streamlit_app/
**Backend verification UI**
- Web-based interface for data inspection
- Displays data from PostgreSQL
- Shows pipeline logs and status
- Allows filtering and searching

### 🛠️ scripts/
**One-time utility scripts**
- Ad-hoc debugging scripts
- One-time data migration scripts
- Testing utilities
- Manual data inspection tools

### 🧪 tests/
**Unit and integration tests**
- Ensures code works as expected
- Prevents regressions
- Tests individual functions and module interactions
- Test fixtures (sample Excel files, mock data)

### 📚 docs/
**Project documentation**
- Architecture diagrams
- Data flow documentation
- Deployment guides
- Troubleshooting guides

---

## Key Files

### .env.example
Template for environment variables. Copy to `.env` and fill in actual values.

### .gitignore
Excludes secrets, data files, logs, and legacy code from Git.

### requirements.txt
Python dependencies. Install with: `pip install -r requirements.txt`

### demo.py
Demonstration script showing logging and configuration systems in action.

### README.md
Main project documentation (stakeholder-friendly).

---

## Auto-Created Directories

These directories are automatically created by the configuration system:
- `data/input/` - Input Excel files
- `data/output/` - Output files
- `logs/` - Log files

---

## Next Steps

This is **foundation only**. No business logic implemented yet.

**To implement next:**
1. AMC-specific extractors
2. Standardization rules
3. Validation logic
4. PostgreSQL connection
5. Telegram alerts
6. Streamlit UI
