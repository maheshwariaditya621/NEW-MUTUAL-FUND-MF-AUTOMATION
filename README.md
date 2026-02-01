# Mutual Fund Portfolio Analytics Platform - Backend Foundation

## Overview

Backend foundation for the Mutual Fund Portfolio Analytics Platform data ingestion system.

**Status**: Step 4 Complete - Backend Foundation Implemented

---

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your PostgreSQL and Telegram credentials
```

### 3. Set Up PostgreSQL

Follow `docs/POSTGRESQL_SETUP_GUIDE.md`

---

## Usage

```bash
python -m src.cli.run_pipeline \
  --amc "HDFC Mutual Fund" \
  --scheme "HDFC Equity Fund" \
  --plan "Direct" \
  --option "Growth" \
  --year 2025 \
  --month 1 \
  --test
```

---

## Documentation

- [PostgreSQL Setup Guide](docs/POSTGRESQL_SETUP_GUIDE.md)
- [Canonical Data Contract v1.0](docs/CANONICAL_DATA_CONTRACT_v1.0.md)
- [Implementation Plan](C:\Users\ADITYA MAHESHWARI\.gemini\antigravity\brain\b7d3b832-0095-496f-9344-91f6a21e80e5\implementation_plan.md)

---

**Version**: 1.0.0  
**Status**: Backend Foundation Complete ✅
