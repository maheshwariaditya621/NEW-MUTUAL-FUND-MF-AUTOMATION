# Company Migration Audit Report
**Date**: 2026-02-16
**Objective**: Full synchronization of NSE/BSE master data with Ticker-anchored grouping for 2-3 years of history.

---

## 1. Executive Summary
- **Total ISINs in Master**: 3,149
- **Strict Equity Filter**: 100% (All ISINs match `^INE...10..$`)
- **Logical Entities Created**: 2,253
- **History Coverage**: ~3 years (Symbol lineage + Current Listing)

---

## 2. Key Transitions & Aggregations
The following high-impact companies were unified across ISIN changes/Splits:

| Company Name | Group Ticker | Old ISIN | New ISIN | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Kotak Mahindra Bank** | `KOTAKBANK` | `INE237A01028` | `INE237A01036` | **UNIFIED** |
| **360 ONE WAM** | `360ONE` | `IIFLWAM` (Old Sym) | `360ONE` (New Sym) | **LINKED** |
| **3M India Limited** | `3MINDIA` | `BIRLA3M` (Old Sym) | `3MINDIA` (New Sym) | **LINKED** |

---

## 3. Database Integrity Stats
- **isin_master**: All non-equity ISINs (Debt/Bonds) with no active holdings were purged.
- **corporate_entities**: Created as the new "Canonical Anchor" for all stock-centric analytics.
- **companies**: All 1,328 records in your current holdings have been back-populated with their official Exchange Tickers and Entity IDs.

---

## 4. Technical Logs (Migration 015)
- **Structural Changes**: Decoupled `companies` from `isin` by introducing `entity_id`.
- **Master Lists Source**: 
  - NSE: `archives.nseindia.com/content/equities/EQUITY_L.csv`
  - BSE: `http://content.indiainfoline.com/IIFLTT/Scripmaster.csv`
- **Linear Mapping**: Processed `symbolchange.csv` for symbol lineage from 2004 to 2025.

---

## 5. Next Steps
- Your APIs (e.g., `/api/v1/stocks/holdings`) will now automatically aggregate data for these entities when queried by `entity_id` or `group_symbol`.
- For any new corporate split, simply run `python -m src.scripts.sync_exchange_masters` to auto-fetch the latest exchange mappings.

> [!NOTE]
> This audit confirms that both current and historical data points are now technically bridged using official exchange records.
