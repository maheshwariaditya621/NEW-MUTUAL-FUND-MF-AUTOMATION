"""
Watchlist router — Personalized Smart Watchlist.

Allows authenticated users to:
- Track stocks and mutual fund schemes
- View institutional activity (MF buying/selling) for tracked stocks
- View portfolio changes for tracked schemes
- Customise which insight modules appear on their dashboard
- Export watchlist data
"""

from typing import Optional, List
from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import StreamingResponse
from psycopg2.extensions import cursor
from pydantic import BaseModel
import io, csv, json
from datetime import datetime

from src.api.dependencies import get_db_cursor, get_current_user
from src.config import logger

router = APIRouter()

# ─────────────────────────── Pydantic Models ──────────────────────────────────

class WatchlistAddRequest(BaseModel):
    asset_type: str          # "stock" or "scheme"
    isin: Optional[str] = None          # for stocks
    scheme_id: Optional[int] = None     # for schemes

class PreferencesUpdateRequest(BaseModel):
    mf_buying:        Optional[bool] = None
    mf_selling:       Optional[bool] = None
    net_activity:     Optional[bool] = None
    top_holders:      Optional[bool] = None
    trend_indicator:  Optional[bool] = None
    popularity_score: Optional[bool] = None

# ─────────────────────────── Helpers ──────────────────────────────────────────

DEFAULT_PREFS = {
    "mf_buying":        True,
    "mf_selling":       True,
    "net_activity":     True,
    "top_holders":      False,
    "trend_indicator":  False,
    "popularity_score": False,
}

def _get_latest_two_periods(cur: cursor, period_id: int = None):
    """Returns (current_period_id, prev_period_id, current_year, current_month).
       If period_id is provided, uses that as the current period and previous one before it.
       prev_period_id may be None if only one period exists."""
    if period_id:
        cur.execute(
            "SELECT period_id, year, month FROM periods WHERE period_id = %s",
            (period_id,)
        )
        row = cur.fetchone()
        if not row:
            return None, None, None, None
        curr_pid, curr_yr, curr_mo = row
        # Find the period immediately before this one
        cur.execute(
            """
            SELECT period_id FROM periods
            WHERE (year < %s) OR (year = %s AND month < %s)
            ORDER BY year DESC, month DESC LIMIT 1
            """,
            (curr_yr, curr_yr, curr_mo)
        )
        prev_row = cur.fetchone()
        prev_pid = prev_row[0] if prev_row else None
        return curr_pid, prev_pid, curr_yr, curr_mo
    else:
        cur.execute(
            "SELECT period_id, year, month FROM periods ORDER BY year DESC, month DESC LIMIT 2"
        )
        rows = cur.fetchall()
        if not rows:
            return None, None, None, None
        curr_pid, curr_yr, curr_mo = rows[0]
        prev_pid = rows[1][0] if len(rows) > 1 else None
        return curr_pid, prev_pid, curr_yr, curr_mo


def _resolve_isin_to_company(isin: str, cur: cursor):
    cur.execute(
        "SELECT company_id, company_name, sector FROM companies WHERE isin = %s",
        (isin.upper(),)
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Stock with ISIN '{isin}' not found")
    return row  # (company_id, company_name, sector)


def _get_user_prefs(user_id: int, cur: cursor) -> dict:
    cur.execute(
        "SELECT prefs FROM user_watchlist_preferences WHERE user_id = %s",
        (user_id,)
    )
    row = cur.fetchone()
    if row:
        stored = row[0] if isinstance(row[0], dict) else json.loads(row[0])
        # Merge with defaults so new modules have a sane value
        return {**DEFAULT_PREFS, **stored}
    return dict(DEFAULT_PREFS)


def _format_shares(val) -> str:
    if val is None:
        return "—"
    v = int(val)
    if abs(v) >= 10_00_000:
        return f"{v/10_00_000:.1f}L"
    if abs(v) >= 1000:
        return f"{v/1000:.1f}K"
    return str(v)


# ─────────────────────────── Periods List ─────────────────────────────────────

@router.get("/periods")
async def list_periods(
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
):
    """Return all available periods for the period picker."""
    cur.execute(
        "SELECT period_id, year, month FROM periods ORDER BY year DESC, month DESC LIMIT 24"
    )
    rows = cur.fetchall()
    periods = []
    for period_id, year, month in rows:
        label = datetime(year, month, 1).strftime("%b %Y")
        periods.append({"period_id": period_id, "year": year, "month": month, "label": label})
    return {"periods": periods}


@router.get("/items")
async def list_watchlist_items(
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
):
    """Return all watchlist items for the currently authenticated user."""
    user_id = current_user["id"]

    cur.execute("""
        SELECT
            w.watchlist_id,
            w.asset_type,
            w.company_id,
            w.scheme_id,
            c.isin,
            COALESCE(ce.canonical_name, c.company_name) AS company_name,
            c.sector,
            s.scheme_name,
            a.amc_name,
            s.plan_type,
            s.option_type,
            w.added_at
        FROM user_watchlist w
        LEFT JOIN companies c           ON w.company_id = c.company_id
        LEFT JOIN corporate_entities ce ON c.entity_id  = ce.entity_id
        LEFT JOIN schemes s             ON w.scheme_id  = s.scheme_id
        LEFT JOIN amcs a                ON s.amc_id     = a.amc_id
        WHERE w.user_id = %s
        ORDER BY w.added_at DESC
    """, (user_id,))

    rows = cur.fetchall()

    items = []
    for row in rows:
        wid, atype, cid, sid, isin, cname, sector, sname, amc, plan, opt, added = row
        if atype == "stock":
            items.append({
                "watchlist_id":  wid,
                "asset_type":    "stock",
                "company_id":    cid,
                "isin":          isin,
                "name":          cname,
                "sector":        sector,
                "added_at":      added.isoformat() if added else None,
            })
        else:
            items.append({
                "watchlist_id":  wid,
                "asset_type":    "scheme",
                "scheme_id":     sid,
                "name":          sname,
                "amc_name":      amc,
                "plan_type":     plan,
                "option_type":   opt,
                "added_at":      added.isoformat() if added else None,
            })

    return {"items": items, "total": len(items)}


@router.post("/items")
async def add_watchlist_item(
    req: WatchlistAddRequest,
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
):
    """Add a stock or scheme to the user's watchlist."""
    user_id = current_user["id"]

    if req.asset_type == "stock":
        if not req.isin:
            raise HTTPException(status_code=400, detail="isin is required for stock assets")
        company_id, company_name, _ = _resolve_isin_to_company(req.isin, cur)

        # Check already watched
        cur.execute(
            "SELECT watchlist_id FROM user_watchlist WHERE user_id = %s AND company_id = %s",
            (user_id, company_id)
        )
        existing = cur.fetchone()
        if existing:
            return {"watchlist_id": existing[0], "added": False, "message": "Already in watchlist"}

        cur.execute(
            "INSERT INTO user_watchlist (user_id, asset_type, company_id) VALUES (%s, %s, %s) RETURNING watchlist_id",
            (user_id, "stock", company_id)
        )
        wid = cur.fetchone()[0]
        logger.info(f"User {user_id} added stock {req.isin} to watchlist")
        return {"watchlist_id": wid, "added": True, "name": company_name}

    elif req.asset_type == "scheme":
        if not req.scheme_id:
            raise HTTPException(status_code=400, detail="scheme_id is required for scheme assets")

        cur.execute("SELECT scheme_name FROM schemes WHERE scheme_id = %s", (req.scheme_id,))
        s = cur.fetchone()
        if not s:
            raise HTTPException(status_code=404, detail="Scheme not found")

        cur.execute(
            "SELECT watchlist_id FROM user_watchlist WHERE user_id = %s AND scheme_id = %s",
            (user_id, req.scheme_id)
        )
        existing = cur.fetchone()
        if existing:
            return {"watchlist_id": existing[0], "added": False, "message": "Already in watchlist"}

        cur.execute(
            "INSERT INTO user_watchlist (user_id, asset_type, scheme_id) VALUES (%s, %s, %s) RETURNING watchlist_id",
            (user_id, "scheme", req.scheme_id)
        )
        wid = cur.fetchone()[0]
        logger.info(f"User {user_id} added scheme {req.scheme_id} to watchlist")
        return {"watchlist_id": wid, "added": True, "name": s[0]}

    else:
        raise HTTPException(status_code=400, detail="asset_type must be 'stock' or 'scheme'")


@router.delete("/items/{watchlist_id}")
async def remove_watchlist_item(
    watchlist_id: int,
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
):
    """Remove an item from the user's watchlist."""
    user_id = current_user["id"]

    cur.execute(
        "DELETE FROM user_watchlist WHERE watchlist_id = %s AND user_id = %s RETURNING watchlist_id",
        (watchlist_id, user_id)
    )
    deleted = cur.fetchone()
    if not deleted:
        raise HTTPException(status_code=404, detail="Watchlist item not found")

    return {"removed": True, "watchlist_id": watchlist_id}


@router.get("/items/check")
async def check_watched(
    asset_type: str = Query(...),
    identifier: str = Query(..., description="ISIN for stocks, scheme_id for schemes"),
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
):
    """Check whether a stock (by ISIN) or scheme (by scheme_id) is in the user's watchlist."""
    user_id = current_user["id"]

    if asset_type == "stock":
        cur.execute(
            """SELECT w.watchlist_id FROM user_watchlist w
               JOIN companies c ON w.company_id = c.company_id
               WHERE w.user_id = %s AND c.isin = %s""",
            (user_id, identifier.upper())
        )
    else:
        cur.execute(
            "SELECT watchlist_id FROM user_watchlist WHERE user_id = %s AND scheme_id = %s",
            (user_id, int(identifier))
        )

    row = cur.fetchone()
    return {"is_watched": bool(row), "watchlist_id": row[0] if row else None}


# ─────────────────────────── Dashboard ────────────────────────────────────────

@router.get("/dashboard")
async def get_dashboard(
    period_id: Optional[int] = Query(None, description="Period ID to analyse. Defaults to latest."),
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
):
    """
    Watchlist dashboard overview metrics.

    Returns:
    - total stocks / schemes tracked
    - stocks with strongest MF buying (by share count delta)
    - stocks with strongest MF selling
    - most active schemes (most portfolio changes)
    - per-asset summary row (buying, selling, net, trend)
    """
    user_id = current_user["id"]
    curr_pid, prev_pid, curr_yr, curr_mo = _get_latest_two_periods(cur, period_id)
    if not curr_pid:
        return {"error": "No portfolio data available"}

    # Fetch user's watchlist
    cur.execute(
        "SELECT asset_type, company_id, scheme_id FROM user_watchlist WHERE user_id = %s",
        (user_id,)
    )
    wlist = cur.fetchall()

    stock_ids  = [r[1] for r in wlist if r[0] == "stock"]
    scheme_ids = [r[2] for r in wlist if r[0] == "scheme"]

    # ── Stock activity: gross buying + gross selling computed per scheme ──
    # Only compare against schemes from AMCs that have uploaded curr-period data.
    # If an AMC hasn't submitted yet, their prev-period holdings must be excluded
    # to avoid showing false 100%-sold entries.
    stock_summaries = []
    if stock_ids and prev_pid:
        placeholders = ",".join(["%s"] * len(stock_ids))
        cur.execute(f"""
            WITH curr_amcs AS (
                -- AMCs that have actually submitted data for the current period
                SELECT DISTINCT s.amc_id
                FROM scheme_snapshots ss
                JOIN schemes s ON ss.scheme_id = s.scheme_id
                WHERE ss.period_id = %s
            ),
            curr AS (
                SELECT ss.scheme_id, eh.company_id, eh.quantity
                FROM equity_holdings eh
                JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
                WHERE ss.period_id = %s AND eh.company_id IN ({placeholders})
            ),
            prev AS (
                -- Only previous-period data for AMCs that ALSO have current-period data
                SELECT ss.scheme_id, eh.company_id, eh.quantity
                FROM equity_holdings eh
                JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
                JOIN schemes s2 ON ss.scheme_id = s2.scheme_id
                JOIN curr_amcs ca ON s2.amc_id = ca.amc_id
                WHERE ss.period_id = %s AND eh.company_id IN ({placeholders})
            ),
            per_scheme AS (
                SELECT
                    COALESCE(c.company_id, p.company_id) AS company_id,
                    COALESCE(c.scheme_id,  p.scheme_id)  AS scheme_id,
                    COALESCE(c.quantity, 0)               AS curr_qty,
                    COALESCE(p.quantity, 0)               AS prev_qty,
                    COALESCE(c.quantity, 0) - COALESCE(p.quantity, 0) AS delta
                FROM curr c
                FULL OUTER JOIN prev p
                    ON c.company_id = p.company_id AND c.scheme_id = p.scheme_id
            )
            SELECT
                co.company_id,
                COALESCE(ce.canonical_name, co.company_name) AS name,
                co.isin,
                co.sector,
                SUM(CASE WHEN ps.delta > 0 THEN ps.delta          ELSE 0 END) AS gross_buying,
                SUM(CASE WHEN ps.delta < 0 THEN ABS(ps.delta)     ELSE 0 END) AS gross_selling,
                SUM(ps.delta)                                                   AS net_change,
                COUNT(DISTINCT CASE WHEN ps.curr_qty > 0 THEN ps.scheme_id END) AS num_funds,
                COUNT(DISTINCT CASE WHEN ps.delta > 0  THEN ps.scheme_id END)   AS buying_schemes,
                COUNT(DISTINCT CASE WHEN ps.delta < 0  THEN ps.scheme_id END)   AS selling_schemes
            FROM per_scheme ps
            JOIN companies co ON ps.company_id = co.company_id
            LEFT JOIN corporate_entities ce ON co.entity_id = ce.entity_id
            GROUP BY co.company_id, co.company_name, ce.canonical_name, co.isin, co.sector
            ORDER BY net_change DESC
        """, [curr_pid, curr_pid, *stock_ids, prev_pid, *stock_ids])

        for row in cur.fetchall():
            cid, name, isin, sector, gross_buy, gross_sell, net, num_funds, buying_sc, selling_sc = row
            gross_buy  = int(gross_buy  or 0)
            gross_sell = int(gross_sell or 0)
            net        = int(net        or 0)
            trend = "up" if net > 0 else ("down" if net < 0 else "stable")

            stock_summaries.append({
                "company_id":      cid,
                "name":            name,
                "isin":            isin,
                "sector":          sector,
                "net_change":      net,
                "buying":          gross_buy,
                "selling":         gross_sell,
                "num_funds":       int(num_funds     or 0),
                "buying_schemes":  int(buying_sc     or 0),
                "selling_schemes": int(selling_sc    or 0),
                "trend":           trend,
            })


    # ── Scheme activity: only compare schemes whose AMC has curr-period data ──
    scheme_summaries = []
    if scheme_ids and prev_pid:
        placeholders = ",".join(["%s"] * len(scheme_ids))
        cur.execute(f"""
            WITH curr_amcs AS (
                SELECT DISTINCT s.amc_id
                FROM scheme_snapshots ss
                JOIN schemes s ON ss.scheme_id = s.scheme_id
                WHERE ss.period_id = %s
            ),
            curr AS (
                SELECT ss.scheme_id, COUNT(DISTINCT eh.company_id) AS holdings_cnt
                FROM scheme_snapshots ss
                JOIN equity_holdings eh ON eh.snapshot_id = ss.snapshot_id
                WHERE ss.period_id = %s AND ss.scheme_id IN ({placeholders})
                GROUP BY ss.scheme_id
            ),
            prev AS (
                SELECT ss.scheme_id, COUNT(DISTINCT eh.company_id) AS holdings_cnt
                FROM scheme_snapshots ss
                JOIN equity_holdings eh ON eh.snapshot_id = ss.snapshot_id
                JOIN schemes s2 ON ss.scheme_id = s2.scheme_id
                JOIN curr_amcs ca ON s2.amc_id = ca.amc_id
                WHERE ss.period_id = %s AND ss.scheme_id IN ({placeholders})
                GROUP BY ss.scheme_id
            )
            SELECT s.scheme_id, s.scheme_name, a.amc_name,
                   COALESCE(curr.holdings_cnt, 0),
                   COALESCE(prev.holdings_cnt, 0),
                   (COALESCE(curr.holdings_cnt, 0) - COALESCE(prev.holdings_cnt, 0)) AS delta
            FROM schemes s
            JOIN amcs a ON s.amc_id = a.amc_id
            -- Only show schemes whose AMC has curr-period data
            JOIN curr_amcs ca2 ON s.amc_id = ca2.amc_id
            LEFT JOIN curr ON curr.scheme_id = s.scheme_id
            LEFT JOIN prev ON prev.scheme_id = s.scheme_id
            WHERE s.scheme_id IN ({placeholders})
            ORDER BY ABS(COALESCE(curr.holdings_cnt, 0) - COALESCE(prev.holdings_cnt, 0)) DESC
        """, [curr_pid, curr_pid, *scheme_ids, prev_pid, *scheme_ids, *scheme_ids])

        for row in cur.fetchall():
            sid, sname, amc, curr_cnt, prev_cnt, delta = row
            scheme_summaries.append({
                "scheme_id":      sid,
                "name":           sname,
                "amc_name":       amc,
                "curr_holdings":  int(curr_cnt),
                "prev_holdings":  int(prev_cnt),
                "delta":          int(delta),
            })

    # ── Overview metrics ──
    strongest_buying  = sorted(stock_summaries, key=lambda x: x["buying"],   reverse=True)[:3]
    strongest_selling = sorted(stock_summaries, key=lambda x: x["selling"],  reverse=True)[:3]
    most_active       = sorted(scheme_summaries, key=lambda x: abs(x["delta"]), reverse=True)[:3]

    total_buying  = sum(s["buying"]  for s in stock_summaries)
    total_selling = sum(s["selling"] for s in stock_summaries)

    return {
        "period":              datetime(curr_yr, curr_mo, 1).strftime('%b %Y'),
        "total_stocks":       len(stock_ids),
        "total_schemes":      len(scheme_ids),
        "total_assets":       len(wlist),
        "total_buying":       total_buying,
        "total_selling":      total_selling,
        "strongest_buying":   strongest_buying,
        "strongest_selling":  strongest_selling,
        "most_active_schemes": most_active,
        "stock_summaries":    stock_summaries,
        "scheme_summaries":   scheme_summaries,
    }


# ─────────────────────────── Stock Activity ───────────────────────────────────

@router.get("/stocks/{isin}/activity")
async def get_stock_activity(
    isin: str,
    period_id: Optional[int] = Query(None, description="Period ID to analyse. Defaults to latest."),
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
):
    """
    Detailed mutual fund activity for a specific stock:
    - MF Buying (schemes that increased shares)
    - MF Selling (schemes that decreased shares)
    - Net institutional activity
    - Top MF holders
    - Trend indicator (3-month direction)
    - MF Popularity Score
    """
    curr_pid, prev_pid, curr_yr, curr_mo = _get_latest_two_periods(cur, period_id)
    if not curr_pid:
        raise HTTPException(status_code=404, detail="No portfolio data available")

    company_id, company_name, sector = _resolve_isin_to_company(isin, cur)
    month_label = datetime(curr_yr, curr_mo, 1).strftime("%b %Y")

    # ── Per-scheme change (drawer detail) ──
    if prev_pid:
        cur.execute("""
            WITH curr_amcs AS (
                SELECT DISTINCT s.amc_id
                FROM scheme_snapshots ss
                JOIN schemes s ON ss.scheme_id = s.scheme_id
                WHERE ss.period_id = %s
            ),
            curr AS (
                SELECT ss.scheme_id, eh.quantity
                FROM equity_holdings eh
                JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
                WHERE ss.period_id = %s AND eh.company_id = %s
            ),
            prev AS (
                SELECT ss.scheme_id, eh.quantity
                FROM equity_holdings eh
                JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
                JOIN schemes s2 ON ss.scheme_id = s2.scheme_id
                JOIN curr_amcs ca ON s2.amc_id = ca.amc_id
                WHERE ss.period_id = %s AND eh.company_id = %s
            )
            SELECT
                s.scheme_name,
                a.amc_name,
                s.scheme_id,
                COALESCE(curr.quantity, 0) AS curr_qty,
                COALESCE(prev.quantity, 0) AS prev_qty,
                (COALESCE(curr.quantity, 0) - COALESCE(prev.quantity, 0)) AS delta,
                CASE WHEN prev.quantity > 0
                     THEN ROUND(((COALESCE(curr.quantity,0) - prev.quantity)::numeric / prev.quantity * 100), 2)
                     ELSE NULL
                END AS pct_change,
                eh_curr.percent_of_nav
            FROM schemes s
            JOIN amcs a ON s.amc_id = a.amc_id
            JOIN curr_amcs ca2 ON s.amc_id = ca2.amc_id
            LEFT JOIN curr ON curr.scheme_id = s.scheme_id
            LEFT JOIN prev ON prev.scheme_id = s.scheme_id
            LEFT JOIN scheme_snapshots ss2 ON ss2.scheme_id = s.scheme_id AND ss2.period_id = %s
            LEFT JOIN equity_holdings eh_curr ON eh_curr.snapshot_id = ss2.snapshot_id AND eh_curr.company_id = %s
            WHERE curr.scheme_id IS NOT NULL OR prev.scheme_id IS NOT NULL
            ORDER BY delta DESC
        """, (curr_pid, curr_pid, company_id, prev_pid, company_id, curr_pid, company_id))
    else:
        cur.execute("""
            SELECT s.scheme_name, a.amc_name, s.scheme_id,
                   eh.quantity, 0, eh.quantity, NULL, eh.percent_of_nav
            FROM equity_holdings eh
            JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
            JOIN schemes s ON ss.scheme_id = s.scheme_id
            JOIN amcs a ON s.amc_id = a.amc_id
            WHERE ss.period_id = %s AND eh.company_id = %s
            ORDER BY eh.quantity DESC
        """, (curr_pid, company_id))

    rows = cur.fetchall()

    buying, selling = [], []
    total_buying, total_selling = 0, 0

    for row in rows:
        sname, amc, sid, curr_q, prev_q, delta, pct, nav = row
        item = {
            "scheme_name": sname,
            "amc_name":    amc,
            "scheme_id":   sid,
            "curr_shares": int(curr_q or 0),
            "prev_shares": int(prev_q or 0),
            "delta":       int(delta or 0),
            "pct_change":  float(pct) if pct else None,
            "month":       month_label,
            "percent_of_nav": float(nav) if nav else None,
        }
        if (delta or 0) > 0:
            buying.append(item)
            total_buying += int(delta)
        elif (delta or 0) < 0:
            item["delta"] = abs(item["delta"])
            selling.append(item)
            total_selling += abs(int(delta))

    buying.sort(key=lambda x: x["delta"], reverse=True)
    selling.sort(key=lambda x: x["delta"], reverse=True)

    net_change = total_buying - total_selling
    net_direction = "accumulation" if net_change > 0 else ("distribution" if net_change < 0 else "neutral")

    # ── Top Holders (current period, sorted by % of NAV) ──
    cur.execute("""
        SELECT s.scheme_name, a.amc_name, eh.quantity, eh.percent_of_nav,
               ss.total_value_inr / 10000000.0 AS aum_cr
        FROM equity_holdings eh
        JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
        JOIN schemes s ON ss.scheme_id = s.scheme_id
        JOIN amcs a ON s.amc_id = a.amc_id
        WHERE ss.period_id = %s AND eh.company_id = %s
        ORDER BY eh.quantity DESC
        LIMIT 10
    """, (curr_pid, company_id))

    top_holders = [{
        "scheme_name":    r[0],
        "amc_name":       r[1],
        "shares_held":    int(r[2]),
        "pct_of_nav":     float(r[3]),
        "scheme_aum_cr":  float(r[4]) if r[4] else None,
    } for r in cur.fetchall()]

    # ── Trend Indicator (last 3 months) ──
    cur.execute("""
        SELECT p.year, p.month, SUM(eh.quantity)
        FROM equity_holdings eh
        JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
        JOIN periods p ON ss.period_id = p.period_id
        WHERE eh.company_id = %s
        GROUP BY p.year, p.month
        ORDER BY p.year DESC, p.month DESC
        LIMIT 3
    """, (company_id,))
    trend_rows = cur.fetchall()

    trend_label = "Stable"
    trend_data  = [{"year": r[0], "month": r[1], "total_shares": int(r[2])} for r in trend_rows]
    if len(trend_data) >= 2:
        if trend_data[0]["total_shares"] > trend_data[-1]["total_shares"]:
            trend_label = "Increasing"
        elif trend_data[0]["total_shares"] < trend_data[-1]["total_shares"]:
            trend_label = "Decreasing"

    # ── Popularity Score ──
    cur.execute("""
        SELECT COUNT(DISTINCT ss.scheme_id) AS scheme_count
        FROM equity_holdings eh
        JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
        WHERE ss.period_id = %s AND eh.company_id = %s
    """, (curr_pid, company_id))
    scheme_count = cur.fetchone()[0] or 0

    if scheme_count >= 50:      pop_score = "Very High"
    elif scheme_count >= 30:    pop_score = "High"
    elif scheme_count >= 15:    pop_score = "Medium"
    elif scheme_count >= 5:     pop_score = "Low"
    else:                       pop_score = "Very Low"

    return {
        "isin":            isin,
        "company_name":    company_name,
        "sector":          sector,
        "period":          month_label,
        "buying":          buying,
        "selling":         selling,
        "net_activity": {
            "total_buying":   _format_shares(total_buying),
            "total_selling":  _format_shares(total_selling),
            "net_change":     _format_shares(abs(net_change)),
            "net_raw":        net_change,
            "direction":      net_direction,
        },
        "top_holders":     top_holders,
        "trend": {
            "label":   trend_label,
            "data":    trend_data,
        },
        "popularity": {
            "score":        pop_score,
            "scheme_count": scheme_count,
        },
    }


# ─────────────────────────── Scheme Activity ──────────────────────────────────

@router.get("/schemes/{scheme_id}/activity")
async def get_scheme_activity(
    scheme_id: int,
    period_id: Optional[int] = Query(None, description="Period ID to analyse. Defaults to latest."),
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
):
    """
    Portfolio evolution for a tracked mutual fund scheme:
    - New stocks added this period
    - Stocks reduced / exited
    - Top current holdings
    """
    curr_pid, prev_pid, curr_yr, curr_mo = _get_latest_two_periods(cur, period_id)
    if not curr_pid:
        raise HTTPException(status_code=404, detail="No portfolio data available")

    cur.execute(
        "SELECT s.scheme_name, a.amc_name, s.plan_type, s.option_type FROM schemes s JOIN amcs a ON s.amc_id = a.amc_id WHERE s.scheme_id = %s",
        (scheme_id,)
    )
    meta = cur.fetchone()
    if not meta:
        raise HTTPException(status_code=404, detail="Scheme not found")


    sname, amc, plan, opt = meta
    month_label = datetime(curr_yr, curr_mo, 1).strftime("%b %Y")

    if prev_pid:
        cur.execute("""
            WITH curr AS (
                SELECT eh.company_id, eh.quantity, eh.percent_of_nav
                FROM equity_holdings eh
                JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
                WHERE ss.scheme_id = %s AND ss.period_id = %s
            ),
            prev AS (
                SELECT eh.company_id, eh.quantity
                FROM equity_holdings eh
                JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
                WHERE ss.scheme_id = %s AND ss.period_id = %s
            )
            SELECT
                COALESCE(ce.canonical_name, c.company_name) AS name,
                c.isin, c.sector,
                COALESCE(curr.quantity, 0) AS curr_qty,
                COALESCE(prev.quantity, 0) AS prev_qty,
                (COALESCE(curr.quantity, 0) - COALESCE(prev.quantity, 0)) AS delta,
                CASE WHEN prev.quantity > 0
                     THEN ROUND(((COALESCE(curr.quantity,0) - prev.quantity)::numeric / prev.quantity * 100), 2)
                     ELSE NULL
                END AS pct_change,
                curr.percent_of_nav
            FROM companies c
            LEFT JOIN corporate_entities ce ON c.entity_id = ce.entity_id
            LEFT JOIN curr ON curr.company_id = c.company_id
            LEFT JOIN prev ON prev.company_id = c.company_id
            WHERE (curr.company_id IS NOT NULL OR prev.company_id IS NOT NULL)
            ORDER BY curr.quantity DESC NULLS LAST
        """, (scheme_id, curr_pid, scheme_id, prev_pid))
    else:
        cur.execute("""
            SELECT COALESCE(ce.canonical_name, c.company_name), c.isin, c.sector,
                   eh.quantity, 0, eh.quantity, NULL, eh.percent_of_nav
            FROM equity_holdings eh
            JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
            JOIN companies c ON eh.company_id = c.company_id
            LEFT JOIN corporate_entities ce ON c.entity_id = ce.entity_id
            WHERE ss.scheme_id = %s AND ss.period_id = %s
            ORDER BY eh.quantity DESC
        """, (scheme_id, curr_pid))

    rows = cur.fetchall()

    new_adds, reductions, top_holdings = [], [], []

    for row in rows:
        name, isin, sector, curr_q, prev_q, delta, pct, nav = row
        item = {
            "stock_name":  name,
            "isin":        isin,
            "sector":      sector,
            "curr_shares": int(curr_q or 0),
            "prev_shares": int(prev_q or 0),
            "delta":       int(delta or 0),
            "pct_change":  float(pct) if pct else None,
            "pct_of_nav":  float(nav) if nav else None,
            "month":       month_label,
        }
        if prev_q == 0 and curr_q > 0:
            new_adds.append(item)
        elif delta < 0 and prev_q > 0:
            item["delta"] = abs(item["delta"])
            reductions.append(item)

        if curr_q > 0:
            top_holdings.append(item)

    new_adds.sort(key=lambda x: x["curr_shares"], reverse=True)
    reductions.sort(key=lambda x: x["delta"], reverse=True)
    top_holdings.sort(key=lambda x: x.get("pct_of_nav") or 0, reverse=True)

    return {
        "scheme_id":    scheme_id,
        "scheme_name":  sname,
        "amc_name":     amc,
        "plan_type":    plan,
        "option_type":  opt,
        "period":       month_label,
        "new_adds":     new_adds[:20],
        "reductions":   reductions[:20],
        "top_holdings": top_holdings[:15],
    }


# ─────────────────────────── Activity Feed ────────────────────────────────────

@router.get("/activity-feed")
async def get_activity_feed(
    period_id: Optional[int] = Query(None, description="Period ID to analyse. Defaults to latest."),
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
):
    """
    Unified chronological activity feed of the most significant portfolio
    changes across all assets in the user's watchlist.
    """
    user_id = current_user["id"]
    curr_pid, prev_pid, curr_yr, curr_mo = _get_latest_two_periods(cur, period_id)

    if not curr_pid or not prev_pid:
        return {"feed": [], "period": None}

    month_label = datetime(curr_yr, curr_mo, 1).strftime("%b %Y")

    # Fetch user's watched stocks and schemes
    cur.execute(
        "SELECT asset_type, company_id, scheme_id FROM user_watchlist WHERE user_id = %s",
        (user_id,)
    )
    wlist = cur.fetchall()
    stock_ids  = [r[1] for r in wlist if r[0] == "stock"]
    scheme_ids = [r[2] for r in wlist if r[0] == "scheme"]

    feed = []

    # ── Stock-level events: per-scheme change for all watched stocks ──
    if stock_ids:
        placeholders = ",".join(["%s"] * len(stock_ids))
        cur.execute(f"""
            WITH curr_amcs AS (
                SELECT DISTINCT s.amc_id
                FROM scheme_snapshots ss
                JOIN schemes s ON ss.scheme_id = s.scheme_id
                WHERE ss.period_id = %s
            ),
            curr AS (
                SELECT eh.company_id, ss.scheme_id, eh.quantity
                FROM equity_holdings eh
                JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
                WHERE ss.period_id = %s AND eh.company_id IN ({placeholders})
            ),
            prev AS (
                SELECT eh.company_id, ss.scheme_id, eh.quantity
                FROM equity_holdings eh
                JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
                JOIN schemes s2 ON ss.scheme_id = s2.scheme_id
                JOIN curr_amcs ca ON s2.amc_id = ca.amc_id
                WHERE ss.period_id = %s AND eh.company_id IN ({placeholders})
            ),
            combined AS (
                SELECT
                    COALESCE(c.company_id, p.company_id) AS company_id,
                    COALESCE(c.scheme_id, p.scheme_id)   AS scheme_id,
                    COALESCE(c.quantity, 0)               AS curr_qty,
                    COALESCE(p.quantity, 0)               AS prev_qty,
                    COALESCE(c.quantity, 0) - COALESCE(p.quantity, 0) AS delta
                FROM curr c
                FULL OUTER JOIN prev p
                    ON c.company_id = p.company_id AND c.scheme_id = p.scheme_id
            )
            SELECT
                COALESCE(ce.canonical_name, c.company_name) AS stock_name,
                s.scheme_name,
                a.amc_name,
                cb.delta,
                cb.curr_qty,
                cb.prev_qty
            FROM combined cb
            JOIN companies c   ON cb.company_id = c.company_id
            LEFT JOIN corporate_entities ce ON c.entity_id = ce.entity_id
            JOIN schemes s     ON cb.scheme_id = s.scheme_id
            JOIN amcs a        ON s.amc_id = a.amc_id
            WHERE ABS(cb.delta) > 0
            ORDER BY ABS(cb.delta) DESC
            LIMIT 20
        """, [curr_pid, curr_pid, *stock_ids, prev_pid, *stock_ids])

        for row in cur.fetchall():
            cname, sname, amc, delta, curr_q, prev_q = row
            action = "bought" if delta > 0 else "reduced"
            if prev_q > 0:
                pct_val = round(abs(delta) / prev_q * 100, 1)
                # Cap pct at 100% max to avoid confusing 100% sold when data is partial
                pct_val = min(pct_val, 100.0)
                pct_str = f"({'+' if delta>0 else '-'}{pct_val}%)"
            else:
                pct_str = "(new position)"

            feed.append({
                "type":    "stock",
                "message": f"{sname} {action} {_format_shares(abs(delta))} shares of {cname} {pct_str}".strip(),
                "delta":   int(delta),
                "month":   month_label,
            })

    # ── Scheme-level events (new adds, exits, significant changes) ──
    if scheme_ids:
        placeholders = ",".join(["%s"] * len(scheme_ids))
        cur.execute(f"""
            WITH curr_amcs AS (
                SELECT DISTINCT s.amc_id
                FROM scheme_snapshots ss
                JOIN schemes s ON ss.scheme_id = s.scheme_id
                WHERE ss.period_id = %s
            ),
            curr AS (
                SELECT ss.scheme_id, eh.company_id, eh.quantity
                FROM equity_holdings eh
                JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
                WHERE ss.period_id = %s AND ss.scheme_id IN ({placeholders})
            ),
            prev AS (
                SELECT ss.scheme_id, eh.company_id, eh.quantity
                FROM equity_holdings eh
                JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
                JOIN schemes s2 ON ss.scheme_id = s2.scheme_id
                JOIN curr_amcs ca ON s2.amc_id = ca.amc_id
                WHERE ss.period_id = %s AND ss.scheme_id IN ({placeholders})
            ),
            combined AS (
                SELECT
                    COALESCE(c.scheme_id,  p.scheme_id)  AS scheme_id,
                    COALESCE(c.company_id, p.company_id) AS company_id,
                    COALESCE(c.quantity, 0)              AS curr_qty,
                    COALESCE(p.quantity, 0)              AS prev_qty,
                    COALESCE(c.quantity, 0) - COALESCE(p.quantity, 0) AS delta
                FROM curr c
                FULL OUTER JOIN prev p
                    ON c.scheme_id = p.scheme_id AND c.company_id = p.company_id
            )
            SELECT
                s.scheme_name,
                COALESCE(ce.canonical_name, co.company_name) AS stock_name,
                cb.delta,
                cb.prev_qty
            FROM combined cb
            JOIN schemes s  ON cb.scheme_id  = s.scheme_id
            JOIN companies co ON cb.company_id = co.company_id
            LEFT JOIN corporate_entities ce ON co.entity_id = ce.entity_id
            WHERE ABS(cb.delta) > 0
            ORDER BY ABS(cb.delta) DESC
            LIMIT 20
        """, [curr_pid, curr_pid, *scheme_ids, prev_pid, *scheme_ids])

        for row in cur.fetchall():
            sname, cname, delta, prev_q = row
            if prev_q == 0 and delta > 0:
                action = "added new position in"
            elif delta < 0 and prev_q > 0 and (prev_q + delta) == 0:
                action = "fully exited"
            elif delta > 0:
                action = "increased position in"
            else:
                action = "reduced"

            feed.append({
                "type":    "scheme",
                "message": f"{sname} {action} {cname} ({_format_shares(abs(delta))} shares)",
                "delta":   int(delta),
                "month":   month_label,
            })

    # Sort by magnitude
    feed.sort(key=lambda x: abs(x.get("delta", 0)), reverse=True)

    return {"feed": feed[:30], "period": month_label}


# ─────────────────────────── Preferences ──────────────────────────────────────

@router.get("/preferences")
async def get_preferences(
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
):
    """Get the current user's watchlist module preferences."""
    return _get_user_prefs(current_user["id"], cur)


@router.put("/preferences")
async def update_preferences(
    req: PreferencesUpdateRequest,
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
):
    """Update watchlist module preferences for the current user."""
    user_id = current_user["id"]
    current = _get_user_prefs(user_id, cur)

    updates = req.dict(exclude_none=True)
    merged  = {**current, **updates}

    cur.execute("""
        INSERT INTO user_watchlist_preferences (user_id, prefs, updated_at)
        VALUES (%s, %s::jsonb, NOW())
        ON CONFLICT (user_id)
        DO UPDATE SET prefs = EXCLUDED.prefs, updated_at = NOW()
    """, (user_id, json.dumps(merged)))

    return {"updated": True, "preferences": merged}


# ─────────────────────────── Export ───────────────────────────────────────────

@router.get("/export")
async def export_watchlist(
    format: str = Query("csv", description="Export format: 'csv' or 'excel'"),
    cur: cursor = Depends(get_db_cursor),
    current_user: dict = Depends(get_current_user)
):
    """Export watchlist activity data as CSV or Excel."""
    user_id = current_user["id"]
    curr_pid, prev_pid, curr_yr, curr_mo = _get_latest_two_periods(cur)
    month_label = datetime(curr_yr, curr_mo, 1).strftime("%b %Y") if curr_yr else "N/A"

    # Retrieve dashboard data for export
    cur.execute("""
        SELECT
            CASE WHEN w.asset_type='stock' THEN COALESCE(ce.canonical_name, c.company_name)
                 ELSE s.scheme_name END AS name,
            w.asset_type,
            c.isin,
            c.sector,
            s.scheme_id
        FROM user_watchlist w
        LEFT JOIN companies c ON w.company_id = c.company_id
        LEFT JOIN corporate_entities ce ON c.entity_id = ce.entity_id
        LEFT JOIN schemes s ON w.scheme_id = s.scheme_id
        WHERE w.user_id = %s
        ORDER BY w.asset_type, name
    """, (user_id,))
    rows = cur.fetchall()

    headers = ["Name", "Type", "ISIN / Scheme ID", "Sector", "Period"]
    data    = []
    for row in rows:
        name, atype, isin, sector, sid = row
        identifier = isin if atype == "stock" else (str(sid) if sid else "")
        data.append([name, atype.title(), identifier, sector or "", month_label])

    if format == "excel":
        try:
            import openpyxl
            wb  = openpyxl.Workbook()
            ws  = wb.active
            ws.title = "Watchlist"
            ws.append(headers)
            for row in data:
                ws.append(row)
            buf = io.BytesIO()
            wb.save(buf)
            buf.seek(0)
            return StreamingResponse(
                buf,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": "attachment; filename=watchlist_export.xlsx"}
            )
        except ImportError:
            logger.warning("openpyxl not installed, falling back to CSV")

    # Default: CSV
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(headers)
    writer.writerows(data)
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=watchlist_export.csv"}
    )
