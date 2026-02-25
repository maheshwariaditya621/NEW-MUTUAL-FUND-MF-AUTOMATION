"""
AI Chatbot endpoint using Groq (free tier) with Llama-3.3-70b.

Uses Groq's OpenAI-compatible chat completions API with parallel tool calling.
100% free, no billing required.
"""

import os
import json
import calendar
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from psycopg2.extensions import cursor
from groq import Groq

from src.api.dependencies import get_db_cursor
from src.config import logger

router = APIRouter()

# ──────────────────────────────────────────────────────────────────
# Configure Groq client
# ──────────────────────────────────────────────────────────────────
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
_groq_client = None

def _get_groq_client() -> Groq:
    global _groq_client
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY not set in .env")
    if _groq_client is None:
        _groq_client = Groq(api_key=GROQ_API_KEY)
    return _groq_client


# ──────────────────────────────────────────────────────────────────
# Request / Response models
# ──────────────────────────────────────────────────────────────────
class ChatRequest(BaseModel):
    message: str


class ChatResponse(BaseModel):
    reply: str


# ──────────────────────────────────────────────────────────────────
# Tool Definitions (OpenAI-compatible format)
# ──────────────────────────────────────────────────────────────────
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_company_info",
            "description": (
                "Get information about a company/stock including market cap, shares outstanding, sector, ISIN, "
                "and NSE symbol. Use when the user asks about a company's details, "
                "shares held, market cap, sector, or stock symbol."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "company_name": {
                        "type": "string",
                        "description": "Company name or NSE/BSE symbol, e.g. 'Reliance', 'INFY', 'TCS'"
                    }
                },
                "required": ["company_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_scheme_holdings_of_stock",
            "description": (
                "Get how many shares of a specific stock a mutual fund scheme holds as of the latest month. "
                "Use when asked e.g. 'How many shares of Reliance does HDFC Arbitrage Fund hold?'"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "scheme_name": {
                        "type": "string",
                        "description": "Mutual fund scheme name e.g. 'HDFC Arbitrage Fund'"
                    },
                    "company_name": {
                        "type": "string",
                        "description": "Company name or NSE/BSE symbol, e.g. 'Reliance', 'INFY'"
                    }
                },
                "required": ["scheme_name", "company_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_top_holdings",
            "description": (
                "Get the top N equity holdings of a mutual fund scheme for the latest month. "
                "Use when asked 'What are the top stocks in X fund?' or 'What does X fund invest in?'"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "scheme_name": {
                        "type": "string",
                        "description": "Mutual fund scheme name"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of top holdings to return (default 5, max 10)"
                    }
                },
                "required": ["scheme_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_funds_holding_stock",
            "description": (
                "Get which mutual funds hold a specific stock, sorted by % to NAV. "
                "Use when asked 'Which funds hold Reliance?' or 'Who is buying Infosys?'"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "company_name": {
                        "type": "string",
                        "description": "Company name or NSE/BSE symbol, e.g. 'Reliance', 'TCS', 'INFY'"
                    }
                },
                "required": ["company_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_amc_holdings_of_stock",
            "description": (
                "Get the total shares of a stock held by ALL schemes of a specific AMC (aggregated). "
                "Use when asked 'How much Reliance does HDFC Mutual Fund hold in total?'"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "amc_name": {
                        "type": "string",
                        "description": "AMC name e.g. 'HDFC', 'ICICI Prudential', 'SBI'"
                    },
                    "company_name": {
                        "type": "string",
                        "description": "Company name or NSE/BSE symbol"
                    }
                },
                "required": ["amc_name", "company_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_industry_total_stake",
            "description": (
                "Get the total ownership percentage (%) and shares held by the entire Mutual Fund industry in a company. "
                "Use when asked 'What is the total MF stake in Reliance?' or 'Which company has highest MF holding?'"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "company_name": {
                        "type": "string",
                        "description": "Company name or NSE/BSE symbol"
                    }
                },
                "required": ["company_name"]
            }
        }
    },
]


# ──────────────────────────────────────────────────────────────────
# Tool executor functions (DB queries)
# ──────────────────────────────────────────────────────────────────

def _get_company_info(company_name: str, cur: cursor) -> dict:
    try:
        cur.execute(
            """
            SELECT company_name, isin, sector, nse_symbol, bse_code, market_cap, mcap_type, mcap_updated_at, shares_outstanding, shares_last_updated_at
            FROM companies
            WHERE company_name ILIKE %s OR nse_symbol = UPPER(%s) OR bse_code = %s
            ORDER BY
                CASE WHEN UPPER(company_name) = UPPER(%s) THEN 1
                     WHEN nse_symbol = UPPER(%s) THEN 2
                     WHEN company_name ILIKE %s THEN 3 ELSE 4 END
            LIMIT 1
            """,
            (f"%{company_name}%", company_name, company_name, company_name, company_name, company_name + "%")
        )
        row = cur.fetchone()
        if not row:
            return {"found": False, "message": f"No company found matching '{company_name}'."}
        name, isin, sector, nse, bse, mcap, mcap_type, mcap_updated_at, shares, shares_updated_at = row
        return {
            "found": True,
            "company_name": name,
            "isin": isin,
            "sector": sector or "Unknown",
            "nse_symbol": nse or "N/A",
            "bse_code": bse or "N/A",
            "market_cap_crore": round(mcap / 10_000_000, 2) if mcap else None,
            "market_cap_type": mcap_type or "N/A",
            "market_cap_updated_at": mcap_updated_at.strftime("%d-%b-%Y") if mcap_updated_at else "N/A",
            "shares_outstanding": shares if shares else None,
            "shares_last_updated_at": shares_updated_at.strftime("%d-%b-%Y") if shares_updated_at else "N/A",
        }
    except Exception as e:
        logger.error(f"_get_company_info error: {e}")
        return {"found": False, "message": str(e)}


def _get_scheme_holdings_of_stock(scheme_name: str, company_name: str, cur: cursor) -> dict:
    try:
        cur.execute(
            """
            SELECT company_id, company_name FROM companies 
            WHERE company_name ILIKE %s OR nse_symbol = UPPER(%s) OR bse_code = %s
            ORDER BY CASE WHEN nse_symbol = UPPER(%s) THEN 1 ELSE 2 END, company_name 
            LIMIT 1
            """,
            (f"%{company_name}%", company_name, company_name, company_name)
        )
        comp = cur.fetchone()
        if not comp:
            return {"found": False, "message": f"No company found matching '{company_name}'."}
        company_id, resolved_company = comp

        cur.execute(
            """
            SELECT s.scheme_id, s.scheme_name, a.amc_name, s.plan_type, s.option_type
            FROM schemes s JOIN amcs a ON s.amc_id = a.amc_id
            WHERE s.scheme_name ILIKE %s
            ORDER BY CASE WHEN UPPER(s.scheme_name) = UPPER(%s) THEN 1
                          WHEN s.scheme_name ILIKE %s THEN 2 ELSE 3 END LIMIT 1
            """,
            (f"%{scheme_name}%", scheme_name, scheme_name + "%")
        )
        scheme = cur.fetchone()
        if not scheme:
            return {"found": False, "message": f"No scheme found matching '{scheme_name}'."}
        scheme_id, resolved_scheme, amc_name, plan_type, option_type = scheme

        cur.execute("SELECT period_id, year, month FROM periods ORDER BY year DESC, month DESC LIMIT 1")
        period = cur.fetchone()
        if not period:
            return {"found": False, "message": "No period data in database."}
        period_id, year, month = period

        cur.execute(
            """
            SELECT eh.quantity, eh.percent_of_nav, eh.market_value_inr / 10000000.0
            FROM equity_holdings eh
            JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
            WHERE ss.scheme_id = %s AND ss.period_id = %s AND eh.company_id = %s LIMIT 1
            """,
            (scheme_id, period_id, company_id)
        )
        holding = cur.fetchone()
        from datetime import date
        month_label = date(year, month, 1).strftime("%B %Y")

        if not holding:
            return {
                "found": True, "data_available": False,
                "scheme_name": resolved_scheme, "amc_name": amc_name,
                "company_name": resolved_company, "as_of": month_label,
                "message": f"{resolved_scheme} does NOT hold {resolved_company} as of {month_label}."
            }
        qty, pnav, value_cr = holding
        
        # Ownership Stake % Calculation
        cur.execute("SELECT shares_outstanding FROM companies WHERE company_id = %s", (company_id,))
        total_shares = cur.fetchone()[0]
        ownership_stake = (float(qty) / float(total_shares) * 100.0) if total_shares and total_shares > 0 else None

        return {
            "found": True, "data_available": True,
            "scheme_name": resolved_scheme, "amc_name": amc_name,
            "plan_type": plan_type, "option_type": option_type,
            "company_name": resolved_company, "as_of": month_label,
            "quantity_shares": int(qty),
            "percent_to_nav": round(float(pnav), 4),
            "market_value_crore": round(float(value_cr), 2),
            "ownership_stake_percent": round(ownership_stake, 4) if ownership_stake else "N/A",
            "total_company_shares": total_shares
        }
    except Exception as e:
        logger.error(f"_get_scheme_holdings_of_stock error: {e}")
        return {"found": False, "message": str(e)}


def _get_top_holdings(scheme_name: str, limit: int, cur: cursor) -> dict:
    try:
        cur.execute(
            """
            SELECT s.scheme_id, s.scheme_name, a.amc_name
            FROM schemes s JOIN amcs a ON s.amc_id = a.amc_id
            WHERE s.scheme_name ILIKE %s
            ORDER BY CASE WHEN UPPER(s.scheme_name) = UPPER(%s) THEN 1
                          WHEN s.scheme_name ILIKE %s THEN 2 ELSE 3 END LIMIT 1
            """,
            (f"%{scheme_name}%", scheme_name, scheme_name + "%")
        )
        scheme = cur.fetchone()
        if not scheme:
            return {"found": False, "message": f"No scheme found matching '{scheme_name}'."}
        scheme_id, resolved_scheme, amc_name = scheme

        cur.execute("SELECT period_id, year, month FROM periods ORDER BY year DESC, month DESC LIMIT 1")
        period = cur.fetchone()
        if not period:
            return {"found": False, "message": "No period data."}
        period_id, year, month = period

        cur.execute(
            """
            SELECT c.company_name, eh.quantity, eh.percent_of_nav,
                   eh.market_value_inr / 10000000.0, c.sector
            FROM equity_holdings eh
            JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
            JOIN companies c ON eh.company_id = c.company_id
            WHERE ss.scheme_id = %s AND ss.period_id = %s
            ORDER BY eh.percent_of_nav DESC LIMIT %s
            """,
            (scheme_id, period_id, limit)
        )
        rows = cur.fetchall()
        from datetime import date
        month_label = date(year, month, 1).strftime("%B %Y")

        if not rows:
            return {"found": True, "data_available": False, "scheme_name": resolved_scheme,
                    "as_of": month_label, "message": f"No holdings data for {resolved_scheme}."}

        return {
            "found": True, "data_available": True,
            "scheme_name": resolved_scheme, "amc_name": amc_name, "as_of": month_label,
            "top_holdings": [
                {"rank": i+1, "company": r[0], "quantity": int(r[1]),
                 "percent_to_nav": round(float(r[2]), 2),
                 "value_crore": round(float(r[3]), 2), "sector": r[4] or "Unknown"}
                for i, r in enumerate(rows)
            ]
        }
    except Exception as e:
        logger.error(f"_get_top_holdings error: {e}")
        return {"found": False, "message": str(e)}


def _get_funds_holding_stock(company_name: str, cur: cursor) -> dict:
    try:
        cur.execute(
            """
            SELECT company_id, company_name FROM companies 
            WHERE company_name ILIKE %s OR nse_symbol = UPPER(%s) OR bse_code = %s
            ORDER BY CASE WHEN nse_symbol = UPPER(%s) THEN 1 ELSE 2 END, company_name 
            LIMIT 1
            """,
            (f"%{company_name}%", company_name, company_name, company_name)
        )
        comp = cur.fetchone()
        if not comp:
            return {"found": False, "message": f"No company found matching '{company_name}'."}
        company_id, resolved_company = comp

        cur.execute("SELECT period_id, year, month FROM periods ORDER BY year DESC, month DESC LIMIT 1")
        period = cur.fetchone()
        if not period:
            return {"found": False, "message": "No period data."}
        period_id, year, month = period

        cur.execute(
            """
            SELECT s.scheme_name, a.amc_name, eh.quantity, eh.percent_of_nav,
                   eh.market_value_inr / 10000000.0
            FROM equity_holdings eh
            JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
            JOIN schemes s ON ss.scheme_id = s.scheme_id
            JOIN amcs a ON s.amc_id = a.amc_id
            WHERE eh.company_id = %s AND ss.period_id = %s
            ORDER BY eh.percent_of_nav DESC LIMIT 20
            """,
            (company_id, period_id)
        )
        rows = cur.fetchall()
        from datetime import date
        month_label = date(year, month, 1).strftime("%B %Y")

        if not rows:
            return {"found": True, "data_available": False, "company_name": resolved_company,
                    "as_of": month_label,
                    "message": f"No mutual fund holds {resolved_company} as of {month_label}."}
        
        # Get total shares for stake calculation
        cur.execute("SELECT shares_outstanding FROM companies WHERE company_id = %s", (company_id,))
        total_shares = cur.fetchone()[0]

        return {
            "found": True, "data_available": True,
            "company_name": resolved_company, "as_of": month_label, "total_funds": len(rows),
            "total_company_shares": total_shares,
            "funds": [
                {
                    "rank": i+1, "scheme_name": r[0], "amc": r[1], "quantity": int(r[2]),
                    "percent_to_nav": round(float(r[3]), 2), "value_crore": round(float(r[4]), 2),
                    "ownership_stake_percent": round((float(r[2]) / float(total_shares) * 100.0), 4) if total_shares and total_shares > 0 else "N/A"
                }
                for i, r in enumerate(rows)
            ]
        }
    except Exception as e:
        logger.error(f"_get_funds_holding_stock error: {e}")
        return {"found": False, "message": str(e)}


def _get_amc_holdings_of_stock(amc_name: str, company_name: str, cur: cursor) -> dict:
    try:
        cur.execute(
            """
            SELECT company_id, company_name, shares_outstanding FROM companies 
            WHERE company_name ILIKE %s OR nse_symbol = UPPER(%s) OR bse_code = %s
            ORDER BY CASE WHEN nse_symbol = UPPER(%s) THEN 1 ELSE 2 END, company_name LIMIT 1
            """,
            (f"%{company_name}%", company_name, company_name, company_name)
        )
        comp = cur.fetchone()
        if not comp: return {"found": False, "message": f"No company matching '{company_name}'."}
        company_id, resolved_company, total_shares = comp

        cur.execute("SELECT amc_id, amc_name FROM amcs WHERE amc_name ILIKE %s ORDER BY length(amc_name) LIMIT 1", (f"%{amc_name}%",))
        amc = cur.fetchone()
        if not amc: return {"found": False, "message": f"No AMC found matching '{amc_name}'."}
        amc_id, resolved_amc = amc

        cur.execute("SELECT period_id, year, month FROM periods ORDER BY year DESC, month DESC LIMIT 1")
        period_id, year, month = cur.fetchone()
        month_label = f"{calendar.month_name[month]} {year}"

        cur.execute(
            """
            SELECT SUM(eh.quantity), SUM(eh.market_value_inr) / 10000000.0, COUNT(DISTINCT s.scheme_id)
            FROM equity_holdings eh
            JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
            JOIN schemes s ON ss.scheme_id = s.scheme_id
            WHERE s.amc_id = %s AND ss.period_id = %s AND eh.company_id = %s
            """,
            (amc_id, period_id, company_id)
        )
        agg = cur.fetchone()
        if not agg or not agg[0]:
            return {"found": True, "data_available": False, "amc": resolved_amc, "company": resolved_company, "as_of": month_label}

        total_qty, total_val_cr, scheme_count = agg
        stake_pct = (float(total_qty) / float(total_shares) * 100.0) if total_shares and total_shares > 0 else None

        return {
            "found": True, "data_available": True,
            "amc_name": resolved_amc, "company_name": resolved_company, "as_of": month_label,
            "total_quantity_shares": int(total_qty),
            "total_value_crore": round(float(total_val_cr), 2),
            "schemes_holding_count": scheme_count,
            "ownership_stake_percent": round(stake_pct, 4) if stake_pct else "N/A"
        }
    except Exception as e:
        logger.error(f"_get_amc_holdings_of_stock error: {e}")
        return {"found": False, "message": str(e)}

def _get_industry_total_stake(company_name: str, cur: cursor) -> dict:
    try:
        cur.execute(
            """
            SELECT company_id, company_name, shares_outstanding FROM companies 
            WHERE company_name ILIKE %s OR nse_symbol = UPPER(%s) OR bse_code = %s
            ORDER BY CASE WHEN nse_symbol = UPPER(%s) THEN 1 ELSE 2 END, company_name LIMIT 1
            """,
            (f"%{company_name}%", company_name, company_name, company_name)
        )
        comp = cur.fetchone()
        if not comp: return {"found": False, "message": f"No company matching '{company_name}'."}
        company_id, resolved_company, total_shares = comp

        cur.execute("SELECT period_id, year, month FROM periods ORDER BY year DESC, month DESC LIMIT 1")
        period_id, year, month = cur.fetchone()
        month_label = f"{calendar.month_name[month]} {year}"

        cur.execute(
            """
            SELECT SUM(eh.quantity), SUM(eh.market_value_inr) / 10000000.0, COUNT(DISTINCT s.amc_id), COUNT(DISTINCT s.scheme_id)
            FROM equity_holdings eh
            JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
            JOIN schemes s ON ss.scheme_id = s.scheme_id
            WHERE ss.period_id = %s AND eh.company_id = %s
            """,
            (period_id, company_id)
        )
        agg = cur.fetchone()
        if not agg or not agg[0]:
            return {"found": True, "data_available": False, "company": resolved_company, "as_of": month_label}

        total_qty, total_val_cr, amc_count, scheme_count = agg
        stake_pct = (float(total_qty) / float(total_shares) * 100.0) if total_shares and total_shares > 0 else None

        return {
            "found": True, "data_available": True,
            "company_name": resolved_company, "as_of": month_label,
            "industry_total_shares": int(total_qty),
            "industry_total_value_crore": round(float(total_val_cr), 2),
            "amcs_holding_count": amc_count,
            "schemes_holding_count": scheme_count,
            "ownership_stake_percent": round(stake_pct, 4) if stake_pct else "N/A",
            "total_company_shares": total_shares
        }
    except Exception as e:
        logger.error(f"_get_industry_total_stake error: {e}")
        return {"found": False, "message": str(e)}

def _dispatch_tool(fn_name: str, fn_args: dict, cur: cursor) -> dict:
    if fn_name == "get_company_info":
        return _get_company_info(fn_args.get("company_name", ""), cur)
    elif fn_name == "get_scheme_holdings_of_stock":
        return _get_scheme_holdings_of_stock(
            fn_args.get("scheme_name", ""), fn_args.get("company_name", ""), cur)
    elif fn_name == "get_top_holdings":
        return _get_top_holdings(fn_args.get("scheme_name", ""), int(fn_args.get("limit", 5)), cur)
    elif fn_name == "get_funds_holding_stock":
        return _get_funds_holding_stock(fn_args.get("company_name", ""), cur)
    elif fn_name == "get_amc_holdings_of_stock":
        return _get_amc_holdings_of_stock(fn_args.get("amc_name", ""), fn_args.get("company_name", ""), cur)
    elif fn_name == "get_industry_total_stake":
        return _get_industry_total_stake(fn_args.get("company_name", ""), cur)
    return {"error": f"Unknown tool: {fn_name}"}


# ──────────────────────────────────────────────────────────────────
# System prompt
# ──────────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are a helpful mutual fund assistant. 
Use tools to answer questions about holdings, market caps, and shares outstanding.
You can search for companies using their FULL NAME or their NSE/BSE SYMBOLS (e.g., 'INFY', 'RELIANCE').
When providing data, focus on "Ownership %" or "Stake %" (which is Fund Shares / Total Shares) when relevant.
Always mention the "last updated date" for market caps or shares.
If data is not found, say so. Format numbers nicely (e.g., use "Crores" for values, and commas for large share counts).
DO NOT use XML tags or special function-calling syntax in your response content."""


# ──────────────────────────────────────────────────────────────────
# Main chat endpoint
# ──────────────────────────────────────────────────────────────────

@router.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest, cur: cursor = Depends(get_db_cursor)):
    """
    AI Chatbot endpoint powered by Groq (free tier).
    Uses Llama-3.3-70b with tool calling to query live DB data.
    """
    if not GROQ_API_KEY:
        raise HTTPException(
            status_code=500,
            detail="GROQ_API_KEY not configured. Please add it to your .env file."
        )

    if not request.message or not request.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty.")

    try:
        client = _get_groq_client()
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": request.message.strip()},
        ]

        # Agentic loop: keep calling until no more tool calls
        max_iterations = 5
        for _ in range(max_iterations):
            response = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=messages,
                tools=TOOLS,
                tool_choice="auto",
                temperature=0,
                max_tokens=2048,
            )

            msg = response.choices[0].message
            messages.append(msg)  # append assistant message

            # If no tool calls, we have the final answer
            if not msg.tool_calls:
                break

            # Execute all tool calls and append results
            for tc in msg.tool_calls:
                fn_name = tc.function.name
                fn_args = json.loads(tc.function.arguments)
                logger.info(f"[Chatbot] Tool: {fn_name}({fn_args})")

                result = _dispatch_tool(fn_name, fn_args, cur)
                logger.info(f"[Chatbot] Result: {result}")

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": json.dumps(result, ensure_ascii=False),
                })

        final_text = response.choices[0].message.content or \
            "I'm sorry, I couldn't generate a response. Please try again."

        return ChatResponse(reply=final_text)

    except Exception as e:
        logger.error(f"Chatbot error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        err_str = str(e)
        if "429" in err_str or "rate_limit" in err_str.lower():
            raise HTTPException(
                status_code=429,
                detail="Rate limit reached. Please wait a moment and try again."
            )
        raise HTTPException(status_code=500, detail=f"Chatbot error: {str(e)}")
