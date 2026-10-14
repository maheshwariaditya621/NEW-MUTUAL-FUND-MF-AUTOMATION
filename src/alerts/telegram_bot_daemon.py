"""
Interactive Telegram Bot Daemon for AV Fincorp.

Provides interactive administrative and investment intelligence commands:
- /status   : Real-time server health, memory, disk, and systemd services
- /pipeline : Latest monthly pipeline disclosure progress across all 50 AMCs
- /backup   : Trigger instant PostgreSQL dump + Google Drive offsite sync
- /stock    : Deep mutual fund ownership analytics for any stock (e.g. /stock INFY)
- /scheme   : Scheme summary, AUM, and top stock holdings (e.g. /scheme Parag Parikh)
- /help     : Command directory and quick guides
"""

import os
import sys
import time
import shutil
import subprocess
import requests
import psycopg2
from typing import Dict, Any, Optional

# Ensure project root is in path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.config.telegram_config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from src.config import logger

AUTHORIZED_CHAT_ID = int(TELEGRAM_CHAT_ID or "1308792142")
BOT_TOKEN = TELEGRAM_BOT_TOKEN
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"


def send_reply(chat_id: int, text: str, parse_mode: str = "HTML"):
    """Sends a message back to the Telegram chat."""
    try:
        requests.post(
            f"{API_URL}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": parse_mode},
            timeout=10
        )
    except Exception as e:
        logger.error(f"Failed to send Telegram reply: {e}")


def get_db_connection():
    """Returns a direct psycopg2 connection."""
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME", "mf_analytics"),
        user=os.getenv("DB_USER", "mf_admin"),
        password=os.getenv("DB_PASSWORD", "MFapp@2026!"),
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432"))
    )


# ============================================================================
# COMMAND HANDLERS
# ============================================================================

def handle_start(chat_id: int):
    msg = (
        "👋 <b>Welcome to AV Fincorp Bot!</b>\n\n"
        "Your private operational & investment intelligence assistant.\n\n"
        "<b>Available Commands:</b>\n"
        "📊 <code>/status</code> — Server health, RAM, Disk & Services\n"
        "🏢 <code>/pipeline</code> — Monthly AMC disclosure progress\n"
        "💾 <code>/backup</code> — Trigger instant DB backup to Google Drive\n"
        "📈 <code>/stock &lt;symbol&gt;</code> — Mutual fund ownership (e.g. <code>/stock INFY</code>)\n"
        "📑 <code>/scheme &lt;name&gt;</code> — Fund holdings & AUM (e.g. <code>/scheme Parag Parikh</code>)\n"
        "❓ <code>/help</code> — Show this guide"
    )
    send_reply(chat_id, msg)


def handle_status(chat_id: int):
    # 1. RAM
    try:
        with open("/proc/meminfo") as f:
            mem = {l.split(":")[0]: int(l.split(":")[1].split()[0]) for l in f}
        total_ram = mem.get("MemTotal", 1) / (1024 * 1024)
        avail_ram = mem.get("MemAvailable", 0) / (1024 * 1024)
        used_ram = total_ram - avail_ram
        ram_pct = (used_ram / total_ram) * 100
        ram_str = f"{used_ram:.1f} GB / {total_ram:.1f} GB ({ram_pct:.0f}%)"
    except Exception:
        ram_str = "N/A"

    # 2. Disk
    disk = shutil.disk_usage("/")
    used_disk = disk.used / (1024 ** 3)
    total_disk = disk.total / (1024 ** 3)
    disk_pct = (disk.used / disk.total) * 100
    disk_str = f"{used_disk:.1f} GB / {total_disk:.1f} GB ({disk_pct:.0f}%)"

    # 3. Services
    def check_service(name):
        r = subprocess.run(["systemctl", "is-active", name], capture_output=True, text=True)
        return "🟢 Active" if r.stdout.strip() == "active" else f"🔴 {r.stdout.strip()}"

    api_st = check_service("mf-api")
    ann_st = check_service("mf-announcements")
    timer_st = check_service("mf-pipeline.timer")

    # 4. DB
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM equity_holdings;")
        cnt = cur.fetchone()[0]
        cur.close()
        conn.close()
        db_str = f"🟢 Connected ({cnt:,} holdings)"
    except Exception as e:
        db_str = f"🔴 Disconnected ({str(e)[:30]})"

    msg = (
        "📊 <b>AV Fincorp Server Status</b>\n\n"
        f"🖥️ <b>Host</b>: Oracle Always Free (Mumbai)\n"
        f"🧠 <b>Memory (RAM)</b>: {ram_str}\n"
        f"💾 <b>Disk Space</b>: {disk_str}\n"
        f"🗄️ <b>PostgreSQL</b>: {db_str}\n\n"
        f"<b>Background Services:</b>\n"
        f"• FastAPI Server: {api_st}\n"
        f"• Announcements Worker: {ann_st}\n"
        f"• Monthly Pipeline Timer: {timer_st}\n\n"
        f"🌐 <b>Live Web</b>: <a href='https://avfincorp.com'>avfincorp.com</a>"
    )
    send_reply(chat_id, msg)


def handle_backup(chat_id: int):
    send_reply(chat_id, "⏳ <b>Starting Backup...</b>\nCreating PostgreSQL snapshot and syncing to Google Drive...")
    try:
        res = subprocess.run(
            ["sudo", "-u", "postgres", "bash", "/home/ubuntu/backups/db_backup.sh"],
            capture_output=True,
            text=True,
            timeout=120
        )
        if res.returncode == 0:
            msg = (
                "✅ <b>Backup Successful!</b>\n\n"
                "• Full PostgreSQL database dump generated.\n"
                "• Local 7-day snapshot preserved.\n"
                "• <b>Synced to Google Drive</b> in <code>mf-backups/</code> ☁️"
            )
        else:
            msg = f"❌ <b>Backup Failed:</b>\n<code>{res.stderr[:200]}</code>"
    except Exception as e:
        msg = f"❌ <b>Backup Exception:</b>\n<code>{str(e)[:200]}</code>"
    send_reply(chat_id, msg)


def handle_pipeline(chat_id: int):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Query latest pipeline run
        cur.execute("""
            SELECT run_id, target_year, target_month, status, success_count, failed_count, skipped_count, start_time
            FROM pipeline_runs
            ORDER BY start_time DESC
            LIMIT 1;
        """)
        run = cur.fetchone()
        if not run:
            send_reply(chat_id, "ℹ️ No pipeline runs recorded yet.")
            return

        run_id, y, m, status, succ, fail, skip, started = run

        cur.execute("""
            SELECT COUNT(*) FROM schemes;
        """)
        total_schemes = cur.fetchone()[0]

        cur.close()
        conn.close()

        msg = (
            f"🏢 <b>Latest Monthly Pipeline Run</b>\n\n"
            f"🆔 <b>Run ID</b>: <code>{run_id}</code>\n"
            f"📅 <b>Target Period</b>: {y}-{m:02d}\n"
            f"📊 <b>Status</b>: <b>{status}</b>\n"
            f"✅ <b>Succeeded AMCs</b>: {succ}\n"
            f"⏸️ <b>Skipped AMCs</b>: {skip}\n"
            f"❌ <b>Failed AMCs</b>: {fail}\n"
            f"📚 <b>Total Monitored Schemes</b>: {total_schemes:,}\n\n"
            f"💡 <i>Scheduled automated monthly execution runs on 7th–12th.</i>"
        )
        send_reply(chat_id, msg)
    except Exception as e:
        send_reply(chat_id, f"❌ Error retrieving pipeline status: {str(e)[:100]}")


def handle_stock(chat_id: int, query: str):
    if not query:
        send_reply(chat_id, "ℹ️ Please provide a stock symbol or name. Example: <code>/stock INFY</code> or <code>/stock RELIANCE</code>")
        return

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Find matching company
        cur.execute("""
            SELECT c.company_id, c.company_name, c.nse_symbol, c.sector,
                   COUNT(DISTINCT s.scheme_id) as scheme_count,
                   ROUND(SUM(eh.market_value_inr) / 10000000.0, 2) as total_crores
            FROM companies c
            JOIN equity_holdings eh ON c.company_id = eh.company_id
            JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
            JOIN schemes s ON ss.scheme_id = s.scheme_id
            WHERE c.nse_symbol ILIKE %s OR c.company_name ILIKE %s
            GROUP BY c.company_id, c.company_name, c.nse_symbol, c.sector
            ORDER BY total_crores DESC
            LIMIT 1;
        """, (query.strip(), f"%{query.strip()}%"))

        stock = cur.fetchone()
        if not stock:
            send_reply(chat_id, f"🔍 No mutual fund holdings found for stock matching '<b>{query}</b>'.")
            cur.close()
            conn.close()
            return

        cid, name, symbol, sector, scheme_count, cr_val = stock
        cr_val = cr_val or 0.0

        # Top 5 funds holding this stock
        cur.execute("""
            SELECT s.scheme_name, a.amc_name, COALESCE(eh.percent_of_nav, 0.0), ROUND(COALESCE(eh.market_value_inr, 0) / 10000000.0, 2) as val_cr
            FROM equity_holdings eh
            JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
            JOIN schemes s ON ss.scheme_id = s.scheme_id
            JOIN amcs a ON s.amc_id = a.amc_id
            WHERE eh.company_id = %s
            ORDER BY eh.percent_of_nav DESC NULLS LAST
            LIMIT 5;
        """, (cid,))

        top_funds = cur.fetchall()
        cur.close()
        conn.close()

        lines = [
            f"📈 <b>{name}</b> ({symbol or 'N/A'})\n",
            f"🏷️ <b>Sector</b>: {sector or 'Unclassified'}",
            f"🏢 <b>Total Holding Schemes</b>: {scheme_count:,}",
            f"💰 <b>Total MF Ownership</b>: <b>₹{cr_val:,.2f} Cr</b>\n",
            "<b>Top 5 Fund Allocations:</b>"
        ]

        for i, (sname, amc, pct, val) in enumerate(top_funds, 1):
            pct_val = pct or 0.0
            v_val = val or 0.0
            lines.append(f"{i}. <b>{sname}</b> ({amc})\n   └ <b>{pct_val:.2f}%</b> of NAV (~₹{v_val:,.2f} Cr)")

        send_reply(chat_id, "\n".join(lines))

    except Exception as e:
        send_reply(chat_id, f"❌ Error querying stock: {str(e)[:100]}")


def handle_scheme(chat_id: int, query: str):
    if not query:
        send_reply(chat_id, "ℹ️ Please provide a scheme name. Example: <code>/scheme Parag Parikh</code> or <code>/scheme HDFC Top 100</code>")
        return

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Find scheme
        cur.execute("""
            SELECT s.scheme_id, s.scheme_name, a.amc_name, s.plan_type, s.option_type
            FROM schemes s
            JOIN amcs a ON s.amc_id = a.amc_id
            WHERE s.scheme_name ILIKE %s
            ORDER BY s.scheme_id
            LIMIT 1;
        """, (f"%{query.strip()}%",))

        scheme = cur.fetchone()
        if not scheme:
            send_reply(chat_id, f"🔍 No scheme found matching '<b>{query}</b>'.")
            cur.close()
            conn.close()
            return

        sid, sname, amc, plan, opt = scheme

        # Get latest snapshot
        cur.execute("""
            SELECT ss.snapshot_id, p.year, p.month, 
                   ROUND(COALESCE(ss.total_equity_value_inr, 0) / 10000000.0, 2) as equity_cr,
                   ROUND(COALESCE(ss.net_assets_inr, 0) / 10000000.0, 2) as nav_cr,
                   COALESCE(ss.holdings_count, 0)
            FROM scheme_snapshots ss
            JOIN periods p ON ss.period_id = p.period_id
            WHERE ss.scheme_id = %s
            ORDER BY p.year DESC, p.month DESC
            LIMIT 1;
        """, (sid,))

        snap = cur.fetchone()
        if not snap:
            send_reply(chat_id, f"📑 <b>{sname}</b>\nAMC: {amc}\nNo snapshot data available.")
            cur.close()
            conn.close()
            return

        snap_id, yr, mo, eq_cr, nav_cr, h_count = snap
        eq_cr = eq_cr or 0.0

        # Top 5 holdings in this snapshot
        cur.execute("""
            SELECT c.company_name, c.nse_symbol, COALESCE(eh.percent_of_nav, 0.0), ROUND(COALESCE(eh.market_value_inr, 0) / 10000000.0, 2) as val_cr
            FROM equity_holdings eh
            JOIN companies c ON eh.company_id = c.company_id
            WHERE eh.snapshot_id = %s
            ORDER BY eh.percent_of_nav DESC NULLS LAST
            LIMIT 5;
        """, (snap_id,))

        holdings = cur.fetchall()
        cur.close()
        conn.close()

        lines = [
            f"📑 <b>{sname}</b>",
            f"🏢 <b>AMC</b>: {amc} ({plan} / {opt})",
            f"📅 <b>Latest Portfolio</b>: {yr}-{mo:02d}",
            f"💰 <b>Total Equity AUM</b>: <b>₹{eq_cr:,.2f} Cr</b> (Holdings: {h_count})\n",
            "<b>Top 5 Portfolio Holdings:</b>"
        ]

        for i, (cname, nse, pct, val) in enumerate(holdings, 1):
            sym = f" ({nse})" if nse else ""
            pct_val = pct or 0.0
            v_val = val or 0.0
            lines.append(f"{i}. <b>{cname}</b>{sym}: <b>{pct_val:.2f}%</b> (~₹{v_val:,.2f} Cr)")

        send_reply(chat_id, "\n".join(lines))

    except Exception as e:
        send_reply(chat_id, f"❌ Error querying scheme: {str(e)[:100]}")


# ============================================================================
# MAIN BOT POLLING LOOP
# ============================================================================

def process_update(update: Dict[str, Any]):
    """Dispatches incoming message to appropriate handler."""
    msg = update.get("message")
    if not msg or "text" not in msg:
        return

    chat_id = msg.get("chat", {}).get("id")
    text = msg.get("text", "").strip()

    # Security check: only authorized admin can issue commands
    if chat_id != AUTHORIZED_CHAT_ID:
        send_reply(chat_id, "⛔ <b>Unauthorized.</b> This bot is restricted to AV Fincorp administrators.")
        return

    parts = text.split(maxsplit=1)
    command = parts[0].lower().split("@")[0]
    arg = parts[1] if len(parts) > 1 else ""

    if command in ("/start", "/help"):
        handle_start(chat_id)
    elif command == "/status":
        handle_status(chat_id)
    elif command == "/backup":
        handle_backup(chat_id)
    elif command == "/pipeline":
        handle_pipeline(chat_id)
    elif command == "/stock":
        handle_stock(chat_id, arg)
    elif command == "/scheme":
        handle_scheme(chat_id, arg)
    else:
        send_reply(chat_id, "❓ Unknown command. Send <code>/help</code> to view available commands.")


def run_bot():
    """Long-polling daemon loop."""
    logger.info("🤖 Starting AV Fincorp Telegram Bot Daemon...")
    offset = 0

    while True:
        try:
            r = requests.get(
                f"{API_URL}/getUpdates",
                params={"offset": offset, "timeout": 30},
                timeout=35
            )
            if r.status_code == 200:
                data = r.json()
                if data.get("ok"):
                    for update in data.get("result", []):
                        offset = update["update_id"] + 1
                        process_update(update)
            else:
                time.sleep(2)
        except Exception as e:
            logger.error(f"Telegram polling error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    run_bot()
