(Authoritative decision log + onboarding guide for downloader layer)

1. Core Philosophy (DO NOT VIOLATE)

This system is NOT:

a scraper

a one-time downloader

a data processor

This system IS:

an idempotent raw data ingestion pipeline

scheduler-first, not manual-first

atomic, not best-effort

designed to run forever without babysitting

Any change that weakens idempotency, atomicity, or isolation is wrong.

2. Downloader Scope (HARD BOUNDARY)

Downloader MUST ONLY:

Call AMC APIs

Download raw files

Store them under canonical paths

Create _SUCCESS.json on full success

Emit events (Telegram)

Downloader MUST NEVER:

Parse Excel

Validate portfolio content

Rename files

Standardize scheme names

Touch database

Touch extraction logic

Raw data means RAW.

3. Why _SUCCESS.json Exists (Non-Negotiable)

Folder existence ≠ success.

Failures explicitly designed against:

Network timeout mid-download

Partial files written

Process crash

API returning empty list temporarily

Rule
A month is complete ONLY IF _SUCCESS.json exists.

Anything else is corruption.

This enables:

Safe retries

Automatic recovery

Deterministic scheduler behavior

Extraction trust guarantees

4. Corruption Handling (Intentional Design)

If:

data/raw/{amc}/2025_03/
  ├── some.xlsx
  └── (NO _SUCCESS.json)


Then:

Folder is INVALID

Folder is MOVED, never merged

Re-downloaded from scratch

Never overwrite. Never patch.

5. Scheduler Design (Why it’s “Two-Stage”)

Scheduler always runs in AUTO MODE.

Each scheduled run:

Stage 1 – Scheduled Range Backfill (Optional)

Configurable (start_year, start_month) → (end_year, end_month)

Heals historical gaps

Runs every scheduled execution

Safe due to _SUCCESS.json

Stage 2 – Eligible Month Check (Always)

Compute latest eligible month (usually previous month)

Download only that month

If API says “not published” → log + exit

Never scan from 2010

This prevents:

Accidental mass downloads

Re-downloading history

Scheduler overload

6. Why API > Playwright (Final Decision)

Playwright was intentionally abandoned.

Reasons:

DOM volatility

Heavy resource usage

Unreliable for long-running schedulers

Hard failure debugging

API advantages:

Faster

Deterministic

Same endpoint used by AMC website

Clean file URLs

Rule:

Prefer API. Use browser automation only if no API exists.

7. Bulk Downloads Are NOT Scheduler Responsibility

Bulk downloads exist for:

Initial historical backfill

Manual repair

Scheduler:

Never loops from earliest year

Never mass-downloads

Never accepts CLI ranges

Strict separation enforced.

8. Telegram Alerts Philosophy

Telegram is:

Observability

NOT control flow

NOT error handling

Downloader emits events:

SUCCESS

WARNING

ERROR

NOT_PUBLISHED

SCHEDULER_SUMMARY

Notifier decides:

When to send

How verbose

Based on config

Downloader never formats Telegram messages.

9. File Count Is Observability Only

Expected:

~80–120 files per month (AMC dependent)

Rules:

< minimum → WARNING

maximum → WARNING

NEVER fail a download based on count

NEVER block _SUCCESS.json

Reason:

Scheme launches/closures

ETF churn

FMP lifecycle effects

10. What the NEXT Agent Should Do

Order of work:

Phase 1 – Add New AMCs (Downloader Only)

Follow checklist below exactly

No extraction

No DB

No validation

Phase 2 – Extraction Layer (Later)

Reads only _SUCCESS.json folders

Writes to data/processed/

Own completion markers

Phase 3 – Validation / Analytics

Strictly downstream

11. Absolute “Do Not Do” List

❌ Do not rename raw files
❌ Do not normalize scheme names
❌ Do not infer completion from folder existence
❌ Do not skip _SUCCESS.json
❌ Do not merge partial folders
❌ Do not hardcode year ranges
❌ Do not mix extraction into downloader

12. Mental Model for Any Agent

“Downloader produces immutable facts.
Everything else is interpretation.”

13. AMC ONBOARDING CHECKLIST (DOWNLOADER ONLY) ✅

This section is mandatory for every new AMC (ICICI, SBI, Axis, etc.)

Step 1: Identify Data Source

☐ Locate official AMC portfolio disclosure page

☐ Open DevTools → Network tab

☐ Trigger month change

☐ Confirm API endpoint exists

☐ Capture:

URL

Method (POST/GET)

Payload

Headers

Response schema

🚫 If only HTML exists → escalate before coding

Step 2: Create AMC Namespace

☐ Folder: data/raw/{amc_name}/

☐ Corrupt folder: data/raw/{amc_name}/_corrupt/

☐ Config entry added (no hardcoding)

Naming rule:

{amc_name} = lowercase, no spaces


Example:

hdfc
icici
sbi
axis

Step 3: Implement Core Downloader

Create:

src/downloaders/{amc}_downloader.py


Must include:

☐ Correct API endpoint

☐ Required payload only

☐ Retry logic (timeouts / 5xx)

☐ Empty-file handling

☐ File streaming download

☐ No parsing

☐ No renaming

☐ No filtering

Step 4: Folder + Atomic Contract

For each (year, month):

☐ Target folder: data/raw/{amc}/YYYY_MM/

☐ Download all files

☐ On full success only, create:

_SUCCESS.json


Mandatory fields:

amc

year

month

files_downloaded

timestamp

🚫 Never create marker on failure

Step 5: Corruption Detection

On startup:

☐ If folder exists without _SUCCESS.json

☐ Move to _corrupt/YYYY_MM/

☐ Log reason

☐ Re-download from scratch

🚫 Never merge partial data

Step 6: File Count Sanity

☐ Define AMC-specific expected range

☐ Log warning if outside range

☐ Never fail download

Step 7: Backfill Logic

Create:

src/scheduler/{amc}_backfill.py


Must support:

☐ Manual range mode (CLI only)

☐ Auto mode (latest eligible month)

☐ _SUCCESS.json as sole completion check

☐ No historical scanning in auto mode

Step 8: Scheduler Integration

Create:

src/scheduler/{amc}_scheduler.py


Rules:

☐ Uses auto mode only

☐ Startup time guard

☐ Runs at configured times

☐ Optional scheduled range healing

☐ No immediate execution on start

Step 9: Telegram Hooks

☐ Emit structured events only

☐ No formatting inside downloader

☐ Reuse notifier module

Step 10: Validation Before Marking “DONE”

An AMC is READY only if:

☐ Single-month download works

☐ Bulk manual range works

☐ Auto scheduler downloads only latest month

☐ Missing month is healed automatically

☐ _SUCCESS.json present

☐ Corruption recovery tested

Final Rule (Very Important)

If a new AMC cannot be onboarded by copying an existing AMC and changing ONLY config + endpoint — the architecture is wrong.

Fix architecture, not hacks.