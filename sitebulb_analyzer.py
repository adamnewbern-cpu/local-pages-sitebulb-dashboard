"""
sitebulb_analyzer.py
====================
Monthly Sitebulb SEO Audit Analysis Pipeline

Reads all project folders from Google Drive (Sitebulb Server Exports),
finds the most recently dated audit for each project, parses and scores
the Hints and Audit Summary sheets, compares with cached previous audits,
and produces a prioritized action items report for Adam's review.

READ README.md BEFORE RUNNING THIS SCRIPT.

Usage:
    python sitebulb_analyzer.py
    python sitebulb_analyzer.py --project "CORT.com"   # single project
    python sitebulb_analyzer.py --dry-run               # parse only, no cache write
"""

import io
import os
import re
import json
import time
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional

# ── Google API ─────────────────────────────────────────────────────────────────
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import gspread

# ── Configuration ──────────────────────────────────────────────────────────────

# Script location (used for credentials only — data now lives in Drive)
SCRIPT_DIR = Path(__file__).parent.resolve()
CREDENTIALS_DIR = SCRIPT_DIR / "credentials"

# Google Drive folder ID for "Sitebulb Server Exports"
# This is the parent folder that contains all GPO project subfolders.
DRIVE_PARENT_FOLDER_ID = "1-3LZeNARxdUP7mAyvSoZ1DjXVjrjbdMZ"

# Name of the audit cache subfolder created inside each project Drive folder
CACHE_FOLDER_NAME = ".audit_cache"

# Google API scopes — Drive full access required for reading folders + writing cache
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

# Only process project folders whose name begins with one of these prefixes.
# Any folder not matching is silently skipped.
ALLOWED_PREFIXES = (
    "GPO Local Pages |",
    "GPO Pages |",
    "GPO Dealer Pages |",
)

# Priority scoring weights
IMPORTANCE_VALUES = {"3": 3, "2": 2, "1": 1, "0": 0}
WARNING_TYPE_VALUES = {"4": 4, "3": 3, "2": 2, "1": 1}

# URL list threshold: show full list if at or below this count
FULL_URL_LIST_THRESHOLD = 12


# ── Authentication ─────────────────────────────────────────────────────────────

def get_google_credentials() -> Credentials:
    """
    Returns valid Google OAuth credentials.
    Tries service account first, then falls back to OAuth user flow.
    Caches the token so re-auth is only needed after expiry.
    """
    service_account_path = CREDENTIALS_DIR / "google_service_account.json"
    oauth_credentials_path = CREDENTIALS_DIR / "google_credentials.json"
    token_path = CREDENTIALS_DIR / "token.json"

    # Prefer service account (no interactive login needed — ideal for scheduled runs)
    if service_account_path.exists():
        creds = service_account.Credentials.from_service_account_file(
            str(service_account_path), scopes=SCOPES
        )
        return creds

    # Fall back to OAuth with token caching
    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not oauth_credentials_path.exists():
                raise FileNotFoundError(
                    f"No Google credentials found.\n"
                    f"Place a service account JSON at: {service_account_path}\n"
                    f"Or an OAuth client JSON at: {oauth_credentials_path}\n"
                    f"See README.md Section 11 for setup instructions."
                )
            flow = InstalledAppFlow.from_client_secrets_file(
                str(oauth_credentials_path), SCOPES
            )
            creds = flow.run_local_server(port=0)

        # Cache the token
        CREDENTIALS_DIR.mkdir(parents=True, exist_ok=True)
        token_path.write_text(creds.to_json())

    return creds


def get_drive_service(creds: Credentials):
    """Returns an authenticated Google Drive API service client."""
    return build("drive", "v3", credentials=creds)


def get_gspread_client(creds: Credentials) -> gspread.Client:
    """Returns an authenticated gspread client."""
    return gspread.authorize(creds)


# ── Google Drive Navigation ────────────────────────────────────────────────────

def drive_list_subfolders(drive_service, parent_id: str) -> list[dict]:
    """
    Returns all subfolders inside a given Drive folder as a list of dicts:
      [{"id": "...", "name": "..."}, ...]
    Automatically handles pagination.
    """
    folders = []
    page_token = None
    query = (
        f"'{parent_id}' in parents "
        f"and mimeType = 'application/vnd.google-apps.folder' "
        f"and trashed = false"
    )
    while True:
        response = drive_service.files().list(
            q=query,
            fields="nextPageToken, files(id, name)",
            pageToken=page_token,
        ).execute()
        folders.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return folders


def drive_list_files(drive_service, parent_id: str) -> list[dict]:
    """
    Returns all non-folder files inside a given Drive folder as a list of dicts:
      [{"id": "...", "name": "...", "mimeType": "..."}, ...]
    Automatically handles pagination.
    """
    files = []
    page_token = None
    query = (
        f"'{parent_id}' in parents "
        f"and mimeType != 'application/vnd.google-apps.folder' "
        f"and trashed = false"
    )
    while True:
        response = drive_service.files().list(
            q=query,
            fields="nextPageToken, files(id, name, mimeType)",
            pageToken=page_token,
        ).execute()
        files.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return files


def drive_find_folder(drive_service, parent_id: str, name: str) -> Optional[dict]:
    """
    Finds a subfolder by exact name inside a parent folder.
    Returns {"id": "...", "name": "..."} or None.
    """
    subfolders = drive_list_subfolders(drive_service, parent_id)
    for folder in subfolders:
        if folder["name"] == name:
            return folder
    return None


def drive_find_or_create_folder(drive_service, parent_id: str, name: str) -> str:
    """
    Finds a subfolder by name, or creates it if it doesn't exist.
    Returns the folder ID.
    """
    existing = drive_find_folder(drive_service, parent_id, name)
    if existing:
        return existing["id"]

    body = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    created = drive_service.files().create(body=body, fields="id").execute()
    return created["id"]


def drive_find_sheet_by_name(drive_service, parent_id: str, name: str) -> Optional[str]:
    """
    Finds a Google Sheets file by name (case-insensitive) inside a Drive folder.
    Returns the file ID (which is also the spreadsheet ID for gspread) or None.
    """
    name_lower = name.lower()
    files = drive_list_files(drive_service, parent_id)
    for f in files:
        if (
            f["name"].lower() == name_lower
            and f.get("mimeType") == "application/vnd.google-apps.spreadsheet"
        ):
            return f["id"]
    return None


# ── Project & Audit Folder Discovery ──────────────────────────────────────────

def list_project_folders(drive_service) -> list[dict]:
    """
    Returns all project folders in the Sitebulb Server Exports Drive folder
    that match an approved prefix (see ALLOWED_PREFIXES).
    All other folders are silently skipped.

    Returns a list of dicts: [{"id": "...", "name": "..."}, ...]
    """
    all_folders = drive_list_subfolders(drive_service, DRIVE_PARENT_FOLDER_ID)
    projects = [
        f for f in all_folders
        if any(f["name"].startswith(prefix) for prefix in ALLOWED_PREFIXES)
    ]
    # Sort alphabetically for consistent processing order
    projects.sort(key=lambda x: x["name"])
    return projects


def find_most_recent_audit_folder(drive_service, project_folder_id: str) -> Optional[dict]:
    """
    Scans a project Drive folder for subfolders named 'Audit YYYY-MM-DD HH:MM:SS'
    and returns the one with the most recent timestamp.
    Returns None if no audit folders are found.

    Returns a dict: {"id": "...", "name": "...", "audit_dt": datetime}
    """
    audit_pattern = re.compile(r"^Audit (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})$")
    subfolders = drive_list_subfolders(drive_service, project_folder_id)
    candidates = []

    for folder in subfolders:
        match = audit_pattern.match(folder["name"])
        if match:
            try:
                dt = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
                candidates.append({**folder, "audit_dt": dt})
            except ValueError:
                continue

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["audit_dt"], reverse=True)
    return candidates[0]


def extract_domain_from_project_name(project_name: str) -> str:
    """
    Attempts to extract the domain name from the project folder name.
    Folder names follow patterns like:
      'GPO Local Pages | The Radon Guys | local.thenashvilleradonguys.com'
    Returns the last pipe-delimited segment trimmed, or the full folder name.
    """
    if "|" in project_name:
        return project_name.split("|")[-1].strip()
    return project_name.strip()


# ── Google Sheets Reading ──────────────────────────────────────────────────────

def read_sheet_as_dicts(gc: gspread.Client, spreadsheet_id: str, sheet_index: int = 0) -> list[dict]:
    """
    Opens a Google Sheet by ID and returns all rows as a list of dicts
    (using the header row as keys). Empty rows are skipped.
    If the default sheet index returns nothing, tries all other tabs
    looking for one that contains a 'Hint' column.
    """
    try:
        spreadsheet = gc.open_by_key(spreadsheet_id)
        time.sleep(1)

        # Try the default sheet first
        worksheet = spreadsheet.get_worksheet(sheet_index)
        records = worksheet.get_all_records(
            empty2zero=False, head=1, value_render_option="FORMATTED_VALUE"
        )
        if records:
            return records

        # If empty, scan all tabs for one with a 'Hint' column
        print(f"    ℹ️  Default tab empty, scanning all tabs...")
        for ws in spreadsheet.worksheets():
            try:
                time.sleep(1)
                candidate = ws.get_all_records(
                    empty2zero=False, head=1, value_render_option="FORMATTED_VALUE"
                )
                if candidate and "Hint" in candidate[0]:
                    print(f"    ℹ️  Found data in tab '{ws.title}'.")
                    return candidate
                else:
                    print(f"    ℹ️  Tab '{ws.title}': {len(candidate)} rows, keys={list(candidate[0].keys())[:5] if candidate else 'no data'}")
            except Exception as tab_err:
                print(f"    ⚠️  Tab '{ws.title}' error: {tab_err}")
                continue
        return []
    except Exception as e:
        print(f"    ⚠️  Could not read spreadsheet {spreadsheet_id}: {e}")
        return []


def read_audit_summary_descriptions(gc: gspread.Client, spreadsheet_id: str) -> dict:
    """
    Reads every tab of the Audit Summary spreadsheet and returns a dict mapping
    hint_name -> description.  Each tab has columns: Type, Importance, Status,
    URLs, Hint, Description, Learn More.  Tabs with no 'Hint' column are skipped.
    """
    descriptions: dict[str, str] = {}
    try:
        spreadsheet = gc.open_by_key(spreadsheet_id)
        time.sleep(1)  # pause after opening spreadsheet
        for ws in spreadsheet.worksheets():
            try:
                time.sleep(1.5)  # pause between each tab to stay under Sheets API quota
                records = ws.get_all_records(empty2zero=False, head=1)
                for row in records:
                    hint_name = str(row.get("Hint", "")).strip()
                    description = str(row.get("Description", "")).strip()
                    if hint_name and description and hint_name not in descriptions:
                        descriptions[hint_name] = description
            except Exception as e:
                print(f"    ⚠️  Skipping Audit Summary tab '{ws.title}': {e}")
    except Exception as e:
        print(f"    ⚠️  Could not read Audit Summary descriptions ({spreadsheet_id}): {e}")
    return descriptions


# ── Hint Parsing & Scoring ─────────────────────────────────────────────────────

def parse_importance(value: str) -> tuple[int, str]:
    """
    Parses an Importance cell value like '3-High' or '1-Low'.
    Returns (numeric_value, label).
    """
    value = str(value).strip()
    match = re.match(r"^(\d)", value)
    if match:
        num = int(match.group(1))
        return num, value
    return 0, "0-None"


def parse_warning_type(value: str) -> tuple[int, str]:
    """
    Parses a Warning Type cell value like '4-Issue' or '2-Opportunity'.
    Returns (numeric_value, label).
    """
    value = str(value).strip()
    match = re.match(r"^(\d)", value)
    if match:
        num = int(match.group(1))
        return num, value
    return 0, "0-Unknown"


def calculate_priority_score(importance_num: int, warning_type_num: int) -> int:
    """
    Composite priority score. Higher = more urgent.
    Formula: (importance * 10) + warning_type
    Max possible: 34 (High/3 + Issue/4)
    Min possible: 0 (None/0 + no warning)
    """
    return (importance_num * 10) + warning_type_num


def calculate_trend(current_urls: int, previous_urls: int) -> str:
    """
    Compares current vs. previous URL counts and returns a trend label.
    """
    if previous_urls == 0 and current_urls > 0:
        return "NEW"
    if current_urls == 0 and previous_urls > 0:
        return "RESOLVED"
    if current_urls > previous_urls:
        return "WORSENING"
    if current_urls < previous_urls:
        return "IMPROVING"
    return "STABLE"


def parse_all_hints(records: list[dict]) -> list[dict]:
    """
    Takes raw rows from All Hints and returns structured, scored hint objects.
    Each record must have columns: Section, Hint, Coverage, URLs, Previous URLs,
    Importance, Warning Type, Learn More, Sheet URL.
    """
    parsed = []
    for row in records:
        section = str(row.get("Section", "")).strip()
        hint_name = str(row.get("Hint", "")).strip()
        if not hint_name:
            continue

        try:
            coverage = float(str(row.get("Coverage", 0)).replace("%", "").strip() or 0)
        except ValueError:
            coverage = 0.0

        try:
            urls = int(str(row.get("URLs", 0)).replace(",", "").strip() or 0)
        except ValueError:
            urls = 0

        try:
            prev_urls = int(str(row.get("Previous URLs", 0)).replace(",", "").strip() or 0)
        except ValueError:
            prev_urls = 0

        imp_num, imp_label = parse_importance(row.get("Importance", "0"))
        warn_num, warn_label = parse_warning_type(row.get("Warning Type", "0"))
        priority = calculate_priority_score(imp_num, warn_num)
        trend = calculate_trend(urls, prev_urls)
        learn_more = str(row.get("Learn More", "")).strip()
        sheet_url = str(row.get("Sheet URL", "")).strip()

        parsed.append({
            "section": section,
            "hint": hint_name,
            "description": None,          # filled in later by merge_descriptions()
            "coverage": coverage,
            "urls": urls,
            "previous_urls": prev_urls if prev_urls != 0 else None,
            "importance": imp_label,
            "importance_num": imp_num,
            "warning_type": warn_label,
            "warning_type_num": warn_num,
            "priority_score": priority,
            "trend": trend,
            "learn_more": learn_more,
            "sheet_url": sheet_url,
        })

    # Sort: highest priority first, then by URLs affected descending
    parsed.sort(key=lambda x: (x["priority_score"], x["urls"]), reverse=True)
    return parsed


def merge_descriptions(hints: list[dict], descriptions: dict) -> list[dict]:
    """
    Stamps the 'description' field onto each hint using data pulled from the
    Audit Summary spreadsheet.  Matching is done by hint name (exact first,
    then case-insensitive fallback).  Unmatched hints keep description=None.
    """
    # Build a lowercase lookup for fuzzy matching
    lower_map = {k.lower(): v for k, v in descriptions.items()}
    for hint in hints:
        name = hint["hint"]
        if name in descriptions:
            hint["description"] = descriptions[name]
        elif name.lower() in lower_map:
            hint["description"] = lower_map[name.lower()]
    return hints


# ── Cache Management (Google Drive) ───────────────────────────────────────────

def load_previous_cache_from_drive(drive_service, project_folder_id: str) -> Optional[dict]:
    """
    Loads the most recent audit cache JSON from the project's .audit_cache
    subfolder in Google Drive. Returns None if no cache exists.
    """
    cache_folder = drive_find_folder(drive_service, project_folder_id, CACHE_FOLDER_NAME)
    if not cache_folder:
        return None

    # List all JSON files in the cache folder
    all_files = drive_list_files(drive_service, cache_folder["id"])
    json_files = [
        f for f in all_files
        if f["name"].endswith(".json")
    ]
    if not json_files:
        return None

    # Sort by name descending to get the most recent date (YYYY-MM-DD.json)
    json_files.sort(key=lambda x: x["name"], reverse=True)
    most_recent = json_files[0]

    try:
        content = drive_service.files().get_media(fileId=most_recent["id"]).execute()
        return json.loads(content.decode("utf-8"))
    except Exception as e:
        print(f"    ⚠️  Could not read cache file {most_recent['name']}: {e}")
        return None


def save_audit_cache_to_drive(
    drive_service,
    project_folder_id: str,
    audit_date: str,
    domain: str,
    project_name: str,
    hints: list[dict],
    summary_metrics: dict,
    dry_run: bool = False,
) -> None:
    """
    Saves a JSON snapshot of this audit to the project's .audit_cache
    subfolder in Google Drive. Skipped if dry_run=True.
    """
    if dry_run:
        return

    cache_data = {
        "project": project_name,
        "domain": domain,
        "audit_date": audit_date,
        "hints": [
            {k: v for k, v in h.items() if k not in ("importance_num", "warning_type_num")}
            for h in hints
        ],
        "summary_metrics": summary_metrics,
    }
    json_bytes = json.dumps(cache_data, indent=2).encode("utf-8")
    file_name = f"{audit_date}.json"

    # Find or create the .audit_cache folder
    cache_folder_id = drive_find_or_create_folder(
        drive_service, project_folder_id, CACHE_FOLDER_NAME
    )

    # Check if a file for this date already exists (update instead of duplicate)
    existing_files = drive_list_files(drive_service, cache_folder_id)
    existing = next((f for f in existing_files if f["name"] == file_name), None)

    media = MediaIoBaseUpload(
        io.BytesIO(json_bytes),
        mimetype="application/json",
        resumable=False,
    )

    if existing:
        drive_service.files().update(
            fileId=existing["id"],
            media_body=media,
        ).execute()
    else:
        drive_service.files().create(
            body={"name": file_name, "parents": [cache_folder_id]},
            media_body=media,
            fields="id",
        ).execute()

    print(f"    💾 Cache saved to Drive: {file_name}")


def apply_cache_trends(hints: list[dict], previous_cache: Optional[dict]) -> list[dict]:
    """
    Cross-references current hints against the previous cache to refine trend labels.
    Useful when the Sitebulb 'Previous URLs' column is stale or missing.
    """
    if not previous_cache:
        return hints

    previous_hints_map = {
        h["hint"]: h["urls"] for h in previous_cache.get("hints", [])
    }

    for hint in hints:
        if hint["hint"] in previous_hints_map:
            cached_prev = previous_hints_map[hint["hint"]]
            # Only override if the cached value differs from Sitebulb's own previous count
            if hint["previous_urls"] == 0 and cached_prev > 0:
                hint["previous_urls"] = cached_prev
                hint["trend"] = calculate_trend(hint["urls"], cached_prev)
        else:
            # Hint didn't exist in last cache — mark as new
            if hint["urls"] > 0:
                hint["trend"] = "NEW"

    return hints


# ── Report Generation ──────────────────────────────────────────────────────────

def format_url_sample(urls: int, sheet_url: str) -> str:
    """
    Returns a display string for the affected URL count.
    If <= FULL_URL_LIST_THRESHOLD, notes that the full list is available.
    If > threshold, links to the Sheet URL.
    """
    if urls <= FULL_URL_LIST_THRESHOLD:
        detail = f"{urls} URL{'s' if urls != 1 else ''} affected (full list available in detail sheet)"
    else:
        detail = f"{urls} URLs affected"

    if sheet_url:
        detail += f"\n    → Full list: {sheet_url}"

    return detail


TREND_ICONS = {
    "NEW": "🆕 NEW",
    "WORSENING": "📈 WORSENING",
    "IMPROVING": "📉 IMPROVING",
    "STABLE": "➡️  STABLE",
    "RESOLVED": "✅ RESOLVED",
}


def generate_project_report(
    project_name: str,
    domain: str,
    audit_date: str,
    hints: list[dict],
    summary_metrics: dict,
    previous_cache: Optional[dict],
) -> str:
    """
    Produces a human-readable, prioritized action items report for one project.
    Follows the language and tone rules defined in README.md Section 7.
    """
    lines = []
    lines.append("=" * 72)
    lines.append(f"PROJECT: {project_name}")
    lines.append(f"Domain:  {domain}")
    lines.append(f"Audit:   {audit_date}")
    if previous_cache:
        lines.append(f"Previous audit: {previous_cache.get('audit_date', 'unknown')}")
    lines.append("=" * 72)

    # ── Summary Metrics ────────────────────────────────────────────────────────
    if summary_metrics:
        lines.append("\n📊 AUDIT SUMMARY")
        lines.append("-" * 40)
        for key, value in summary_metrics.items():
            lines.append(f"  {key}: {value}")

    # ── Separate resolved from active hints ───────────────────────────────────
    active_hints = [h for h in hints if h["trend"] != "RESOLVED"]
    resolved_hints = [h for h in hints if h["trend"] == "RESOLVED"]
    new_or_worsening = [h for h in active_hints if h["trend"] in ("NEW", "WORSENING")]

    # ── Urgent flags ──────────────────────────────────────────────────────────
    if new_or_worsening:
        lines.append(f"\n⚠️  ATTENTION: {len(new_or_worsening)} issue(s) are NEW or WORSENING")
        for h in new_or_worsening:
            lines.append(f"   • {TREND_ICONS[h['trend']]}: {h['hint']} ({h['urls']} URLs)")

    # ── Action Items (active hints, sorted by priority) ───────────────────────
    lines.append(f"\n📋 ACTION ITEMS — {len(active_hints)} hint(s) to address")
    lines.append("-" * 40)

    if not active_hints:
        lines.append("  No active hints. Site is clean.")
    else:
        for i, hint in enumerate(active_hints, 1):
            trend_icon = TREND_ICONS.get(hint["trend"], "")
            lines.append(
                f"\n[{i}] {hint['importance']} | {hint['warning_type']} | "
                f"Priority Score: {hint['priority_score']} | {trend_icon}"
            )
            lines.append(f"    Section:  {hint['section']}")
            lines.append(f"    Issue:    {hint['hint']}")
            lines.append(f"    Coverage: {hint['coverage']}% of crawled URLs")
            lines.append(f"    Affected: {format_url_sample(hint['urls'], hint['sheet_url'])}")
            if hint["learn_more"]:
                lines.append(f"    Docs:     {hint['learn_more']}")

    # ── Resolved Since Last Crawl ──────────────────────────────────────────────
    if resolved_hints:
        lines.append(f"\n✅ RESOLVED SINCE LAST CRAWL — {len(resolved_hints)} item(s)")
        lines.append("-" * 40)
        for hint in resolved_hints:
            lines.append(f"  • {hint['hint']} (was {hint['previous_urls']} URLs, now 0)")

    lines.append("\n" + "=" * 72)
    lines.append("⏸️  AWAITING ADAM'S REVIEW AND APPROVAL BEFORE TRELLO CARDS ARE CREATED")
    lines.append("=" * 72 + "\n")

    return "\n".join(lines)


# ── Main Pipeline ──────────────────────────────────────────────────────────────

def run_pipeline(target_project: Optional[str] = None, dry_run: bool = False) -> None:
    """
    Main entry point. Discovers all projects in Google Drive, processes each one,
    and outputs a consolidated action items report for Adam's review.
    """
    print("\n" + "█" * 72)
    print("  SITEBULB SEO AUDIT PIPELINE — STARTING")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Data source: Google Drive (folder ID: {DRIVE_PARENT_FOLDER_ID})")
    print("█" * 72 + "\n")

    # Acknowledge README
    readme_path = SCRIPT_DIR / "README.md"
    if readme_path.exists():
        print("📖 README.md found and acknowledged.\n")
    else:
        print("⚠️  WARNING: README.md not found. Proceeding with built-in defaults.\n")

    # Authenticate
    print("🔐 Authenticating with Google APIs...")
    try:
        creds = get_google_credentials()
        drive_service = get_drive_service(creds)
        gc = get_gspread_client(creds)
        print("   ✅ Authentication successful.\n")
    except FileNotFoundError as e:
        print(f"   ❌ Authentication failed: {e}")
        print("   Cannot proceed without credentials. See README.md Section 11.")
        return
    except Exception as e:
        print(f"   ❌ Unexpected auth error: {e}")
        return

    # Discover projects from Google Drive
    print(f"☁️  Scanning Google Drive folder for projects...")
    all_projects = list_project_folders(drive_service)

    if target_project:
        all_projects = [
            p for p in all_projects
            if target_project.lower() in p["name"].lower()
        ]
        if not all_projects:
            print(f"❌ No project folder matching '{target_project}' found in Drive.")
            return

    print(f"📁 Found {len(all_projects)} project(s) to process.")
    if dry_run:
        print("   (DRY RUN — no cache writes)")
    print()
    print(f"📣 Notifying Adam: Monthly SEO audit pipeline has started. "
          f"Processing {len(all_projects)} project(s).\n")

    all_reports = []
    all_projects_json = []   # collects per-project dicts for dashboard JSON
    completed = 0
    errors = []

    for project in all_projects:
        project_name = project["name"]
        project_folder_id = project["id"]
        domain = extract_domain_from_project_name(project_name)

        print(f"{'─' * 72}")
        print(f"🔍 Processing: {project_name}")
        print(f"   Domain: {domain}")

        # Step 1: Find most recent audit folder
        audit_folder = find_most_recent_audit_folder(drive_service, project_folder_id)
        if not audit_folder:
            msg = f"   ⚠️  No audit folders found. Skipping."
            print(msg)
            errors.append(f"{project_name}: No audit folders found.")
            continue

        audit_date_str = audit_folder["audit_dt"].strftime("%Y-%m-%d")
        audit_folder_id = audit_folder["id"]
        print(f"   Most recent audit: {audit_folder['name']}")

        # Step 2: Load Audit Summary — read all tabs for descriptions
        summary_metrics = {}
        descriptions: dict = {}
        summary_sheet_id = drive_find_sheet_by_name(
            drive_service, audit_folder_id, "Audit Summary"
        )
        if summary_sheet_id:
            print(f"   📊 Reading Audit Summary (all tabs)...")
            descriptions = read_audit_summary_descriptions(gc, summary_sheet_id)
            print(f"      {len(descriptions)} hint descriptions extracted.")
        else:
            print(f"   ⚠️  Audit Summary sheet not found in Drive audit folder.")

        # Step 3: Find the Hints subfolder and load All Hints
        hints_folder = drive_find_folder(drive_service, audit_folder_id, "Hints")
        if not hints_folder:
            msg = f"   ⚠️  Hints subfolder not found in Drive. Skipping."
            print(msg)
            errors.append(f"{project_name}: Hints folder missing in Drive.")
            continue

        all_hints_sheet_id = drive_find_sheet_by_name(
            drive_service, hints_folder["id"], "All Hints"
        )
        if not all_hints_sheet_id:
            msg = f"   ⚠️  All Hints sheet not found in Drive. Skipping."
            print(msg)
            errors.append(f"{project_name}: All Hints sheet missing in Drive.")
            continue

        print(f"   🗒️  Reading All Hints...")
        time.sleep(2)  # pause before reading All Hints to respect API quota
        raw_records = read_sheet_as_dicts(gc, all_hints_sheet_id)
        if not raw_records:
            print(f"   ⚠️  All Hints sheet is empty or unreadable.")
            errors.append(f"{project_name}: All Hints sheet empty.")
            continue

        # Step 4: Parse, score, and merge descriptions
        hints = parse_all_hints(raw_records)
        hints = merge_descriptions(hints, descriptions)
        print(f"   ✅ {len(hints)} hint(s) parsed and scored.")

        # Step 5: Load cache from Drive and apply trend refinement
        previous_cache = load_previous_cache_from_drive(drive_service, project_folder_id)
        if previous_cache:
            print(f"   📂 Previous cache loaded: {previous_cache.get('audit_date')}")
        hints = apply_cache_trends(hints, previous_cache)

        # Flag new/worsening High or Medium issues immediately
        urgent = [
            h for h in hints
            if h["trend"] in ("NEW", "WORSENING") and h["importance_num"] >= 2
        ]
        for h in urgent:
            print(f"   ⚠️  ALERT: {h['hint']} — {TREND_ICONS[h['trend']]} ({h['urls']} URLs)")

        # Step 6: Save cache to Drive
        save_audit_cache_to_drive(
            drive_service,
            project_folder_id,
            audit_date_str,
            domain,
            project_name,
            hints,
            summary_metrics,
            dry_run=dry_run,
        )

        # Step 7: Generate text report
        report = generate_project_report(
            project_name, domain, audit_date_str,
            hints, summary_metrics, previous_cache
        )
        all_reports.append(report)
        completed += 1
        action_count = len([h for h in hints if h["trend"] != "RESOLVED"])
        print(f"   ✅ {project_name} complete. {action_count} action item(s).")
        print(f"   📣 Adam: {project_name} audit complete. {action_count} action items identified.")
        time.sleep(3)  # pause between projects to avoid hitting API rate limits

        # Collect data for dashboard JSON output
        all_projects_json.append({
            "folder_name": project_name,
            "domain": domain,
            "audit_date": audit_date_str,
            "hints": [
                {k: v for k, v in h.items() if k not in ("importance_num", "warning_type_num")}
                for h in hints
            ],
        })

    # ── Consolidated Output ────────────────────────────────────────────────────
    output_path = SCRIPT_DIR / f"action_items_{datetime.now().strftime('%Y-%m-%d')}.txt"

    if not dry_run and all_reports:
        header = (
            f"SITEBULB SEO AUDIT — ACTION ITEMS REPORT\n"
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Projects processed: {completed} / {len(all_projects)}\n"
            f"{'─' * 72}\n"
            f"ACTION REQUIRED: Please review the items below and reply with approval\n"
            f"before Trello cards are created. Run trello_publisher.py to publish.\n"
        )
        full_report = header + "\n\n".join(all_reports)
        output_path.write_text(full_report, encoding="utf-8")
        print(f"\n📄 Full report saved locally: {output_path.name}")

    # ── Dashboard JSON output ──────────────────────────────────────────────────
    # Writes data/all_hints.json which the GitHub Pages dashboard reads.
    # This file is committed to the repo by GitHub Actions on each run.
    if all_projects_json:
        data_dir = SCRIPT_DIR / "data"
        data_dir.mkdir(exist_ok=True)
        dashboard_payload = {
            "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "projects": all_projects_json,
        }
        json_path = data_dir / "all_hints.json"
        json_path.write_text(json.dumps(dashboard_payload, indent=2), encoding="utf-8")
        if not dry_run:
            print(f"📊 Dashboard JSON saved: data/all_hints.json ({len(all_projects_json)} projects)")
        else:
            for report in all_reports:
                print(report)

    # ── Errors summary ─────────────────────────────────────────────────────────
    if errors:
        print(f"\n⚠️  {len(errors)} project(s) had issues:")
        for err in errors:
            print(f"   • {err}")

    # ── Final notification ─────────────────────────────────────────────────────
    print("\n" + "█" * 72)
    print(f"  PIPELINE COMPLETE")
    print(f"  {completed}/{len(all_projects)} project(s) processed successfully.")
    if not dry_run and all_reports:
        print(f"  Report saved to: {output_path.name}")
    print(f"\n  📣 Adam: All {completed} projects have been processed.")
    print(f"  Please review '{output_path.name}' and approve before")
    print(f"  running trello_publisher.py to create Trello cards.")
    print("█" * 72 + "\n")


# ── Entry Point ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sitebulb SEO Audit Analysis Pipeline (Google Drive edition)"
    )
    parser.add_argument(
        "--project",
        type=str,
        default=None,
        help="Process only a specific project (partial name match). "
             "Example: --project 'ATI Physical Therapy'",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and score without writing cache or output files. "
             "Prints report to console.",
    )
    args = parser.parse_args()
    run_pipeline(target_project=args.project, dry_run=args.dry_run)
