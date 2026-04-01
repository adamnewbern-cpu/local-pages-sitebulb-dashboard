"""
ga4_analyzer.py
===============
Google Analytics 4 Data Pipeline

Pulls monthly GA4 data for all properties listed in credentials/ga4_properties.json,
computes MoM and YoY comparisons, and outputs data/ga4_data.json for use by
ga4_recommendations.py and the dashboard.

Usage:
    python ga4_analyzer.py                          # all properties
    python ga4_analyzer.py --site "ATI Physical Therapy"  # single property
    python ga4_analyzer.py --dry-run                # skip file write

Requirements:
    pip install google-analytics-data google-auth --break-system-packages

Setup:
    1. Enable "Google Analytics Data API" in Google Cloud Console
    2. Add service account email as Viewer in GA4 Admin → Property Access Management
    3. Run discover_ga4_properties.py to get property IDs
    4. Add "ga4_property_id" to each entry in credentials/gsc_properties.json:
       { "site_url": "...", "display_name": "ATI Physical Therapy",
         "ga4_property_id": "123456789" }
    Sites without a ga4_property_id are skipped.

Output schema (data/ga4_data.json):
    {
      "generated_at": "...",
      "sites": [
        {
          "property_id":  "123456789",
          "display_name": "ATI Physical Therapy",
          "window": {
            "current": { "label": "March 2026", "start": "...", "end": "..." },
            "mom":     { "label": "February 2026", ... },
            "yoy":     { "label": "March 2025", ... }
          },
          "yoy_available": true,
          "totals": { ... },
          "events":   [ { "event_name": "calls", "current": 94, "mom": 88, ... } ],
          "channels": [ { "channel": "Organic Search", "current_sessions": 420, ... } ],
          "top_pages": [ { "landing_page": "/chicago/...", "current_sessions": 145, ... } ],
          "ytd": {
            "current_year": 2026,
            "months": [ { "month": "2026-01", "label": "January 2026",
                          "sessions": 4200, "prior_year_sessions": 800 } ],
            "ytd_current": 9032,
            "ytd_prior": 2350,
            "ytd_change_pct": 284.1
          }
        }
      ]
    }
"""

import os
import json
import argparse
import time
import calendar
import warnings
from datetime import date, timedelta
from pathlib import Path

warnings.filterwarnings("ignore", category=FutureWarning)

from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    DateRange,
    Dimension,
    Metric,
    FilterExpression,
    Filter,
    FilterExpressionList,
    OrderBy,
    Pivot,
)

# ── Configuration ──────────────────────────────────────────────────────────────

SCRIPT_DIR           = Path(__file__).parent.resolve()
CREDENTIALS_DIR      = SCRIPT_DIR / "credentials"
DATA_DIR             = SCRIPT_DIR / "data"
SERVICE_ACCOUNT_PATH = CREDENTIALS_DIR / "google_service_account.json"
# ga4_analyzer reads from gsc_properties.json — the same site list used by
# gsc_analyzer.py.  Each entry must include a "ga4_property_id" field.
# Run discover_ga4_properties.py to find the numeric ID for each site.
GSC_PROPERTIES_PATH  = CREDENTIALS_DIR / "gsc_properties.json"
OUTPUT_PATH          = DATA_DIR / "ga4_data.json"

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]

# Pause between API calls (seconds) — GA4 has a 60 req/min quota
API_DELAY = 0.5

# Top N landing pages to include
TOP_PAGES_LIMIT = 10

# Standard GA4 auto-collected / generic events to exclude from the events table.
# Everything NOT on this list is treated as a meaningful custom event / KPI.
GENERIC_EVENTS_EXCLUDE = {
    "first_visit",
    "session_start",
    "page_view",
    "scroll",
    "user_engagement",
    "click",
    "all_clicks",
    "gtm.js",
    "gtm.dom",
    "gtm.load",
    "gtm.historyChange",
    "gtm.historyChange-v2",
    "js_error",
    "form_start",
    "video_start",
    "video_progress",
    "video_complete",
    "file_download",
}


# ── Auth ───────────────────────────────────────────────────────────────────────

def get_client():
    if not SERVICE_ACCOUNT_PATH.exists():
        raise FileNotFoundError(
            f"Service account not found at {SERVICE_ACCOUNT_PATH}"
        )
    creds = service_account.Credentials.from_service_account_file(
        str(SERVICE_ACCOUNT_PATH), scopes=SCOPES
    )
    return BetaAnalyticsDataClient(credentials=creds)


# ── Date helpers ───────────────────────────────────────────────────────────────

def last_full_month():
    """Returns (start_iso, end_iso, label) for the most recently completed month."""
    today = date.today()
    last_day = today.replace(day=1) - timedelta(days=1)
    first_day = last_day.replace(day=1)
    return first_day.isoformat(), last_day.isoformat(), last_day.strftime("%B %Y")


def month_ago(start_iso: str, months: int):
    """Shift a month-start date back by N months. Returns (start, end, label)."""
    d = date.fromisoformat(start_iso)
    month = d.month - months
    year  = d.year
    while month <= 0:
        month += 12
        year  -= 1
    last_day_num = calendar.monthrange(year, month)[1]
    start = date(year, month, 1)
    end   = date(year, month, last_day_num)
    return start.isoformat(), end.isoformat(), end.strftime("%B %Y")


def ytd_range(year: int, end_month: int):
    """Returns (start_iso, end_iso) covering Jan 1 → last day of end_month for year."""
    last_day_num = calendar.monthrange(year, end_month)[1]
    return date(year, 1, 1).isoformat(), date(year, end_month, last_day_num).isoformat()


# ── Calculation helpers ────────────────────────────────────────────────────────

def pct_change(current, previous):
    if previous is None or previous == 0:
        return None
    return round((current - previous) / abs(previous) * 100, 1)


def safe_float(val, decimals=2):
    try:
        return round(float(val), decimals)
    except (TypeError, ValueError):
        return 0.0


def safe_int(val):
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


# ── GA4 API helpers ────────────────────────────────────────────────────────────

def run_report(client, property_id: str, start: str, end: str,
               dimensions: list, metrics: list,
               dimension_filter=None, order_bys=None, limit: int = 100):
    """
    Execute a GA4 RunReport request. Returns list of row dicts.
    Each row has keys for each dimension and metric by name.
    """
    dim_objs    = [Dimension(name=d) for d in dimensions]
    metric_objs = [Metric(name=m) for m in metrics]

    kwargs = dict(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start, end_date=end)],
        dimensions=dim_objs,
        metrics=metric_objs,
        limit=limit,
    )
    if dimension_filter:
        kwargs["dimension_filter"] = dimension_filter
    if order_bys:
        kwargs["order_bys"] = order_bys

    try:
        response = client.run_report(RunReportRequest(**kwargs))
    except Exception as e:
        print(f"      ⚠️  API error for property {property_id}: {e}")
        return []

    rows = []
    for row in response.rows:
        entry = {}
        for i, dim in enumerate(dimensions):
            entry[dim] = row.dimension_values[i].value
        for i, met in enumerate(metrics):
            entry[met] = row.metric_values[i].value
        rows.append(entry)

    time.sleep(API_DELAY)
    return rows


def run_multi_range_report(client, property_id: str,
                            ranges: list,          # list of (start, end, label)
                            dimensions: list,
                            metrics: list,
                            dimension_filter=None,
                            order_bys=None,
                            limit: int = 100):
    """
    Runs a single RunReport request with multiple DateRanges to fetch
    current, MoM, and YoY in one API call.
    Returns list of row dicts with extra 'date_range' key (index 0/1/2).
    """
    dim_objs    = [Dimension(name=d) for d in dimensions]
    metric_objs = [Metric(name=m) for m in metrics]
    date_ranges = [DateRange(start_date=s, end_date=e, name=lbl)
                   for s, e, lbl in ranges]

    kwargs = dict(
        property=f"properties/{property_id}",
        date_ranges=date_ranges,
        dimensions=["dateRange"] + dim_objs if isinstance(dim_objs[0], str)
                   else [Dimension(name="dateRange")] + dim_objs,
        metrics=metric_objs,
        limit=limit,
    )
    if dimension_filter:
        kwargs["dimension_filter"] = dimension_filter
    if order_bys:
        kwargs["order_bys"] = order_bys

    try:
        response = client.run_report(RunReportRequest(**kwargs))
    except Exception as e:
        print(f"      ⚠️  Multi-range API error for property {property_id}: {e}")
        return []

    rows = []
    for row in response.rows:
        entry = {"date_range": row.dimension_values[0].value}
        for i, dim in enumerate(dimensions):
            entry[dim] = row.dimension_values[i + 1].value
        for i, met in enumerate(metrics):
            entry[met] = row.metric_values[i].value
        rows.append(entry)

    time.sleep(API_DELAY)
    return rows


# ── Per-period fetchers ────────────────────────────────────────────────────────

def fetch_totals(client, property_id, start, end):
    """Fetch Sessions, Engaged Sessions, Engagement Rate for a date window."""
    rows = run_report(
        client, property_id, start, end,
        dimensions=[],
        metrics=["sessions", "engagedSessions", "engagementRate"],
        limit=1,
    )
    if not rows:
        return {"sessions": 0, "engaged_sessions": 0, "engagement_rate": 0.0}
    r = rows[0]
    return {
        "sessions":        safe_int(r.get("sessions", 0)),
        "engaged_sessions": safe_int(r.get("engagedSessions", 0)),
        "engagement_rate":  safe_float(r.get("engagementRate", 0), decimals=4),
    }


def fetch_events(client, property_id, start, end):
    """Fetch custom event counts, excluding generic auto-collected events."""
    # Build filter to exclude generic events
    exclude_filters = [
        Filter(
            field_name="eventName",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.EXACT,
                value=evt,
                case_sensitive=False,
            )
        )
        for evt in GENERIC_EVENTS_EXCLUDE
    ]
    # NOT(event IN generic_list) — use AND of NOT conditions
    not_filters = [
        FilterExpression(not_expression=FilterExpression(filter=f))
        for f in exclude_filters
    ]
    dim_filter = FilterExpression(
        and_group=FilterExpressionList(expressions=not_filters)
    )

    rows = run_report(
        client, property_id, start, end,
        dimensions=["eventName"],
        metrics=["eventCount"],
        dimension_filter=dim_filter,
        order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="eventCount"),
                           desc=True)],
        limit=50,
    )
    return {r["eventName"]: safe_int(r.get("eventCount", 0)) for r in rows}


def fetch_channels(client, property_id, start, end):
    """Fetch sessions by Default Channel Group."""
    rows = run_report(
        client, property_id, start, end,
        dimensions=["sessionDefaultChannelGroup"],
        metrics=["sessions"],
        order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"),
                           desc=True)],
        limit=20,
    )
    return {r["sessionDefaultChannelGroup"]: safe_int(r.get("sessions", 0))
            for r in rows}


def fetch_top_pages(client, property_id, start, end, limit=TOP_PAGES_LIMIT):
    """Fetch top landing pages by sessions with engaged sessions and engagement rate."""
    rows = run_report(
        client, property_id, start, end,
        dimensions=["landingPage"],
        metrics=["sessions", "engagedSessions", "engagementRate"],
        order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"),
                           desc=True)],
        limit=limit,
    )
    return {
        r["landingPage"]: {
            "sessions":        safe_int(r.get("sessions", 0)),
            "engaged_sessions": safe_int(r.get("engagedSessions", 0)),
            "engagement_rate":  safe_float(r.get("engagementRate", 0), decimals=4),
        }
        for r in rows
    }


def fetch_monthly_sessions(client, property_id, start, end):
    """Fetch sessions grouped by month within a date range. Returns dict month→sessions."""
    rows = run_report(
        client, property_id, start, end,
        dimensions=["year", "month"],
        metrics=["sessions"],
        order_bys=[
            OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="year")),
            OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="month")),
        ],
        limit=24,
    )
    result = {}
    for r in rows:
        key = f"{r['year']}-{r['month'].zfill(2)}"
        result[key] = safe_int(r.get("sessions", 0))
    return result


# ── Property analyzer ──────────────────────────────────────────────────────────

def analyze_property(client, property_id: str, display_name: str,
                     cur_start: str, cur_end: str, cur_label: str,
                     mom_start: str, mom_end: str, mom_label: str,
                     yoy_start: str, yoy_end: str, yoy_label: str) -> dict:

    print(f"   → Totals...")
    cur_totals = fetch_totals(client, property_id, cur_start, cur_end)
    mom_totals = fetch_totals(client, property_id, mom_start, mom_end)
    yoy_totals = fetch_totals(client, property_id, yoy_start, yoy_end)

    yoy_available = (yoy_totals["sessions"] > 0 or yoy_totals["engaged_sessions"] > 0)

    if not yoy_available:
        yoy_totals = None

    totals = {
        "current": cur_totals,
        "mom":     mom_totals,
        "yoy":     yoy_totals,
        "yoy_available": yoy_available,
        # Sessions
        "mom_sessions_change_pct":        pct_change(cur_totals["sessions"], mom_totals["sessions"]),
        "yoy_sessions_change_pct":        pct_change(cur_totals["sessions"], yoy_totals["sessions"]) if yoy_available else None,
        # Engaged sessions
        "mom_engaged_sessions_change_pct": pct_change(cur_totals["engaged_sessions"], mom_totals["engaged_sessions"]),
        "yoy_engaged_sessions_change_pct": pct_change(cur_totals["engaged_sessions"], yoy_totals["engaged_sessions"]) if yoy_available else None,
        # Engagement rate (store as percentage points delta, not pct of pct)
        "mom_engagement_rate_change_pts":  round(cur_totals["engagement_rate"] - mom_totals["engagement_rate"], 4) if mom_totals else None,
        "yoy_engagement_rate_change_pts":  round(cur_totals["engagement_rate"] - (yoy_totals["engagement_rate"] if yoy_available else 0), 4) if yoy_available else None,
    }

    # ── Events ────────────────────────────────────────────────────────────────
    print(f"   → Events...")
    cur_events = fetch_events(client, property_id, cur_start, cur_end)
    mom_events = fetch_events(client, property_id, mom_start, mom_end)
    yoy_events = fetch_events(client, property_id, yoy_start, yoy_end) if yoy_available else {}

    all_event_names = sorted(
        cur_events.keys(),
        key=lambda e: cur_events.get(e, 0),
        reverse=True,
    )

    events = []
    for evt in all_event_names:
        cur_cnt = cur_events.get(evt, 0)
        mom_cnt = mom_events.get(evt, 0)
        yoy_cnt = yoy_events.get(evt, 0) if yoy_available else None
        events.append({
            "event_name":       evt,
            "current":          cur_cnt,
            "mom":              mom_cnt,
            "yoy":              yoy_cnt,
            "mom_change_pct":   pct_change(cur_cnt, mom_cnt),
            "yoy_change_pct":   pct_change(cur_cnt, yoy_cnt) if yoy_available else None,
        })

    # ── Channels ──────────────────────────────────────────────────────────────
    print(f"   → Channels...")
    cur_channels = fetch_channels(client, property_id, cur_start, cur_end)
    mom_channels = fetch_channels(client, property_id, mom_start, mom_end)
    yoy_channels = fetch_channels(client, property_id, yoy_start, yoy_end) if yoy_available else {}

    total_cur_sessions = cur_totals["sessions"] or 1  # avoid div/0
    all_channel_names  = sorted(cur_channels.keys(), key=lambda c: cur_channels.get(c, 0), reverse=True)

    channels = []
    for ch in all_channel_names:
        cur_s = cur_channels.get(ch, 0)
        mom_s = mom_channels.get(ch, 0)
        yoy_s = yoy_channels.get(ch, 0) if yoy_available else None
        channels.append({
            "channel":              ch,
            "current_sessions":     cur_s,
            "current_pct":          round(cur_s / total_cur_sessions * 100, 1),
            "mom_sessions":         mom_s,
            "yoy_sessions":         yoy_s,
            "mom_sessions_change_pct": pct_change(cur_s, mom_s),
            "yoy_sessions_change_pct": pct_change(cur_s, yoy_s) if yoy_available else None,
        })

    # ── Top Landing Pages ──────────────────────────────────────────────────────
    print(f"   → Top landing pages...")
    cur_pages = fetch_top_pages(client, property_id, cur_start, cur_end)
    mom_pages = fetch_top_pages(client, property_id, mom_start, mom_end, limit=50)
    yoy_pages = fetch_top_pages(client, property_id, yoy_start, yoy_end, limit=50) if yoy_available else {}

    top_pages = []
    for page_path, cur_data in cur_pages.items():
        mom_data = mom_pages.get(page_path, {})
        yoy_data = yoy_pages.get(page_path, {}) if yoy_available else {}
        top_pages.append({
            "landing_page":               page_path,
            "current_sessions":           cur_data["sessions"],
            "current_engaged_sessions":   cur_data["engaged_sessions"],
            "current_engagement_rate":    cur_data["engagement_rate"],
            "mom_sessions":               mom_data.get("sessions", 0),
            "yoy_sessions":               yoy_data.get("sessions", 0) if yoy_available else None,
            "mom_sessions_change_pct":    pct_change(cur_data["sessions"], mom_data.get("sessions", 0)),
            "yoy_sessions_change_pct":    pct_change(cur_data["sessions"], yoy_data.get("sessions", 0)) if yoy_available else None,
        })

    # ── YTD ───────────────────────────────────────────────────────────────────
    print(f"   → YTD sessions...")
    cur_year  = date.fromisoformat(cur_end).year
    cur_month = date.fromisoformat(cur_end).month

    ytd_cur_start, ytd_cur_end = ytd_range(cur_year, cur_month)
    ytd_pri_start, ytd_pri_end = ytd_range(cur_year - 1, cur_month)

    cur_monthly = fetch_monthly_sessions(client, property_id, ytd_cur_start, ytd_cur_end)
    pri_monthly = fetch_monthly_sessions(client, property_id, ytd_pri_start, ytd_pri_end)

    ytd_months = []
    for m in range(1, cur_month + 1):
        key      = f"{cur_year}-{str(m).zfill(2)}"
        pri_key  = f"{cur_year - 1}-{str(m).zfill(2)}"
        lbl_date = date(cur_year, m, 1)
        ytd_months.append({
            "month":               key,
            "label":               lbl_date.strftime("%B %Y"),
            "sessions":            cur_monthly.get(key, 0),
            "prior_year_sessions": pri_monthly.get(pri_key, 0),
        })

    ytd_current = sum(m["sessions"] for m in ytd_months)
    ytd_prior   = sum(m["prior_year_sessions"] for m in ytd_months)

    ytd = {
        "current_year":    cur_year,
        "prior_year":      cur_year - 1,
        "months":          ytd_months,
        "ytd_current":     ytd_current,
        "ytd_prior":       ytd_prior,
        "ytd_change_pct":  pct_change(ytd_current, ytd_prior),
    }

    return {
        "property_id":  property_id,
        "display_name": display_name,
        "window": {
            "current": {"label": cur_label, "start": cur_start, "end": cur_end},
            "mom":     {"label": mom_label, "start": mom_start, "end": mom_end},
            "yoy":     {"label": yoy_label, "start": yoy_start, "end": yoy_end},
        },
        "yoy_available": yoy_available,
        "totals":     totals,
        "events":     events,
        "channels":   channels,
        "top_pages":  top_pages,
        "ytd":        ytd,
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="GA4 monthly data pipeline")
    parser.add_argument("--site",    help="Process only this display_name")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print results but do not write ga4_data.json")
    args = parser.parse_args()

    # Load site list from gsc_properties.json (shared config with gsc_analyzer.py)
    if not GSC_PROPERTIES_PATH.exists():
        raise FileNotFoundError(
            f"No gsc_properties.json found at {GSC_PROPERTIES_PATH}"
        )
    with open(GSC_PROPERTIES_PATH) as f:
        all_sites = json.load(f)

    # Only process sites that have a ga4_property_id configured
    properties = [
        {"property_id": s["ga4_property_id"], "display_name": s["display_name"]}
        for s in all_sites
        if s.get("ga4_property_id")
    ]

    if not properties:
        print("⚠️  No sites with 'ga4_property_id' found in gsc_properties.json.")
        print("   Add ga4_property_id to each site entry and re-run.")
        print("   Use discover_ga4_properties.py to look up IDs.")
        return

    skipped = len(all_sites) - len(properties)
    if skipped:
        print(f"ℹ️  Skipping {skipped} site(s) with no ga4_property_id configured.")

    if args.site:
        properties = [p for p in properties
                      if p["display_name"].lower() == args.site.lower()]
        if not properties:
            raise ValueError(f"No GA4-configured property found with display_name '{args.site}'")

    # Build date windows (same for all properties)
    cur_start, cur_end, cur_label = last_full_month()
    mom_start, mom_end, mom_label = month_ago(cur_start, 1)
    yoy_start, yoy_end, yoy_label = month_ago(cur_start, 12)

    print(f"\n📅 Current window : {cur_label} ({cur_start} → {cur_end})")
    print(f"   MoM comparison : {mom_label} ({mom_start} → {mom_end})")
    print(f"   YoY comparison : {yoy_label} ({yoy_start} → {yoy_end})")
    print(f"\n🔌 Connecting to GA4 Data API...")
    client = get_client()

    results = []
    total = len(properties)

    for i, prop in enumerate(properties, 1):
        pid  = prop["property_id"]
        name = prop["display_name"]
        print(f"\n[{i}/{total}] {name} (property: {pid})")

        try:
            result = analyze_property(
                client, pid, name,
                cur_start, cur_end, cur_label,
                mom_start, mom_end, mom_label,
                yoy_start, yoy_end, yoy_label,
            )
            results.append(result)
            status = "✅"
            if not result["yoy_available"]:
                status += " (MoM only — YoY data not yet available)"
            print(f"   {status} Done")
        except Exception as e:
            print(f"   ❌ Failed: {e}")
            results.append({
                "property_id":  pid,
                "display_name": name,
                "error":        str(e),
            })

    output = {
        "generated_at": date.today().isoformat(),
        "window": {
            "current": {"label": cur_label, "start": cur_start, "end": cur_end},
            "mom":     {"label": mom_label, "start": mom_start, "end": mom_end},
            "yoy":     {"label": yoy_label, "start": yoy_start, "end": yoy_end},
        },
        "sites": results,
    }

    if args.dry_run:
        print(f"\n[dry-run] Processed {len(results)} properties. File write skipped.")
        print(json.dumps(output, indent=2)[:2000], "...")
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    ok  = sum(1 for r in results if "error" not in r)
    err = len(results) - ok
    print(f"\n✅ Complete — {ok} properties written to {OUTPUT_PATH}")
    if err:
        print(f"⚠️  {err} propert{'y' if err == 1 else 'ies'} failed — check output above")


if __name__ == "__main__":
    main()
