"""
ga4_analyzer.py
===============
Google Analytics 4 Data Pipeline — Historical Append Mode

Pulls GA4 data for all properties in credentials/gsc_properties.json and
appends to per-site JSON files under data/sites/{slug}/ga4.json.

Each weekly run appends:
  - Daily rows (sessions, engaged_sessions, engagement_rate) — deduplicated by date
  - One weekly snapshot covering both:
      * Rolling 30-day window  (default dashboard view)
      * Last full calendar month (monthly comparison view)

Output structure:
    data/sites/{slug}/ga4.json    ← raw data, append-only
    data/sites/manifest.json      ← updated by gsc_analyzer.py (shared)

Usage:
    python ga4_analyzer.py
    python ga4_analyzer.py --site "ATI Physical Therapy"   # single site
    python ga4_analyzer.py --dry-run                        # skip file writes

Requirements:
    pip install google-analytics-data google-auth --break-system-packages
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
    RunReportRequest, DateRange, Dimension, Metric,
    FilterExpression, Filter, FilterExpressionList, OrderBy,
)

# ── Configuration ──────────────────────────────────────────────────────────────

SCRIPT_DIR           = Path(__file__).parent.resolve()
CREDENTIALS_DIR      = SCRIPT_DIR / "credentials"
DATA_DIR             = SCRIPT_DIR / "data"
SITES_DIR            = DATA_DIR / "sites"
SERVICE_ACCOUNT_PATH = CREDENTIALS_DIR / "google_service_account.json"
GSC_PROPERTIES_PATH  = CREDENTIALS_DIR / "gsc_properties.json"

SCOPES         = ["https://www.googleapis.com/auth/analytics.readonly"]
API_DELAY      = 0.5
TOP_PAGES_LIMIT = 10
MAX_SNAPSHOTS  = 52   # ~1 year of weekly snapshots

# Standard GA4 auto-collected events to exclude — everything else is custom/meaningful
GENERIC_EVENTS_EXCLUDE = {
    "first_visit", "session_start", "page_view", "scroll", "user_engagement",
    "click", "all_clicks", "gtm.js", "gtm.dom", "gtm.load",
    "gtm.historyChange", "gtm.historyChange-v2", "js_error",
    "form_start", "video_start", "video_progress", "video_complete", "file_download",
}


# ── Auth ───────────────────────────────────────────────────────────────────────

def get_client():
    if not SERVICE_ACCOUNT_PATH.exists():
        raise FileNotFoundError(f"Service account not found at {SERVICE_ACCOUNT_PATH}")
    creds = service_account.Credentials.from_service_account_file(
        str(SERVICE_ACCOUNT_PATH), scopes=SCOPES
    )
    return BetaAnalyticsDataClient(credentials=creds)


# ── Date helpers ───────────────────────────────────────────────────────────────

def rolling_30d():
    today = date.today()
    end   = today - timedelta(days=1)
    start = end - timedelta(days=29)
    return start.isoformat(), end.isoformat(), "Last 30 Days"

def prior_30d(r30_start_iso: str):
    end   = date.fromisoformat(r30_start_iso) - timedelta(days=1)
    start = end - timedelta(days=29)
    return start.isoformat(), end.isoformat(), "Prior 30 Days"

def same_period_yoy(start_iso: str, end_iso: str):
    start = date.fromisoformat(start_iso) - timedelta(days=365)
    end   = date.fromisoformat(end_iso)   - timedelta(days=365)
    return start.isoformat(), end.isoformat(), "Same Period LY"

def last_full_month():
    today     = date.today()
    last_day  = today.replace(day=1) - timedelta(days=1)
    first_day = last_day.replace(day=1)
    return first_day.isoformat(), last_day.isoformat(), last_day.strftime("%B %Y")

def month_ago(start_iso: str, months: int):
    d     = date.fromisoformat(start_iso)
    month = d.month - months
    year  = d.year
    while month <= 0:
        month += 12
        year  -= 1
    last_day_num = calendar.monthrange(year, month)[1]
    start = date(year, month, 1)
    end   = date(year, month, last_day_num)
    return start.isoformat(), end.isoformat(), end.strftime("%B %Y")

def next_monday() -> str:
    today = date.today()
    days  = (7 - today.weekday()) % 7
    if days == 0:
        days = 7
    return (today + timedelta(days=days)).isoformat()

def ytd_range(year: int, end_month: int):
    last_day_num = calendar.monthrange(year, end_month)[1]
    return date(year, 1, 1).isoformat(), date(year, end_month, last_day_num).isoformat()


# ── Site file helpers ──────────────────────────────────────────────────────────

def site_slug(url: str) -> str:
    slug = url.lower()
    slug = slug.replace("https://", "").replace("http://", "")
    slug = slug.replace("sc-domain:", "")
    slug = slug.rstrip("/")
    return slug

def load_site_file(slug: str) -> dict:
    path = SITES_DIR / slug / "ga4.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {"daily": [], "weekly_snapshots": []}

def save_site_file(slug: str, data: dict):
    path = SITES_DIR / slug / "ga4.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def merge_daily_rows(existing: list, new_rows: list) -> list:
    rows_by_date = {r["date"]: r for r in existing}
    for r in new_rows:
        rows_by_date[r["date"]] = r
    return sorted(rows_by_date.values(), key=lambda r: r["date"])

def append_snapshot(existing: list, new_snapshot: dict) -> list:
    existing.append(new_snapshot)
    return existing[-MAX_SNAPSHOTS:]


# ── Calculation helpers ────────────────────────────────────────────────────────

def pct_change(current, previous):
    if previous is None or previous == 0:
        return None
    return round((current - previous) / abs(previous) * 100, 1)

def safe_float(val, decimals=2):
    try:   return round(float(val), decimals)
    except: return 0.0

def safe_int(val):
    try:   return int(val)
    except: return 0


# ── GA4 API helpers ────────────────────────────────────────────────────────────

def run_report(client, property_id: str, start: str, end: str,
               dimensions: list, metrics: list,
               dimension_filter=None, order_bys=None, limit: int = 100):
    dim_objs    = [Dimension(name=d) for d in dimensions]
    metric_objs = [Metric(name=m) for m in metrics]
    kwargs = dict(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start, end_date=end)],
        dimensions=dim_objs,
        metrics=metric_objs,
        limit=limit,
    )
    if dimension_filter: kwargs["dimension_filter"] = dimension_filter
    if order_bys:        kwargs["order_bys"] = order_bys
    try:
        response = client.run_report(RunReportRequest(**kwargs))
    except Exception as e:
        print(f"      ⚠️  GA4 API error for {property_id}: {e}")
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

def fetch_totals(client, property_id, start, end):
    rows = run_report(client, property_id, start, end,
                      dimensions=[], metrics=["sessions", "engagedSessions", "engagementRate"],
                      limit=1)
    if not rows:
        return {"sessions": 0, "engaged_sessions": 0, "engagement_rate": 0.0}
    r = rows[0]
    return {
        "sessions":         safe_int(r.get("sessions", 0)),
        "engaged_sessions": safe_int(r.get("engagedSessions", 0)),
        "engagement_rate":  safe_float(r.get("engagementRate", 0), decimals=4),
    }

def fetch_daily_sessions(client, property_id, start, end):
    """Fetch daily sessions/engagement for the date range."""
    rows = run_report(client, property_id, start, end,
                      dimensions=["date"],
                      metrics=["sessions", "engagedSessions", "engagementRate"],
                      order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"))],
                      limit=60)
    result = []
    for r in rows:
        result.append({
            "date":             r["date"][:4] + "-" + r["date"][4:6] + "-" + r["date"][6:],
            "sessions":         safe_int(r.get("sessions", 0)),
            "engaged_sessions": safe_int(r.get("engagedSessions", 0)),
            "engagement_rate":  safe_float(r.get("engagementRate", 0), decimals=4),
        })
    return result

def build_event_filter():
    exclude_filters = [
        Filter(field_name="eventName",
               string_filter=Filter.StringFilter(
                   match_type=Filter.StringFilter.MatchType.EXACT,
                   value=evt, case_sensitive=False))
        for evt in GENERIC_EVENTS_EXCLUDE
    ]
    not_filters = [
        FilterExpression(not_expression=FilterExpression(filter=f))
        for f in exclude_filters
    ]
    return FilterExpression(and_group=FilterExpressionList(expressions=not_filters))

def fetch_events(client, property_id, start, end):
    rows = run_report(client, property_id, start, end,
                      dimensions=["eventName"], metrics=["eventCount"],
                      dimension_filter=build_event_filter(),
                      order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="eventCount"),
                                         desc=True)],
                      limit=50)
    return {r["eventName"]: safe_int(r.get("eventCount", 0)) for r in rows}

def fetch_channels(client, property_id, start, end):
    rows = run_report(client, property_id, start, end,
                      dimensions=["sessionDefaultChannelGroup"], metrics=["sessions"],
                      order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"),
                                         desc=True)],
                      limit=20)
    return {r["sessionDefaultChannelGroup"]: safe_int(r.get("sessions", 0)) for r in rows}

def fetch_top_pages(client, property_id, start, end, limit=TOP_PAGES_LIMIT):
    rows = run_report(client, property_id, start, end,
                      dimensions=["landingPage"],
                      metrics=["sessions", "engagedSessions", "engagementRate"],
                      order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"),
                                         desc=True)],
                      limit=limit)
    return {
        r["landingPage"]: {
            "sessions":         safe_int(r.get("sessions", 0)),
            "engaged_sessions": safe_int(r.get("engagedSessions", 0)),
            "engagement_rate":  safe_float(r.get("engagementRate", 0), decimals=4),
        }
        for r in rows
    }

def fetch_monthly_sessions(client, property_id, start, end):
    rows = run_report(client, property_id, start, end,
                      dimensions=["year", "month"], metrics=["sessions"],
                      order_bys=[
                          OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="year")),
                          OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="month")),
                      ],
                      limit=24)
    return {f"{r['year']}-{r['month'].zfill(2)}": safe_int(r.get("sessions", 0)) for r in rows}


# ── Window analyzer ────────────────────────────────────────────────────────────

def analyze_window(client, property_id: str,
                   cur_start: str, cur_end: str, cur_label: str,
                   mom_start: str, mom_end: str, mom_label: str,
                   yoy_start: str, yoy_end: str, yoy_label: str) -> dict:
    """
    Pull and analyze GA4 data for one comparison triple.
    Returns analysis dict with _daily key (used by analyze_property, not stored in snapshot).
    """
    # ── Totals ────────────────────────────────────────────────────────────────
    cur_totals = fetch_totals(client, property_id, cur_start, cur_end)
    mom_totals = fetch_totals(client, property_id, mom_start, mom_end)
    yoy_totals = fetch_totals(client, property_id, yoy_start, yoy_end)

    yoy_available = yoy_totals["sessions"] > 0 or yoy_totals["engaged_sessions"] > 0
    if not yoy_available:
        yoy_totals = None

    totals = {
        "current":   cur_totals,
        "mom":       mom_totals,
        "yoy":       yoy_totals,
        "yoy_available": yoy_available,
        "mom_sessions_change_pct":         pct_change(cur_totals["sessions"],         mom_totals["sessions"]),
        "yoy_sessions_change_pct":         pct_change(cur_totals["sessions"],         yoy_totals["sessions"])         if yoy_available else None,
        "mom_engaged_sessions_change_pct": pct_change(cur_totals["engaged_sessions"], mom_totals["engaged_sessions"]),
        "yoy_engaged_sessions_change_pct": pct_change(cur_totals["engaged_sessions"], yoy_totals["engaged_sessions"]) if yoy_available else None,
        "mom_engagement_rate_change_pts":  round(cur_totals["engagement_rate"] - mom_totals["engagement_rate"], 4),
        "yoy_engagement_rate_change_pts":  round(cur_totals["engagement_rate"] - yoy_totals["engagement_rate"], 4) if yoy_available else None,
    }

    # ── Daily rows (current period only) ─────────────────────────────────────
    daily = fetch_daily_sessions(client, property_id, cur_start, cur_end)

    # ── Events ────────────────────────────────────────────────────────────────
    cur_events = fetch_events(client, property_id, cur_start, cur_end)
    mom_events = fetch_events(client, property_id, mom_start, mom_end)
    yoy_events = fetch_events(client, property_id, yoy_start, yoy_end) if yoy_available else {}

    events = []
    for evt in sorted(cur_events, key=lambda e: cur_events.get(e, 0), reverse=True):
        cur_cnt = cur_events.get(evt, 0)
        mom_cnt = mom_events.get(evt, 0)
        yoy_cnt = yoy_events.get(evt, 0) if yoy_available else None
        events.append({
            "event_name":     evt,
            "current":        cur_cnt,
            "mom":            mom_cnt,
            "yoy":            yoy_cnt,
            "mom_change_pct": pct_change(cur_cnt, mom_cnt),
            "yoy_change_pct": pct_change(cur_cnt, yoy_cnt) if yoy_available else None,
        })

    # ── Channels ──────────────────────────────────────────────────────────────
    cur_channels = fetch_channels(client, property_id, cur_start, cur_end)
    mom_channels = fetch_channels(client, property_id, mom_start, mom_end)
    yoy_channels = fetch_channels(client, property_id, yoy_start, yoy_end) if yoy_available else {}

    total_cur = cur_totals["sessions"] or 1
    channels = []
    for ch in sorted(cur_channels, key=lambda c: cur_channels.get(c, 0), reverse=True):
        cur_s = cur_channels.get(ch, 0)
        mom_s = mom_channels.get(ch, 0)
        yoy_s = yoy_channels.get(ch, 0) if yoy_available else None
        channels.append({
            "channel":               ch,
            "current_sessions":      cur_s,
            "current_pct":           round(cur_s / total_cur * 100, 1),
            "mom_sessions":          mom_s,
            "yoy_sessions":          yoy_s,
            "mom_sessions_change_pct": pct_change(cur_s, mom_s),
            "yoy_sessions_change_pct": pct_change(cur_s, yoy_s) if yoy_available else None,
        })

    # ── Top landing pages ─────────────────────────────────────────────────────
    cur_pages = fetch_top_pages(client, property_id, cur_start, cur_end)
    mom_pages = fetch_top_pages(client, property_id, mom_start, mom_end, limit=50)
    yoy_pages = fetch_top_pages(client, property_id, yoy_start, yoy_end, limit=50) if yoy_available else {}

    top_pages = []
    for path, cur_data in cur_pages.items():
        mom_data = mom_pages.get(path, {})
        yoy_data = yoy_pages.get(path, {}) if yoy_available else {}
        top_pages.append({
            "landing_page":             path,
            "current_sessions":         cur_data["sessions"],
            "current_engaged_sessions": cur_data["engaged_sessions"],
            "current_engagement_rate":  cur_data["engagement_rate"],
            "mom_sessions":             mom_data.get("sessions", 0),
            "yoy_sessions":             yoy_data.get("sessions", 0) if yoy_available else None,
            "mom_sessions_change_pct":  pct_change(cur_data["sessions"], mom_data.get("sessions", 0)),
            "yoy_sessions_change_pct":  pct_change(cur_data["sessions"], yoy_data.get("sessions", 0)) if yoy_available else None,
        })

    return {
        "window": {
            "current": {"start": cur_start, "end": cur_end,  "label": cur_label},
            "mom":     {"start": mom_start, "end": mom_end,  "label": mom_label},
            "yoy":     {"start": yoy_start, "end": yoy_end,  "label": yoy_label},
        },
        "yoy_available": yoy_available,
        "totals":        totals,
        "events":        events,
        "channels":      channels,
        "top_pages":     top_pages,
        "_daily":        daily,   # merged into site file, not stored in snapshot
    }


# ── YTD helper ─────────────────────────────────────────────────────────────────

def fetch_ytd(client, property_id: str, cur_end: str) -> dict:
    """Fetch YTD monthly session totals for current year vs. prior year."""
    cur_year  = date.fromisoformat(cur_end).year
    cur_month = date.fromisoformat(cur_end).month

    ytd_cur_start, ytd_cur_end = ytd_range(cur_year, cur_month)
    ytd_pri_start, ytd_pri_end = ytd_range(cur_year - 1, cur_month)

    cur_monthly = fetch_monthly_sessions(client, property_id, ytd_cur_start, ytd_cur_end)
    pri_monthly = fetch_monthly_sessions(client, property_id, ytd_pri_start, ytd_pri_end)

    ytd_months = []
    for m in range(1, cur_month + 1):
        key     = f"{cur_year}-{str(m).zfill(2)}"
        pri_key = f"{cur_year - 1}-{str(m).zfill(2)}"
        ytd_months.append({
            "month":               key,
            "label":               date(cur_year, m, 1).strftime("%B %Y"),
            "sessions":            cur_monthly.get(key, 0),
            "prior_year_sessions": pri_monthly.get(pri_key, 0),
        })

    ytd_current = sum(m["sessions"] for m in ytd_months)
    ytd_prior   = sum(m["prior_year_sessions"] for m in ytd_months)

    return {
        "current_year": cur_year,
        "prior_year":   cur_year - 1,
        "months":       ytd_months,
        "ytd_current":  ytd_current,
        "ytd_prior":    ytd_prior,
        "ytd_change_pct": pct_change(ytd_current, ytd_prior),
    }


# ── Per-property analysis ──────────────────────────────────────────────────────

def analyze_property(client, property_id: str, site_url: str, display_name: str) -> dict:
    """
    Pull data for both rolling_30d and last_calendar_month windows.
    Returns snapshot dict (for appending) and daily_new rows (for merging).
    """
    r30_start, r30_end, _           = rolling_30d()
    r30_mom_s, r30_mom_e, _         = prior_30d(r30_start)
    r30_yoy_s, r30_yoy_e, _         = same_period_yoy(r30_start, r30_end)
    cal_start,  cal_end,  cal_label = last_full_month()
    mom_start,  mom_end,  mom_label = month_ago(cal_start, 1)
    yoy_start,  yoy_end,  yoy_label = month_ago(cal_start, 12)

    print(f"  📅 Rolling 30d : {r30_start} → {r30_end}")
    print(f"  📅 Last month  : {cal_label} ({cal_start} → {cal_end})")

    print(f"     [1/3] Rolling 30-day window...")
    r30 = analyze_window(client, property_id,
                         r30_start, r30_end, "Last 30 Days",
                         r30_mom_s, r30_mom_e, "Prior 30 Days",
                         r30_yoy_s, r30_yoy_e, "Same Period LY")

    print(f"     [2/3] Last calendar month...")
    cal = analyze_window(client, property_id,
                         cal_start, cal_end, cal_label,
                         mom_start, mom_end, mom_label,
                         yoy_start, yoy_end, yoy_label)

    print(f"     [3/3] YTD sessions...")
    ytd = fetch_ytd(client, property_id, cal_end)

    # Merge daily rows from both windows (deduplicated by date)
    daily_rows = merge_daily_rows(r30.pop("_daily", []), cal.pop("_daily", []))

    return {
        "daily_new": daily_rows,
        "snapshot": {
            "snapshot_date":       date.today().isoformat(),
            "rolling_30d":         r30,
            "last_calendar_month": cal,
            "ytd":                 ytd,
        },
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="GA4 Analyzer — Historical Append Mode")
    parser.add_argument("--site",    help="Process only this display_name (partial match)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print results but skip writing files")
    args = parser.parse_args()

    print("\n📊 GA4 Analyzer — Starting\n")

    if not GSC_PROPERTIES_PATH.exists():
        raise FileNotFoundError(f"No gsc_properties.json found at {GSC_PROPERTIES_PATH}")

    with open(GSC_PROPERTIES_PATH) as f:
        all_sites = json.load(f)

    properties = [
        {"property_id": s["ga4_property_id"],
         "site_url":    s["site_url"],
         "display_name": s["display_name"]}
        for s in all_sites if s.get("ga4_property_id")
    ]

    skipped = len(all_sites) - len(properties)
    if skipped:
        print(f"ℹ️  Skipping {skipped} site(s) with no ga4_property_id.")

    if not properties:
        print("⚠️  No sites with ga4_property_id found.")
        return

    if args.site:
        properties = [p for p in properties
                      if args.site.lower() in p["display_name"].lower()
                      or args.site.lower() in p["site_url"].lower()]
        if not properties:
            print(f"❌ No properties matched --site '{args.site}'")
            return
        print(f"   Filtered to {len(properties)} matching site(s)")

    print(f"📋 Processing {len(properties)} GA4 properties")
    print(f"🔌 Connecting to GA4 Data API...")
    client = get_client()
    print("✅ Authenticated\n")

    errors = []

    for i, prop in enumerate(properties, 1):
        pid          = prop["property_id"]
        site_url     = prop["site_url"]
        display_name = prop["display_name"]
        slug         = site_slug(site_url)

        print(f"[{i}/{len(properties)}] {display_name} (property: {pid})")

        try:
            result = analyze_property(client, pid, site_url, display_name)

            r30_sessions = result["snapshot"]["rolling_30d"]["totals"]["current"]["sessions"]
            cal_sessions = result["snapshot"]["last_calendar_month"]["totals"]["current"]["sessions"]

            if args.dry_run:
                print(f"  🔍 Dry-run — rolling_30d: {r30_sessions:,} sessions | "
                      f"last_cal_month: {cal_sessions:,} sessions\n")
            else:
                existing         = load_site_file(slug)
                merged_daily     = merge_daily_rows(existing.get("daily", []),
                                                    result["daily_new"])
                merged_snapshots = append_snapshot(existing.get("weekly_snapshots", []),
                                                   result["snapshot"])
                updated = {
                    "site_url":         site_url,
                    "display_name":     display_name,
                    "property_id":      pid,
                    "meta": {
                        "last_updated": date.today().isoformat(),
                        "next_refresh": next_monday(),
                    },
                    "daily":            merged_daily,
                    "weekly_snapshots": merged_snapshots,
                }
                save_site_file(slug, updated)
                print(f"  ✅ rolling_30d: {r30_sessions:,} sessions | "
                      f"last_cal_month: {cal_sessions:,} sessions | "
                      f"{len(merged_daily)} daily rows | "
                      f"{len(merged_snapshots)} snapshots\n")

        except Exception as e:
            print(f"  ❌ Failed: {e}\n")
            errors.append({"display_name": display_name, "site_url": site_url, "error": str(e)})

    ok = len(properties) - len(errors)
    print(f"\n{'🔍 Dry-run complete' if args.dry_run else '✅ Complete'} — "
          f"{ok}/{len(properties)} properties succeeded, {len(errors)} errors")
    if errors:
        print("\n⚠️  Errors:")
        for e in errors:
            print(f"   {e['display_name']}: {e['error']}")
    print("\n🏁 Run ga4_recommendations.py next to generate AI analysis.\n")


if __name__ == "__main__":
    main()
