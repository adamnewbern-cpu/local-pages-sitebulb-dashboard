"""
gsc_analyzer.py
===============
Google Search Console Data Pipeline — Historical Append Mode

Pulls GSC data for all properties in credentials/gsc_properties.json and
appends to per-site JSON files under data/sites/{slug}/gsc.json.

Each weekly run appends:
  - Daily rows (clicks, impressions, CTR, position) — deduplicated by date
  - One weekly snapshot covering both:
      * Rolling 30-day window  (default dashboard view)
      * Last full calendar month (monthly comparison view)

Output structure:
    data/sites/{slug}/gsc.json    ← raw data, append-only
    data/sites/manifest.json      ← site index for dashboard lazy-loading

Usage:
    python gsc_analyzer.py
    python gsc_analyzer.py --site "ATI Physical Therapy"   # single site
    python gsc_analyzer.py --dry-run                        # skip file writes

Requirements:
    pip install google-auth google-auth-httplib2 google-api-python-client python-dotenv --break-system-packages
"""

import os
import json
import argparse
import time
import calendar
from datetime import datetime, timedelta, date
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build

# ── Configuration ──────────────────────────────────────────────────────────────

SCRIPT_DIR           = Path(__file__).parent.resolve()
CREDENTIALS_DIR      = SCRIPT_DIR / "credentials"
DATA_DIR             = SCRIPT_DIR / "data"
SITES_DIR            = DATA_DIR / "sites"
MANIFEST_PATH        = SITES_DIR / "manifest.json"
SERVICE_ACCOUNT_PATH = CREDENTIALS_DIR / "google_service_account.json"
PROPERTIES_PATH      = CREDENTIALS_DIR / "gsc_properties.json"

SCOPES        = ["https://www.googleapis.com/auth/webmasters.readonly"]
MAX_ROWS      = 500
API_DELAY     = 0.5
MAX_SNAPSHOTS = 52   # ~1 year of weekly snapshots per site


# ── Authentication ─────────────────────────────────────────────────────────────

def get_credentials():
    if not SERVICE_ACCOUNT_PATH.exists():
        raise FileNotFoundError(f"Service account not found at {SERVICE_ACCOUNT_PATH}")
    return service_account.Credentials.from_service_account_file(
        str(SERVICE_ACCOUNT_PATH), scopes=SCOPES
    )

def build_service():
    return build("searchconsole", "v1", credentials=get_credentials(), cache_discovery=False)


# ── Date helpers ───────────────────────────────────────────────────────────────

def rolling_30d():
    """Last 30 complete days ending yesterday."""
    today = date.today()
    end   = today - timedelta(days=1)
    start = end - timedelta(days=29)
    return start.isoformat(), end.isoformat(), "Last 30 Days"

def prior_30d(r30_start_iso: str):
    """The 30 days immediately before the rolling_30d window."""
    end   = date.fromisoformat(r30_start_iso) - timedelta(days=1)
    start = end - timedelta(days=29)
    return start.isoformat(), end.isoformat(), "Prior 30 Days"

def same_period_yoy(start_iso: str, end_iso: str):
    """Same date range shifted back 365 days."""
    start = date.fromisoformat(start_iso) - timedelta(days=365)
    end   = date.fromisoformat(end_iso)   - timedelta(days=365)
    return start.isoformat(), end.isoformat(), "Same Period LY"

def last_full_month():
    """Most recently completed calendar month."""
    today     = date.today()
    last_day  = today.replace(day=1) - timedelta(days=1)
    first_day = last_day.replace(day=1)
    return first_day.isoformat(), last_day.isoformat(), last_day.strftime("%B %Y")

def month_ago(start_iso: str, months: int):
    """Shift a month-start date back N months."""
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
    """ISO date of the coming Monday (next scheduled refresh)."""
    today = date.today()
    days  = (7 - today.weekday()) % 7
    if days == 0:
        days = 7
    return (today + timedelta(days=days)).isoformat()


# ── Site file helpers ──────────────────────────────────────────────────────────

def site_slug(url: str) -> str:
    """Convert a site URL to a safe directory name."""
    slug = url.lower()
    slug = slug.replace("https://", "").replace("http://", "")
    slug = slug.replace("sc-domain:", "")
    slug = slug.rstrip("/")
    return slug

def load_site_file(slug: str) -> dict:
    path = SITES_DIR / slug / "gsc.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {"daily": [], "weekly_snapshots": []}

def save_site_file(slug: str, data: dict):
    path = SITES_DIR / slug / "gsc.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def merge_daily_rows(existing: list, new_rows: list) -> list:
    """Merge new daily rows into existing, deduplicating by date."""
    rows_by_date = {r["date"]: r for r in existing}
    for r in new_rows:
        rows_by_date[r["date"]] = r
    return sorted(rows_by_date.values(), key=lambda r: r["date"])

def append_snapshot(existing: list, new_snapshot: dict) -> list:
    """Add snapshot and trim to MAX_SNAPSHOTS."""
    existing.append(new_snapshot)
    return existing[-MAX_SNAPSHOTS:]


# ── GSC API helpers ────────────────────────────────────────────────────────────

def query_gsc(service, site_url: str, start_date: str, end_date: str,
              dimensions: list, row_limit: int = MAX_ROWS) -> list:
    rows = []
    start_row = 0
    while True:
        body = {
            "startDate":  start_date,
            "endDate":    end_date,
            "dimensions": dimensions,
            "rowLimit":   min(row_limit, 25000),
            "startRow":   start_row,
        }
        try:
            response = (
                service.searchanalytics()
                .query(siteUrl=site_url, body=body)
                .execute()
            )
        except Exception as e:
            print(f"      ⚠️  API error: {e}")
            break
        batch = response.get("rows", [])
        rows.extend(batch)
        if len(batch) < body["rowLimit"] or len(rows) >= row_limit:
            break
        start_row += len(batch)
        time.sleep(API_DELAY)
    return rows

def parse_rows(rows: list, dimensions: list) -> list:
    result = []
    for row in rows:
        keys  = row.get("keys", [])
        entry = {dim: keys[i] if i < len(keys) else None
                 for i, dim in enumerate(dimensions)}
        entry["clicks"]      = int(row.get("clicks", 0))
        entry["impressions"] = int(row.get("impressions", 0))
        entry["ctr"]         = round(float(row.get("ctr", 0)) * 100, 2)
        entry["position"]    = round(float(row.get("position", 0)), 1)
        result.append(entry)
    return result

def aggregate_totals(rows: list) -> dict:
    if not rows:
        return {"clicks": 0, "impressions": 0, "ctr": 0.0, "position": 0.0}
    total_clicks      = sum(r["clicks"] for r in rows)
    total_impressions = sum(r["impressions"] for r in rows)
    avg_ctr = (total_clicks / total_impressions * 100) if total_impressions else 0
    avg_pos = (sum(r["position"] * r["impressions"] for r in rows) / total_impressions
               if total_impressions else 0)
    return {
        "clicks":      total_clicks,
        "impressions": total_impressions,
        "ctr":         round(avg_ctr, 2),
        "position":    round(avg_pos, 1),
    }


# ── Calculation helpers ────────────────────────────────────────────────────────

def pct_change(current, previous):
    if previous is None or previous == 0:
        return None
    return round((current - previous) / abs(previous) * 100, 1)

def trend_label(current_val, previous_val, lower_is_better=False) -> str:
    if previous_val is None or previous_val == 0:
        return "NEW"
    pct = (current_val - previous_val) / abs(previous_val) * 100
    improving = pct > 5 if not lower_is_better else pct < -5
    worsening = pct < -5 if not lower_is_better else pct > 5
    if improving:   return "IMPROVING"
    elif worsening: return "WORSENING"
    return "STABLE"


# ── Window analyzer ────────────────────────────────────────────────────────────

def analyze_window(service, site_url: str,
                   cur_start: str, cur_end: str, cur_label: str,
                   mom_start: str, mom_end: str, mom_label: str,
                   yoy_start: str, yoy_end: str, yoy_label: str) -> dict:
    """
    Pull and analyze GSC data for one comparison triple (current / mom / yoy).
    Returns analysis dict including a _daily key (used by analyze_site, not stored in snapshot).
    """
    # ── Totals (3 periods) ────────────────────────────────────────────────────
    cur_date_rows = query_gsc(service, site_url, cur_start, cur_end, ["date"])
    time.sleep(API_DELAY)
    mom_date_rows = query_gsc(service, site_url, mom_start, mom_end, ["date"])
    time.sleep(API_DELAY)
    yoy_date_rows = query_gsc(service, site_url, yoy_start, yoy_end, ["date"])
    time.sleep(API_DELAY)

    cur_totals = aggregate_totals(parse_rows(cur_date_rows, ["date"]))
    mom_totals = aggregate_totals(parse_rows(mom_date_rows, ["date"]))
    yoy_totals = aggregate_totals(parse_rows(yoy_date_rows, ["date"]))

    yoy_available = yoy_totals["clicks"] > 0 or yoy_totals["impressions"] > 0

    # Daily rows for current period (internal — merged into site file)
    cur_by_date = sorted(parse_rows(cur_date_rows, ["date"]), key=lambda r: r["date"])
    daily = [
        {"date": r["date"], "clicks": r["clicks"],
         "impressions": r["impressions"], "ctr": r["ctr"], "position": r["position"]}
        for r in cur_by_date
    ]

    # ── Top queries ───────────────────────────────────────────────────────────
    cur_query_rows = query_gsc(service, site_url, cur_start, cur_end, ["query"])
    time.sleep(API_DELAY)
    mom_query_rows = query_gsc(service, site_url, mom_start, mom_end, ["query"])
    time.sleep(API_DELAY)
    yoy_query_rows = (query_gsc(service, site_url, yoy_start, yoy_end, ["query"])
                      if yoy_available else [])
    if yoy_available:
        time.sleep(API_DELAY)

    cur_queries = {r["query"]: r for r in parse_rows(cur_query_rows, ["query"])}
    mom_queries = {r["query"]: r for r in parse_rows(mom_query_rows, ["query"])}
    yoy_queries = {r["query"]: r for r in parse_rows(yoy_query_rows, ["query"])}

    top_queries = []
    for q, cur in sorted(cur_queries.items(), key=lambda x: -x[1]["clicks"])[:50]:
        mom = mom_queries.get(q)
        yoy = yoy_queries.get(q)
        top_queries.append({
            "query":                 q,
            "clicks":                cur["clicks"],
            "impressions":           cur["impressions"],
            "ctr":                   cur["ctr"],
            "position":              cur["position"],
            "mom_clicks":            mom["clicks"]   if mom else None,
            "mom_position":          mom["position"] if mom else None,
            "mom_clicks_change_pct": pct_change(cur["clicks"], mom["clicks"] if mom else None),
            "mom_position_change":   round(cur["position"] - mom["position"], 1) if mom else None,
            "trend":                 trend_label(cur["clicks"], mom["clicks"] if mom else None),
            "yoy_clicks":            yoy["clicks"]   if yoy else None,
            "yoy_position":          yoy["position"] if yoy else None,
            "yoy_clicks_change_pct": pct_change(cur["clicks"], yoy["clicks"] if yoy else None),
            "yoy_position_change":   round(cur["position"] - yoy["position"], 1) if yoy else None,
            "yoy_trend":             trend_label(cur["clicks"], yoy["clicks"] if yoy else None),
        })

    # ── Top pages ─────────────────────────────────────────────────────────────
    cur_page_rows = query_gsc(service, site_url, cur_start, cur_end, ["page"])
    time.sleep(API_DELAY)
    mom_page_rows = query_gsc(service, site_url, mom_start, mom_end, ["page"])
    time.sleep(API_DELAY)
    yoy_page_rows = (query_gsc(service, site_url, yoy_start, yoy_end, ["page"])
                     if yoy_available else [])
    if yoy_available:
        time.sleep(API_DELAY)

    cur_pages = {r["page"]: r for r in parse_rows(cur_page_rows, ["page"])}
    mom_pages = {r["page"]: r for r in parse_rows(mom_page_rows, ["page"])}
    yoy_pages = {r["page"]: r for r in parse_rows(yoy_page_rows, ["page"])}

    top_pages = []
    for page, cur in sorted(cur_pages.items(), key=lambda x: -x[1]["clicks"])[:30]:
        mom = mom_pages.get(page)
        yoy = yoy_pages.get(page)
        top_pages.append({
            "page":                  page,
            "clicks":                cur["clicks"],
            "impressions":           cur["impressions"],
            "ctr":                   cur["ctr"],
            "position":              cur["position"],
            "mom_clicks":            mom["clicks"]   if mom else None,
            "mom_clicks_change_pct": pct_change(cur["clicks"], mom["clicks"] if mom else None),
            "mom_position_change":   round(cur["position"] - mom["position"], 1) if mom else None,
            "yoy_clicks":            yoy["clicks"]   if yoy else None,
            "yoy_clicks_change_pct": pct_change(cur["clicks"], yoy["clicks"] if yoy else None),
            "yoy_position_change":   round(cur["position"] - yoy["position"], 1) if yoy else None,
            "trend":                 trend_label(cur["clicks"], mom["clicks"] if mom else None),
            "yoy_trend":             trend_label(cur["clicks"], yoy["clicks"] if yoy else None),
        })

    # ── Position buckets ──────────────────────────────────────────────────────
    all_cur = list(cur_queries.values())
    all_yoy = list(yoy_queries.values())

    def bucket(rows, lo, hi):
        return [q for q in rows if lo <= q["position"] <= hi]

    position_buckets = {
        "top_3":     {"count": len(bucket(all_cur, 0, 3)),
                      "clicks": sum(q["clicks"] for q in bucket(all_cur, 0, 3)),
                      "impressions": sum(q["impressions"] for q in bucket(all_cur, 0, 3)),
                      "yoy_count": len(bucket(all_yoy, 0, 3))},
        "pos_4_10":  {"count": len(bucket(all_cur, 4, 10)),
                      "clicks": sum(q["clicks"] for q in bucket(all_cur, 4, 10)),
                      "impressions": sum(q["impressions"] for q in bucket(all_cur, 4, 10)),
                      "yoy_count": len(bucket(all_yoy, 4, 10))},
        "pos_11_20": {"count": len(bucket(all_cur, 11, 20)),
                      "clicks": sum(q["clicks"] for q in bucket(all_cur, 11, 20)),
                      "impressions": sum(q["impressions"] for q in bucket(all_cur, 11, 20)),
                      "yoy_count": len(bucket(all_yoy, 11, 20))},
        "pos_21_50": {"count": len(bucket(all_cur, 21, 50)),
                      "clicks": sum(q["clicks"] for q in bucket(all_cur, 21, 50)),
                      "impressions": sum(q["impressions"] for q in bucket(all_cur, 21, 50))},
    }

    # ── Signals ───────────────────────────────────────────────────────────────
    signals = {
        "declining_queries": sorted(
            [q for q in top_queries if q["trend"] == "WORSENING"],
            key=lambda q: (q["mom_clicks"] or 0) - q["clicks"], reverse=True
        )[:10],
        "rising_queries": sorted(
            [q for q in top_queries if q["trend"] == "IMPROVING"],
            key=lambda q: q["clicks"], reverse=True
        )[:10],
        "page2_opportunities": sorted(
            [q for q in all_cur if 10 < q["position"] <= 20 and q["impressions"] >= 100],
            key=lambda q: -q["impressions"]
        )[:15],
        "low_ctr_queries": sorted(
            [q for q in all_cur if q["impressions"] >= 200 and q["ctr"] < 2.0
             and q["position"] <= 15],
            key=lambda q: -q["impressions"]
        )[:10],
    }

    # ── Totals block ──────────────────────────────────────────────────────────
    totals = {
        "current":   cur_totals,
        "mom":       mom_totals,
        "yoy":       yoy_totals if yoy_available else None,
        "yoy_available": yoy_available,
        "mom_clicks_change_pct":      pct_change(cur_totals["clicks"],      mom_totals["clicks"]),
        "mom_impressions_change_pct": pct_change(cur_totals["impressions"], mom_totals["impressions"]),
        "mom_position_change":        round(cur_totals["position"] - mom_totals["position"], 1)
                                      if mom_totals["position"] else None,
        "mom_ctr_change":             round(cur_totals["ctr"] - mom_totals["ctr"], 2)
                                      if mom_totals["ctr"] else None,
        "yoy_clicks_change_pct":      pct_change(cur_totals["clicks"],      yoy_totals["clicks"])
                                      if yoy_available else None,
        "yoy_impressions_change_pct": pct_change(cur_totals["impressions"], yoy_totals["impressions"])
                                      if yoy_available else None,
        "yoy_position_change":        round(cur_totals["position"] - yoy_totals["position"], 1)
                                      if (yoy_available and yoy_totals["position"]) else None,
        "yoy_ctr_change":             round(cur_totals["ctr"] - yoy_totals["ctr"], 2)
                                      if (yoy_available and yoy_totals["ctr"]) else None,
        "clicks_trend":               trend_label(cur_totals["clicks"],   mom_totals["clicks"]),
        "position_trend":             trend_label(cur_totals["position"], mom_totals["position"],
                                                  lower_is_better=True),
    }

    return {
        "window": {
            "current": {"start": cur_start, "end": cur_end,  "label": cur_label},
            "mom":     {"start": mom_start, "end": mom_end,  "label": mom_label},
            "yoy":     {"start": yoy_start, "end": yoy_end,  "label": yoy_label},
        },
        "yoy_available":    yoy_available,
        "totals":           totals,
        "top_queries":      top_queries,
        "top_pages":        top_pages,
        "position_buckets": position_buckets,
        "signals":          signals,
        "_daily":           daily,   # merged into site file, not stored in snapshot
    }


# ── Per-site analysis ─────────────────────────────────────────────────────────

def analyze_site(service, site_url: str, display_name: str) -> dict:
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
    r30 = analyze_window(service, site_url,
                         r30_start, r30_end, "Last 30 Days",
                         r30_mom_s, r30_mom_e, "Prior 30 Days",
                         r30_yoy_s, r30_yoy_e, "Same Period LY")

    print(f"     [2/3] Last calendar month...")
    cal = analyze_window(service, site_url,
                         cal_start, cal_end, cal_label,
                         mom_start, mom_end, mom_label,
                         yoy_start, yoy_end, yoy_label)

    print(f"     [3/3] Devices & countries...")
    device_rows  = query_gsc(service, site_url, cal_start, cal_end, ["device"])
    time.sleep(API_DELAY)
    country_rows = query_gsc(service, site_url, cal_start, cal_end, ["country"], row_limit=10)
    time.sleep(API_DELAY)

    devices   = {r["device"]: r for r in parse_rows(device_rows, ["device"])}
    countries = sorted(parse_rows(country_rows, ["country"]),
                       key=lambda r: -r["clicks"])[:10]

    # Merge daily rows from both windows (deduplicated by date)
    daily_rows = merge_daily_rows(r30.pop("_daily", []), cal.pop("_daily", []))

    return {
        "daily_new": daily_rows,
        "snapshot": {
            "snapshot_date":       date.today().isoformat(),
            "rolling_30d":         r30,
            "last_calendar_month": cal,
            "devices":             devices,
            "countries":           countries,
        },
    }


# ── Manifest ───────────────────────────────────────────────────────────────────

def update_manifest(all_properties: list, updated_slugs: set):
    """Write data/sites/manifest.json for dashboard site selector."""
    sites = []
    for prop in all_properties:
        url  = prop["site_url"]
        slug = site_slug(url)
        # Check if per-site file exists (may have been written on a previous run)
        site_file = SITES_DIR / slug / "gsc.json"
        last_updated = None
        if site_file.exists():
            try:
                with open(site_file) as f:
                    d = json.load(f)
                last_updated = d.get("meta", {}).get("last_updated")
            except Exception:
                pass
        sites.append({
            "site_url":        url,
            "display_name":    prop.get("display_name", url),
            "slug":            slug,
            "ga4_property_id": prop.get("ga4_property_id"),
            "last_updated":    last_updated,
            "next_refresh":    next_monday(),
        })

    manifest = {
        "generated_at": date.today().isoformat(),
        "next_refresh":  next_monday(),
        "sites":         sites,
    }
    SITES_DIR.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"  📋 Manifest updated ({len(sites)} sites) → {MANIFEST_PATH}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="GSC Analyzer — Historical Append Mode")
    parser.add_argument("--site",    help="Analyze only this site (partial name/URL match)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run analysis but skip writing output files")
    args = parser.parse_args()

    print("\n🔍 GSC Analyzer — Starting\n")

    if not PROPERTIES_PATH.exists():
        print(f"❌ Properties file not found: {PROPERTIES_PATH}")
        return

    with open(PROPERTIES_PATH) as f:
        all_properties = json.load(f)

    properties = all_properties[:]
    if args.site:
        properties = [p for p in properties
                      if args.site.lower() in p["site_url"].lower()
                      or args.site.lower() in p.get("display_name", "").lower()]
        if not properties:
            print(f"❌ No properties matched --site '{args.site}'")
            return
        print(f"   Filtered to {len(properties)} matching site(s)")

    print(f"📋 Loaded {len(properties)} properties")

    try:
        service = build_service()
        print("✅ Authenticated\n")
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return

    updated_slugs = set()
    errors = []

    for i, prop in enumerate(properties, 1):
        site_url     = prop["site_url"]
        display_name = prop.get("display_name", site_url)
        slug         = site_slug(site_url)

        print(f"[{i}/{len(properties)}] {display_name}")

        try:
            result = analyze_site(service, site_url, display_name)

            r30_clicks = result["snapshot"]["rolling_30d"]["totals"]["current"]["clicks"]
            cal_clicks = result["snapshot"]["last_calendar_month"]["totals"]["current"]["clicks"]

            if args.dry_run:
                print(f"  🔍 Dry-run — rolling_30d: {r30_clicks:,} clicks | "
                      f"last_cal_month: {cal_clicks:,} clicks\n")
            else:
                existing         = load_site_file(slug)
                merged_daily     = merge_daily_rows(existing.get("daily", []),
                                                    result["daily_new"])
                merged_snapshots = append_snapshot(existing.get("weekly_snapshots", []),
                                                   result["snapshot"])
                updated = {
                    "site_url":         site_url,
                    "display_name":     display_name,
                    "meta": {
                        "last_updated": date.today().isoformat(),
                        "next_refresh": next_monday(),
                    },
                    "daily":            merged_daily,
                    "weekly_snapshots": merged_snapshots,
                }
                save_site_file(slug, updated)
                updated_slugs.add(slug)
                print(f"  ✅ rolling_30d: {r30_clicks:,} clicks | "
                      f"last_cal_month: {cal_clicks:,} clicks | "
                      f"{len(merged_daily)} daily rows | "
                      f"{len(merged_snapshots)} snapshots\n")

        except Exception as e:
            print(f"  ❌ Failed: {e}\n")
            errors.append({"site_url": site_url, "display_name": display_name, "error": str(e)})

        if i < len(properties):
            time.sleep(1)

    if not args.dry_run:
        update_manifest(all_properties, updated_slugs)

    total = len(properties)
    ok    = total - len(errors)
    print(f"\n{'🔍 Dry-run complete' if args.dry_run else '✅ Complete'} — "
          f"{ok}/{total} sites succeeded, {len(errors)} errors")
    if errors:
        print("\n⚠️  Errors:")
        for e in errors:
            print(f"   {e['display_name']}: {e['error']}")
    print("\n🏁 Run gsc_recommendations.py next to generate AI analysis.\n")


if __name__ == "__main__":
    main()
