"""
gsc_analyzer.py
===============
Google Search Console Data Pipeline

Pulls 30-day performance data for all GSC properties listed in
credentials/gsc_properties.json, compares against the prior 30-day
period, and outputs data/gsc_data.json for use by gsc_recommendations.py.

Usage:
    python gsc_analyzer.py
    python gsc_analyzer.py --site "https://example.com/"   # single site
    python gsc_analyzer.py --dry-run                        # output JSON, skip nothing

Requirements:
    pip install google-auth google-auth-httplib2 google-api-python-client python-dotenv --break-system-packages

Setup:
    1. Enable "Google Search Console API" in Google Cloud Console
    2. Add your service account email as a Full user on each GSC property
    3. Create credentials/gsc_properties.json (see format below)

gsc_properties.json format:
    [
      { "site_url": "https://www.example.com/", "display_name": "Example.com" },
      { "site_url": "sc-domain:example.com",    "display_name": "Example (domain)" }
    ]

    Note: URL-prefix properties use "https://www.example.com/"
          Domain properties use "sc-domain:yourdomain.com"
"""

import os
import json
import argparse
import time
from datetime import datetime, timedelta, date
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build

# ── Configuration ──────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent.resolve()
CREDENTIALS_DIR = SCRIPT_DIR / "credentials"
DATA_DIR = SCRIPT_DIR / "data"

SERVICE_ACCOUNT_PATH = CREDENTIALS_DIR / "google_service_account.json"
PROPERTIES_PATH = CREDENTIALS_DIR / "gsc_properties.json"
OUTPUT_PATH = DATA_DIR / "gsc_data.json"

SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]

# How many days in each analysis window
WINDOW_DAYS = 30

# Max rows to pull per dimension query (GSC API limit is 25000)
MAX_ROWS = 500

# Pause between API calls to respect rate limits
API_DELAY = 0.5


# ── Authentication ─────────────────────────────────────────────────────────────

def get_credentials():
    if not SERVICE_ACCOUNT_PATH.exists():
        raise FileNotFoundError(
            f"Service account not found at {SERVICE_ACCOUNT_PATH}\n"
            f"Copy your google_service_account.json file there."
        )
    return service_account.Credentials.from_service_account_file(
        str(SERVICE_ACCOUNT_PATH), scopes=SCOPES
    )


def build_service():
    creds = get_credentials()
    return build("searchconsole", "v1", credentials=creds, cache_discovery=False)


# ── Date helpers ───────────────────────────────────────────────────────────────

def date_range(days_ago_end: int, days_ago_start: int):
    """Returns (start_str, end_str) for a window ending `days_ago_end` days ago."""
    today = date.today()
    end = today - timedelta(days=days_ago_end)
    start = end - timedelta(days=days_ago_start - 1)
    return start.isoformat(), end.isoformat()


def current_window():
    """Last 30 days (ending 3 days ago to allow GSC data to settle)."""
    return date_range(3, WINDOW_DAYS)


def previous_window():
    """Prior 30 days (the window immediately before the current one)."""
    today = date.today()
    end = today - timedelta(days=3 + WINDOW_DAYS)
    start = end - timedelta(days=WINDOW_DAYS - 1)
    return start.isoformat(), end.isoformat()


# ── GSC API helpers ────────────────────────────────────────────────────────────

def query_gsc(service, site_url: str, start_date: str, end_date: str,
              dimensions: list, row_limit: int = MAX_ROWS) -> list:
    """
    Execute a Search Analytics query. Returns list of row dicts.
    Handles pagination automatically.
    """
    rows = []
    start_row = 0
    while True:
        body = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": dimensions,
            "rowLimit": min(row_limit, 25000),
            "startRow": start_row,
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
    """Convert raw GSC row dicts into clean dicts with named dimension keys."""
    result = []
    for row in rows:
        keys = row.get("keys", [])
        entry = {dim: keys[i] if i < len(keys) else None
                 for i, dim in enumerate(dimensions)}
        entry["clicks"] = int(row.get("clicks", 0))
        entry["impressions"] = int(row.get("impressions", 0))
        entry["ctr"] = round(float(row.get("ctr", 0)) * 100, 2)      # as %
        entry["position"] = round(float(row.get("position", 0)), 1)
        result.append(entry)
    return result


# ── Aggregation helpers ────────────────────────────────────────────────────────

def aggregate_totals(rows: list) -> dict:
    """Sum clicks/impressions, average CTR and position across all rows."""
    if not rows:
        return {"clicks": 0, "impressions": 0, "ctr": 0.0, "position": 0.0}
    total_clicks = sum(r["clicks"] for r in rows)
    total_impressions = sum(r["impressions"] for r in rows)
    avg_ctr = (total_clicks / total_impressions * 100) if total_impressions else 0
    avg_pos = (sum(r["position"] * r["impressions"] for r in rows) / total_impressions
               if total_impressions else 0)
    return {
        "clicks": total_clicks,
        "impressions": total_impressions,
        "ctr": round(avg_ctr, 2),
        "position": round(avg_pos, 1),
    }


def trend_label(current_val, previous_val, lower_is_better=False) -> str:
    """Return IMPROVING / WORSENING / STABLE based on percent change."""
    if previous_val is None or previous_val == 0:
        return "NEW"
    pct = (current_val - previous_val) / abs(previous_val) * 100
    improving = pct > 5 if not lower_is_better else pct < -5
    worsening = pct < -5 if not lower_is_better else pct > 5
    if improving:
        return "IMPROVING"
    elif worsening:
        return "WORSENING"
    return "STABLE"


def pct_change(current, previous) -> float | None:
    if previous is None or previous == 0:
        return None
    return round((current - previous) / abs(previous) * 100, 1)


# ── Per-site analysis ─────────────────────────────────────────────────────────

def analyze_site(service, site_url: str, display_name: str) -> dict:
    """Pull and analyze all GSC data for a single property."""
    cur_start, cur_end = current_window()
    prev_start, prev_end = previous_window()

    print(f"  📊 Pulling: {display_name}")
    print(f"     Current:  {cur_start} → {cur_end}")
    print(f"     Previous: {prev_start} → {prev_end}")

    # ── Overall totals ────────────────────────────────────────────────────────
    print(f"     Fetching overall totals...")
    cur_date_rows = query_gsc(service, site_url, cur_start, cur_end, ["date"])
    time.sleep(API_DELAY)
    prev_date_rows = query_gsc(service, site_url, prev_start, prev_end, ["date"])
    time.sleep(API_DELAY)

    cur_totals = aggregate_totals(parse_rows(cur_date_rows, ["date"]))
    prev_totals = aggregate_totals(parse_rows(prev_date_rows, ["date"]))

    # Daily breakdown for sparkline/trend chart data
    cur_by_date = sorted(parse_rows(cur_date_rows, ["date"]), key=lambda r: r["date"])
    daily = [{"date": r["date"], "clicks": r["clicks"], "impressions": r["impressions"],
               "ctr": r["ctr"], "position": r["position"]} for r in cur_by_date]

    # ── Top queries (keywords) ────────────────────────────────────────────────
    print(f"     Fetching top queries...")
    cur_query_rows = query_gsc(service, site_url, cur_start, cur_end, ["query"])
    time.sleep(API_DELAY)
    prev_query_rows = query_gsc(service, site_url, prev_start, prev_end, ["query"])
    time.sleep(API_DELAY)

    cur_queries = {r["query"]: r for r in parse_rows(cur_query_rows, ["query"])}
    prev_queries = {r["query"]: r for r in parse_rows(prev_query_rows, ["query"])}

    top_queries = []
    for q, cur in sorted(cur_queries.items(), key=lambda x: -x[1]["clicks"])[:50]:
        prev = prev_queries.get(q)
        top_queries.append({
            "query": q,
            "clicks": cur["clicks"],
            "impressions": cur["impressions"],
            "ctr": cur["ctr"],
            "position": cur["position"],
            "prev_clicks": prev["clicks"] if prev else None,
            "prev_impressions": prev["impressions"] if prev else None,
            "prev_position": prev["position"] if prev else None,
            "clicks_change_pct": pct_change(cur["clicks"], prev["clicks"] if prev else None),
            "position_change": round(cur["position"] - prev["position"], 1) if prev else None,
            "trend": trend_label(cur["clicks"], prev["clicks"] if prev else None),
        })

    # ── Top pages ─────────────────────────────────────────────────────────────
    print(f"     Fetching top pages...")
    cur_page_rows = query_gsc(service, site_url, cur_start, cur_end, ["page"])
    time.sleep(API_DELAY)
    prev_page_rows = query_gsc(service, site_url, prev_start, prev_end, ["page"])
    time.sleep(API_DELAY)

    cur_pages = {r["page"]: r for r in parse_rows(cur_page_rows, ["page"])}
    prev_pages = {r["page"]: r for r in parse_rows(prev_page_rows, ["page"])}

    top_pages = []
    for page, cur in sorted(cur_pages.items(), key=lambda x: -x[1]["clicks"])[:30]:
        prev = prev_pages.get(page)
        top_pages.append({
            "page": page,
            "clicks": cur["clicks"],
            "impressions": cur["impressions"],
            "ctr": cur["ctr"],
            "position": cur["position"],
            "prev_clicks": prev["clicks"] if prev else None,
            "prev_position": prev["position"] if prev else None,
            "clicks_change_pct": pct_change(cur["clicks"], prev["clicks"] if prev else None),
            "position_change": round(cur["position"] - prev["position"], 1) if prev else None,
            "trend": trend_label(cur["clicks"], prev["clicks"] if prev else None),
        })

    # ── Device breakdown ──────────────────────────────────────────────────────
    print(f"     Fetching device breakdown...")
    device_rows = query_gsc(service, site_url, cur_start, cur_end, ["device"])
    time.sleep(API_DELAY)
    devices = {r["device"]: r for r in parse_rows(device_rows, ["device"])}

    # ── Country breakdown (top 10) ────────────────────────────────────────────
    print(f"     Fetching country breakdown...")
    country_rows = query_gsc(service, site_url, cur_start, cur_end, ["country"],
                              row_limit=10)
    time.sleep(API_DELAY)
    countries = sorted(parse_rows(country_rows, ["country"]),
                       key=lambda r: -r["clicks"])[:10]

    # ── Position buckets (for opportunity analysis) ───────────────────────────
    all_cur_queries = list(cur_queries.values())
    pos_1_3   = [q for q in all_cur_queries if q["position"] <= 3]
    pos_4_10  = [q for q in all_cur_queries if 3 < q["position"] <= 10]
    pos_11_20 = [q for q in all_cur_queries if 10 < q["position"] <= 20]
    pos_21_50 = [q for q in all_cur_queries if 20 < q["position"] <= 50]

    position_buckets = {
        "top_3": {
            "count": len(pos_1_3),
            "clicks": sum(q["clicks"] for q in pos_1_3),
            "impressions": sum(q["impressions"] for q in pos_1_3),
        },
        "pos_4_10": {
            "count": len(pos_4_10),
            "clicks": sum(q["clicks"] for q in pos_4_10),
            "impressions": sum(q["impressions"] for q in pos_4_10),
        },
        "pos_11_20": {
            "count": len(pos_11_20),
            "clicks": sum(q["clicks"] for q in pos_11_20),
            "impressions": sum(q["impressions"] for q in pos_11_20),
        },
        "pos_21_50": {
            "count": len(pos_21_50),
            "clicks": sum(q["clicks"] for q in pos_21_50),
            "impressions": sum(q["impressions"] for q in pos_21_50),
        },
    }

    # ── Declining queries (potential warnings) ────────────────────────────────
    declining_queries = sorted(
        [q for q in top_queries if q["trend"] == "WORSENING"],
        key=lambda q: (q["prev_clicks"] or 0) - q["clicks"],
        reverse=True
    )[:10]

    # ── Rising queries (quick wins / opportunities) ───────────────────────────
    rising_queries = sorted(
        [q for q in top_queries if q["trend"] == "IMPROVING"],
        key=lambda q: q["clicks"],
        reverse=True
    )[:10]

    # ── Page-2 keywords with high impressions (opportunity signals) ───────────
    page2_opportunities = sorted(
        [q for q in all_cur_queries
         if 10 < q["position"] <= 20 and q["impressions"] >= 100],
        key=lambda q: -q["impressions"]
    )[:15]

    # ── Low CTR on high-impression queries ────────────────────────────────────
    low_ctr_queries = sorted(
        [q for q in all_cur_queries
         if q["impressions"] >= 200 and q["ctr"] < 2.0 and q["position"] <= 15],
        key=lambda q: -q["impressions"]
    )[:10]

    return {
        "site_url": site_url,
        "display_name": display_name,
        "analysis_date": datetime.utcnow().isoformat(),
        "window": {
            "current": {"start": cur_start, "end": cur_end},
            "previous": {"start": prev_start, "end": prev_end},
        },
        "totals": {
            "current": cur_totals,
            "previous": prev_totals,
            "clicks_change_pct": pct_change(cur_totals["clicks"], prev_totals["clicks"]),
            "impressions_change_pct": pct_change(cur_totals["impressions"], prev_totals["impressions"]),
            "position_change": round(cur_totals["position"] - prev_totals["position"], 1)
                               if prev_totals["position"] else None,
            "ctr_change": round(cur_totals["ctr"] - prev_totals["ctr"], 2)
                          if prev_totals["ctr"] else None,
            "clicks_trend": trend_label(cur_totals["clicks"], prev_totals["clicks"]),
            "position_trend": trend_label(cur_totals["position"], prev_totals["position"],
                                          lower_is_better=True),
        },
        "daily": daily,
        "top_queries": top_queries,
        "top_pages": top_pages,
        "devices": devices,
        "countries": countries,
        "position_buckets": position_buckets,
        "signals": {
            "declining_queries": declining_queries,
            "rising_queries": rising_queries,
            "page2_opportunities": page2_opportunities,
            "low_ctr_queries": low_ctr_queries,
        },
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="GSC Data Analyzer")
    parser.add_argument("--site", help="Analyze only this site URL (partial match OK)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run analysis but skip writing output file")
    args = parser.parse_args()

    print("\n🔍 GSC Analyzer — Starting\n")

    # Load property list
    if not PROPERTIES_PATH.exists():
        print(f"❌ Properties file not found: {PROPERTIES_PATH}")
        print(f"   Create credentials/gsc_properties.json with your site list.")
        print(f"   See the module docstring for the required format.")
        return

    with open(PROPERTIES_PATH) as f:
        properties = json.load(f)

    print(f"📋 Loaded {len(properties)} properties from gsc_properties.json")

    # Filter to single site if requested
    if args.site:
        properties = [p for p in properties
                      if args.site.lower() in p["site_url"].lower()
                      or args.site.lower() in p.get("display_name", "").lower()]
        if not properties:
            print(f"❌ No properties matched --site '{args.site}'")
            return
        print(f"   (Filtered to {len(properties)} matching site)")

    # Build GSC service
    try:
        service = build_service()
        print("✅ Authenticated with service account\n")
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return

    # Analyze each property
    results = []
    errors = []
    for i, prop in enumerate(properties, 1):
        site_url = prop["site_url"]
        display_name = prop.get("display_name", site_url)
        print(f"[{i}/{len(properties)}] {display_name}")
        try:
            data = analyze_site(service, site_url, display_name)
            results.append(data)
            clicks = data["totals"]["current"]["clicks"]
            chg = data["totals"]["clicks_change_pct"]
            chg_str = (f" ({'+' if chg >= 0 else ''}{chg}%)" if chg is not None else "")
            print(f"  ✅ Done — {clicks:,} clicks{chg_str}\n")
        except Exception as e:
            print(f"  ❌ Failed: {e}\n")
            errors.append({"site_url": site_url, "display_name": display_name, "error": str(e)})
        if i < len(properties):
            time.sleep(1)

    # Assemble output
    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "window_days": WINDOW_DAYS,
        "sites": results,
        "errors": errors,
    }

    # Write JSON
    DATA_DIR.mkdir(exist_ok=True)
    if args.dry_run:
        print("🔍 Dry-run mode — skipping file write")
        print(json.dumps(output, indent=2)[:2000] + "\n... (truncated)")
    else:
        with open(OUTPUT_PATH, "w") as f:
            json.dump(output, f, indent=2)
        print(f"✅ Output written to {OUTPUT_PATH}")
        print(f"   {len(results)} sites analyzed, {len(errors)} errors")

    if errors:
        print("\n⚠️  Errors:")
        for e in errors:
            print(f"   {e['display_name']}: {e['error']}")

    print("\n🏁 Done. Run gsc_recommendations.py next to generate AI analysis.\n")


if __name__ == "__main__":
    main()
