"""
ga4_recommendations.py
======================
AI-Powered GA4 Recommendations Generator — Per-Site Output

Reads per-site data from data/sites/{slug}/ga4.json (produced by ga4_analyzer.py),
sends each site's latest snapshot to Claude Haiku, and writes dual AI analyses to
data/sites/{slug}/ga4_analysis.json.

Each site gets two analyses per run:
  - rolling_30d         : based on the rolling 30-day snapshot
  - last_calendar_month : based on the last full calendar month snapshot

Usage:
    python ga4_recommendations.py
    python ga4_recommendations.py --site "ATI Physical Therapy"
    python ga4_recommendations.py --model claude-haiku-4-5-20251001

Requirements:
    pip install anthropic python-dotenv --break-system-packages
"""

import os
import json
import argparse
import time
from datetime import date
from pathlib import Path

try:
    import anthropic
except ImportError:
    print("❌ anthropic package not installed. Run: pip install anthropic --break-system-packages")
    exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Configuration ──────────────────────────────────────────────────────────────

SCRIPT_DIR      = Path(__file__).parent.resolve()
DATA_DIR        = SCRIPT_DIR / "data"
SITES_DIR       = DATA_DIR / "sites"
PROPERTIES_PATH = SCRIPT_DIR / "credentials" / "gsc_properties.json"

DEFAULT_MODEL = "claude-haiku-4-5-20251001"
API_DELAY     = 1.0


# ── Site file helpers ──────────────────────────────────────────────────────────

def site_slug(url: str) -> str:
    slug = url.lower()
    slug = slug.replace("https://", "").replace("http://", "")
    slug = slug.replace("sc-domain:", "")
    slug = slug.rstrip("/")
    return slug

def load_site_data(slug: str) -> dict | None:
    path = SITES_DIR / slug / "ga4.json"
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)

def save_analysis(slug: str, data: dict):
    path = SITES_DIR / slug / "ga4_analysis.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# ── Prompt builder ─────────────────────────────────────────────────────────────

def build_prompt(site_name: str, window_data: dict, ytd: dict) -> str:
    """Build a GA4 analysis prompt for one window."""
    t             = window_data["totals"]
    cur           = t["current"]
    yoy_available = window_data.get("yoy_available", False)

    cur_label = window_data["window"]["current"]["label"]
    mom_label = window_data["window"].get("mom", {}).get("label", "Prior Period")
    yoy_label = window_data["window"].get("yoy", {}).get("label", "Same Period LY")

    def fmt_pct(val):
        if val is None: return "N/A"
        return f"{'+' if val >= 0 else ''}{val}%"

    def fmt_rate(val):
        if val is None: return "N/A"
        return f"{round(val * 100, 1)}%"

    def fmt_pts(val):
        if val is None: return "N/A"
        return f"{'+' if val >= 0 else ''}{round(val * 100, 1)} pp"

    if yoy_available:
        perf_header  = f"| Metric | {cur_label} | MoM Change | YoY Change |"
        perf_divider = "|--------|------------|------------|------------|"
        perf_rows    = (
            f"| Sessions         | {cur.get('sessions',0):,} | {fmt_pct(t.get('mom_sessions_change_pct'))} | {fmt_pct(t.get('yoy_sessions_change_pct'))} |\n"
            f"| Engaged Sessions | {cur.get('engaged_sessions',0):,} | {fmt_pct(t.get('mom_engaged_sessions_change_pct'))} | {fmt_pct(t.get('yoy_engaged_sessions_change_pct'))} |\n"
            f"| Engagement Rate  | {fmt_rate(cur.get('engagement_rate'))} | {fmt_pts(t.get('mom_engagement_rate_change_pts'))} | {fmt_pts(t.get('yoy_engagement_rate_change_pts'))} |"
        )
        yoy_note    = f"- **YoY comparison:** vs. {yoy_label}"
        yoy_warning = ""
    else:
        perf_header  = f"| Metric | {cur_label} | MoM Change |"
        perf_divider = "|--------|------------|------------|"
        perf_rows    = (
            f"| Sessions         | {cur.get('sessions',0):,} | {fmt_pct(t.get('mom_sessions_change_pct'))} |\n"
            f"| Engaged Sessions | {cur.get('engaged_sessions',0):,} | {fmt_pct(t.get('mom_engaged_sessions_change_pct'))} |\n"
            f"| Engagement Rate  | {fmt_rate(cur.get('engagement_rate'))} | {fmt_pts(t.get('mom_engagement_rate_change_pts'))} |"
        )
        yoy_note    = ""
        yoy_warning = (
            "\n⚠️ Note: YoY data is not available for this property (new GA4 property). "
            "Base analysis on MoM trends only. Do not reference YoY."
        )

    # Events
    events = window_data.get("events", [])
    event_lines = []
    for evt in events[:15]:
        mom_chg = fmt_pct(evt.get("mom_change_pct"))
        line    = f"  - {evt['event_name']}: {evt['current']:,} events | MoM {mom_chg}"
        if yoy_available:
            line += f" | YoY {fmt_pct(evt.get('yoy_change_pct'))}"
        event_lines.append(line)

    # Channels
    channels = window_data.get("channels", [])
    channel_lines = []
    for ch in channels:
        line = (f"  - {ch['channel']}: {ch['current_sessions']:,} sessions "
                f"({ch['current_pct']}% of total) | MoM {fmt_pct(ch.get('mom_sessions_change_pct'))}")
        if yoy_available:
            line += f" | YoY {fmt_pct(ch.get('yoy_sessions_change_pct'))}"
        channel_lines.append(line)

    # Top pages
    top_pages = window_data.get("top_pages", [])
    page_lines = []
    for pg in top_pages[:10]:
        er   = fmt_rate(pg.get("current_engagement_rate"))
        line = (f"  - {pg['landing_page']}: {pg['current_sessions']:,} sessions, "
                f"{pg['current_engaged_sessions']:,} engaged ({er}) | MoM {fmt_pct(pg.get('mom_sessions_change_pct'))}")
        if yoy_available:
            line += f" | YoY {fmt_pct(pg.get('yoy_sessions_change_pct'))}"
        page_lines.append(line)

    # YTD (from snapshot-level ytd, shared across both windows)
    ytd_lines = []
    if ytd:
        months   = ytd.get("months", [])
        cur_year = ytd.get("current_year", "")
        pri_year = ytd.get("prior_year", "")
        ytd_lines.append(
            f"  YTD {cur_year}: {ytd.get('ytd_current',0):,} sessions | "
            f"YTD {pri_year}: {ytd.get('ytd_prior',0):,} sessions | "
            f"Change: {fmt_pct(ytd.get('ytd_change_pct'))}"
        )
        if months:
            ytd_lines.append("\n  Monthly breakdown:")
            for m in months:
                cur_s = m.get("sessions", 0)
                pri_s = m.get("prior_year_sessions", 0)
                chg   = round((cur_s - pri_s) / abs(pri_s) * 100, 1) if pri_s else None
                ytd_lines.append(
                    f"    {m['label']}: {cur_s:,} sessions (vs {pri_s:,} in {pri_year} → {fmt_pct(chg)})"
                )
            # Quarterly
            qtrs = {}
            for m in months:
                month_num = int(m["month"].split("-")[1])
                q = (month_num - 1) // 3 + 1
                qtrs.setdefault(q, {"cur": 0, "pri": 0})
                qtrs[q]["cur"] += m.get("sessions", 0)
                qtrs[q]["pri"] += m.get("prior_year_sessions", 0)
            if qtrs:
                ytd_lines.append("\n  Quarterly summary:")
                for q_num, q_data in sorted(qtrs.items()):
                    chg = round((q_data["cur"] - q_data["pri"]) / abs(q_data["pri"]) * 100, 1) \
                          if q_data["pri"] else None
                    ytd_lines.append(
                        f"    Q{q_num} {cur_year}: {q_data['cur']:,} sessions "
                        f"(vs {q_data['pri']:,} in {pri_year} → {fmt_pct(chg)})"
                    )

    prompt = f"""You are an expert SEO and digital analytics analyst writing a GA4 performance summary for {site_name}.
{yoy_warning}
## Event Glossary
- **navigational_clicks**: Internal clicks within the local pages site — low conversion value
- **outbound_clicks**: Any click off-site to an external URL — moderate value
- **mainsite_clicks**: Clicks specifically to the client's main website — high value, deeper funnel
- **calls**: Phone call initiations — high-value conversion
- **book_now** / **schedule_** / **appointment_**: Booking or scheduling actions — high-value conversion
- **store_locate** / **find_location**: Location finder interactions — high intent, moderate-to-high value
- All other custom events should be interpreted by name and weighted accordingly.

## Reporting Period
- **Window:** {cur_label}
- **MoM comparison:** vs. {mom_label}
{yoy_note}

## Traffic & Engagement KPIs
{perf_header}
{perf_divider}
{perf_rows}

## Custom Events
{chr(10).join(event_lines) if event_lines else '  No custom events recorded'}

## Traffic Channels
{chr(10).join(channel_lines) if channel_lines else '  No channel data'}

## Top Landing Pages
{chr(10).join(page_lines) if page_lines else '  No landing page data'}

## Year-to-Date Traffic
{chr(10).join(ytd_lines) if ytd_lines else '  No YTD data available'}

---

Respond with this JSON structure ONLY — no markdown, no extra text:

{{
  "summary": "Narrative summary covering traffic trends, engagement quality, notable event/conversion movements{', and YTD trajectory' if ytd else ''}. Always use MoM and YoY shorthands. Write as many sentences as the data warrants.",
  "quick_wins": [
    {{"title": "...", "detail": "Specific action referencing actual page, channel, or event by name", "impact": "low|medium|high", "effort": "low|medium|high"}}
  ],
  "potential_warnings": [
    {{"title": "...", "detail": "What is declining, by how much, and why it matters for the business", "severity": "low|medium|high"}}
  ],
  "biggest_opportunities": [
    {{"title": "...", "detail": "Specific opportunity — name the channel, page, or event", "impact": "low|medium|high", "effort": "low|medium|high"}}
  ]
}}

Rules:
- Always use MoM and YoY shorthands — never spell out month-over-month or year-over-year
- Weight mainsite_clicks, calls, book_now, and scheduling events as high-value conversions
- Weight navigational_clicks as low-value; outbound_clicks as moderate
- Be specific — name actual pages, channels, and events from the data
- 3–5 items per category max; omit a category if nothing to flag
- 1–2 sentences per detail field
- Do not invent YoY comparisons if no YoY data was provided
"""
    return prompt


# ── Claude API call ────────────────────────────────────────────────────────────

def get_analysis(client: anthropic.Anthropic, site_name: str,
                 window_data: dict, ytd: dict, model: str) -> dict:
    prompt = build_prompt(site_name, window_data, ytd)
    message = client.messages.create(
        model=model,
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = message.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"): raw = raw[4:]
        raw = raw.strip()
    if raw.endswith("```"):
        raw = raw[:raw.rfind("```")].strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"      ⚠️  JSON parse error: {e}")
        return {"summary": "Analysis could not be parsed.", "quick_wins": [],
                "potential_warnings": [], "biggest_opportunities": [], "_raw": raw[:500]}


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="GA4 AI Recommendations — Per-Site Output")
    parser.add_argument("--site",    help="Process only this site (partial name/URL match)")
    parser.add_argument("--model",   default=DEFAULT_MODEL)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print first analysis without writing files")
    args = parser.parse_args()

    print("\n🤖 GA4 Recommendations — Starting\n")

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("❌ ANTHROPIC_API_KEY not set. Add to .env file.")
        return

    if not PROPERTIES_PATH.exists():
        print(f"❌ Properties file not found: {PROPERTIES_PATH}")
        return

    with open(PROPERTIES_PATH) as f:
        all_properties = json.load(f)

    # Only process sites with GA4 configured
    ga4_properties = [p for p in all_properties if p.get("ga4_property_id")]

    if args.site:
        ga4_properties = [p for p in ga4_properties
                          if args.site.lower() in p.get("display_name", "").lower()
                          or args.site.lower() in p["site_url"].lower()]
        if not ga4_properties:
            print(f"❌ No GA4-configured sites matched --site '{args.site}'")
            return

    # Load per-site data files
    sites_to_process = []
    for prop in ga4_properties:
        slug = site_slug(prop["site_url"])
        data = load_site_data(slug)
        if data is None:
            print(f"  ⏭️  Skipping {prop.get('display_name')} — no ga4.json found")
            continue
        snapshots = data.get("weekly_snapshots", [])
        if not snapshots:
            print(f"  ⏭️  Skipping {prop.get('display_name')} — no snapshots yet")
            continue
        latest = snapshots[-1]
        sites_to_process.append({
            "slug":         slug,
            "site_url":     prop["site_url"],
            "display_name": prop.get("display_name", prop["site_url"]),
            "snapshot":     latest,
        })

    if not sites_to_process:
        print("❌ No sites with snapshot data found. Run ga4_analyzer.py first.")
        return

    print(f"📋 Processing {len(sites_to_process)} sites with model: {args.model}")
    print(f"   Each site = 2 analyses (rolling_30d + last_calendar_month)")
    print(f"   Estimated cost: ~${len(sites_to_process) * 0.006:.2f} (haiku)\n")

    ai_client = anthropic.Anthropic(api_key=api_key)
    processed = 0
    errors    = []

    for i, site in enumerate(sites_to_process, 1):
        name     = site["display_name"]
        slug     = site["slug"]
        snapshot = site["snapshot"]
        ytd      = snapshot.get("ytd", {})

        print(f"[{i}/{len(sites_to_process)}] {name}")

        result = {
            "site_url":     site["site_url"],
            "display_name": name,
            "generated_at": date.today().isoformat(),
            "model":        args.model,
        }

        for window_key in ("rolling_30d", "last_calendar_month"):
            window_data = snapshot.get(window_key)
            if not window_data:
                print(f"    ⚠️  No {window_key} data in snapshot, skipping")
                continue
            try:
                analysis = get_analysis(ai_client, name, window_data, ytd, args.model)
                result[window_key] = {
                    "window":        window_data["window"],
                    "yoy_available": window_data.get("yoy_available", False),
                    "totals":        window_data["totals"],
                    "events":        window_data.get("events", []),
                    "channels":      window_data.get("channels", []),
                    "top_pages":     window_data.get("top_pages", []),
                    "ytd":           ytd,
                    "summary":       analysis.get("summary", ""),
                    "recommendations": {
                        "quick_wins":            analysis.get("quick_wins", []),
                        "potential_warnings":    analysis.get("potential_warnings", []),
                        "biggest_opportunities": analysis.get("biggest_opportunities", []),
                    },
                }
                qw = len(analysis.get("quick_wins", []))
                pw = len(analysis.get("potential_warnings", []))
                bo = len(analysis.get("biggest_opportunities", []))
                print(f"    ✅ {window_key}: {qw} wins, {pw} warnings, {bo} opportunities")
                time.sleep(API_DELAY)
            except Exception as e:
                print(f"    ❌ {window_key} failed: {e}")
                errors.append({"site": name, "window": window_key, "error": str(e)})

        if args.dry_run and i == 1:
            print("\n🔍 Dry-run — first result (truncated):")
            print(json.dumps(result, indent=2)[:2000])
            return

        if not args.dry_run:
            save_analysis(slug, result)
            processed += 1

        print()
        if i < len(sites_to_process):
            time.sleep(API_DELAY)

    print(f"{'🔍 Dry-run complete' if args.dry_run else f'✅ Complete — {processed} sites written'}")
    if errors:
        print(f"\n⚠️  {len(errors)} window-level errors:")
        for e in errors:
            print(f"   {e['site']} ({e['window']}): {e['error']}")
    print("\n🏁 Push data/sites/ to GitHub to update the dashboard.\n")


if __name__ == "__main__":
    main()
