"""
ga4_recommendations.py
======================
AI-Powered GA4 Recommendations Generator

Reads data/ga4_data.json (produced by ga4_analyzer.py), sends each site's
data to the Claude API, and outputs structured recommendations to
data/ga4_recommendations.json for display in the dashboard.

Usage:
    python ga4_recommendations.py
    python ga4_recommendations.py --site "ATI Physical Therapy"  # single site
    python ga4_recommendations.py --model claude-haiku-4-5-20251001

Requirements:
    pip install anthropic python-dotenv --break-system-packages

Setup:
    Store your Anthropic API key in a .env file in this folder:
        ANTHROPIC_API_KEY=sk-ant-...
"""

import os
import json
import argparse
import time
from datetime import datetime
from pathlib import Path

try:
    import anthropic
except ImportError:
    print("❌ anthropic package not installed.")
    print("   Run: pip install anthropic --break-system-packages")
    exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Configuration ──────────────────────────────────────────────────────────────

SCRIPT_DIR    = Path(__file__).parent.resolve()
DATA_DIR      = SCRIPT_DIR / "data"
GA4_DATA_PATH = DATA_DIR / "ga4_data.json"
OUTPUT_PATH   = DATA_DIR / "ga4_recommendations.json"

DEFAULT_MODEL = "claude-haiku-4-5-20251001"
API_DELAY     = 1.0


# ── Prompt builder ─────────────────────────────────────────────────────────────

def build_prompt(site: dict) -> str:
    """
    Build a structured GA4 analysis prompt.
    Covers engagement KPIs, custom events, channels, top pages, and YTD trends.
    Adapts gracefully when YoY data is unavailable (new properties).
    """
    t             = site["totals"]
    cur           = t["current"]
    mom           = t.get("mom", {})
    yoy_available = site.get("yoy_available", False)

    cur_label = site["window"]["current"]["label"]
    mom_label = site["window"].get("mom", {}).get("label", "Prior Month")
    yoy_label = site["window"].get("yoy", {}).get("label", "Same Month LY")

    def fmt_pct(val):
        if val is None:
            return "N/A"
        return f"{'+' if val >= 0 else ''}{val}%"

    def fmt_rate(val):
        """Format engagement rate (stored as decimal 0–1) as percentage."""
        if val is None:
            return "N/A"
        return f"{round(val * 100, 1)}%"

    def fmt_pts(val):
        """Format engagement rate delta in percentage points."""
        if val is None:
            return "N/A"
        return f"{'+' if val >= 0 else ''}{round(val * 100, 1)} pp"

    # ── Performance table ──────────────────────────────────────────────────────
    if yoy_available:
        perf_header  = f"| Metric | {cur_label} | MoM Change | YoY Change |"
        perf_divider = "|--------|------------|------------|------------|"
        perf_rows = (
            f"| Sessions         | {cur.get('sessions', 0):,} "
            f"| {fmt_pct(t.get('mom_sessions_change_pct'))} "
            f"| {fmt_pct(t.get('yoy_sessions_change_pct'))} |\n"
            f"| Engaged Sessions | {cur.get('engaged_sessions', 0):,} "
            f"| {fmt_pct(t.get('mom_engaged_sessions_change_pct'))} "
            f"| {fmt_pct(t.get('yoy_engaged_sessions_change_pct'))} |\n"
            f"| Engagement Rate  | {fmt_rate(cur.get('engagement_rate'))} "
            f"| {fmt_pts(t.get('mom_engagement_rate_change_pts'))} "
            f"| {fmt_pts(t.get('yoy_engagement_rate_change_pts'))} |"
        )
        yoy_note    = f"- **YoY comparison:** vs. {yoy_label}"
        yoy_warning = ""
    else:
        perf_header  = f"| Metric | {cur_label} | MoM Change |"
        perf_divider = "|--------|------------|------------|"
        perf_rows = (
            f"| Sessions         | {cur.get('sessions', 0):,} "
            f"| {fmt_pct(t.get('mom_sessions_change_pct'))} |\n"
            f"| Engaged Sessions | {cur.get('engaged_sessions', 0):,} "
            f"| {fmt_pct(t.get('mom_engaged_sessions_change_pct'))} |\n"
            f"| Engagement Rate  | {fmt_rate(cur.get('engagement_rate'))} "
            f"| {fmt_pts(t.get('mom_engagement_rate_change_pts'))} |"
        )
        yoy_note    = ""
        yoy_warning = (
            "\n⚠️ Note: YoY data is not available for this property "
            "(new GA4 property or insufficient historical data). "
            "Base your analysis on MoM trends only and do not reference "
            "year-over-year comparisons in your summary or recommendations."
        )

    # ── Custom events table ────────────────────────────────────────────────────
    events = site.get("events", [])
    event_lines = []
    for evt in events[:15]:
        mom_chg = fmt_pct(evt.get("mom_change_pct"))
        yoy_chg = fmt_pct(evt.get("yoy_change_pct")) if yoy_available else None
        line = f"  - {evt['event_name']}: {evt['current']:,} events"
        if yoy_available:
            line += f" | MoM {mom_chg} | YoY {yoy_chg}"
        else:
            line += f" | MoM {mom_chg}"
        event_lines.append(line)

    # ── Traffic channels ───────────────────────────────────────────────────────
    channels = site.get("channels", [])
    channel_lines = []
    for ch in channels:
        mom_chg = fmt_pct(ch.get("mom_sessions_change_pct"))
        yoy_chg = fmt_pct(ch.get("yoy_sessions_change_pct")) if yoy_available else None
        line = (
            f"  - {ch['channel']}: {ch['current_sessions']:,} sessions "
            f"({ch['current_pct']}% of total)"
        )
        if yoy_available:
            line += f" | MoM {mom_chg} | YoY {yoy_chg}"
        else:
            line += f" | MoM {mom_chg}"
        channel_lines.append(line)

    # ── Top landing pages ──────────────────────────────────────────────────────
    top_pages = site.get("top_pages", [])
    page_lines = []
    for pg in top_pages[:10]:
        mom_chg = fmt_pct(pg.get("mom_sessions_change_pct"))
        yoy_chg = fmt_pct(pg.get("yoy_sessions_change_pct")) if yoy_available else None
        er = fmt_rate(pg.get("current_engagement_rate"))
        line = (
            f"  - {pg['landing_page']}: {pg['current_sessions']:,} sessions, "
            f"{pg['current_engaged_sessions']:,} engaged ({er})"
        )
        if yoy_available:
            line += f" | MoM {mom_chg} | YoY {yoy_chg}"
        else:
            line += f" | MoM {mom_chg}"
        page_lines.append(line)

    # ── YTD summary ────────────────────────────────────────────────────────────
    ytd      = site.get("ytd", {})
    months   = ytd.get("months", [])
    cur_year = ytd.get("current_year", "")
    pri_year = ytd.get("prior_year", "")

    ytd_lines = []
    if months:
        ytd_lines.append(
            f"  YTD {cur_year}: {ytd.get('ytd_current', 0):,} sessions | "
            f"YTD {pri_year}: {ytd.get('ytd_prior', 0):,} sessions | "
            f"Change: {fmt_pct(ytd.get('ytd_change_pct'))}"
        )
        ytd_lines.append("")
        ytd_lines.append("  Monthly breakdown:")
        for m in months:
            cur_s = m.get("sessions", 0)
            pri_s = m.get("prior_year_sessions", 0)
            chg   = round((cur_s - pri_s) / abs(pri_s) * 100, 1) if pri_s else None
            chg_str = fmt_pct(chg)
            ytd_lines.append(
                f"    {m['label']}: {cur_s:,} sessions "
                f"(vs {pri_s:,} in {pri_year} → {chg_str})"
            )

        # Quarterly summary (Q1 = Jan–Mar, Q2 = Apr–Jun, etc.)
        qtrs = {}
        for m in months:
            month_num = int(m["month"].split("-")[1])
            q = (month_num - 1) // 3 + 1
            qtrs.setdefault(q, {"cur": 0, "pri": 0})
            qtrs[q]["cur"] += m.get("sessions", 0)
            qtrs[q]["pri"] += m.get("prior_year_sessions", 0)

        if qtrs:
            ytd_lines.append("")
            ytd_lines.append("  Quarterly summary:")
            for q_num, q_data in sorted(qtrs.items()):
                chg = round((q_data["cur"] - q_data["pri"]) / abs(q_data["pri"]) * 100, 1) \
                      if q_data["pri"] else None
                ytd_lines.append(
                    f"    Q{q_num} {cur_year}: {q_data['cur']:,} sessions "
                    f"(vs {q_data['pri']:,} in {pri_year} → {fmt_pct(chg)})"
                )

    prompt = f"""You are an expert SEO and digital analytics analyst writing a monthly GA4 performance summary for {site['display_name']}.

Analyze the Google Analytics 4 data below and produce a concise monthly insights report covering traffic trends, engagement quality, conversion events, channel performance, and year-to-date trajectory. Focus on what changed, why it matters, and what actions to take.

## Event Glossary (apply this context when interpreting all event data)
- **navigational_clicks**: Internal clicks within the local pages site (page-to-page navigation) — low conversion value, indicates browsing behavior
- **outbound_clicks**: Any click that takes the user off-site to an external URL — moderate value, shows interest but not direct conversion
- **mainsite_clicks**: Clicks specifically to the client's main website — high value, indicates the user is moving deeper into the client's funnel
- **calls**: Phone call initiations — high-value conversion
- **book_now** / **schedule_** / **appointment_**: Booking or scheduling actions — high-value conversion
- **store_locate** / **find_location**: Location finder interactions — high intent, moderate-to-high value
- Any other custom events not in the above list should be interpreted based on their name and weighted accordingly.
{yoy_warning}
## Reporting Period
- **Current Month:** {cur_label}
- **MoM comparison:** vs. {mom_label}
{yoy_note}

## Traffic & Engagement KPIs
{perf_header}
{perf_divider}
{perf_rows}

## Custom Events (Conversion & Engagement KPIs)
Note: Standard GA4 auto-collected events have been filtered out. All events below are custom/meaningful.
{chr(10).join(event_lines) if event_lines else '  No custom events recorded this period'}

## Traffic Channel Breakdown
{chr(10).join(channel_lines) if channel_lines else '  No channel data available'}

## Top Landing Pages
{chr(10).join(page_lines) if page_lines else '  No landing page data available'}

## Year-to-Date Traffic ({cur_year} vs {pri_year})
{chr(10).join(ytd_lines) if ytd_lines else '  No YTD data available'}

---

Please provide your analysis in the following JSON structure ONLY — no markdown, no extra text, just valid JSON:

{{
  "summary": "A narrative summary of this site's GA4 performance in {cur_label}. Cover traffic trends, engagement quality, and the most notable event/conversion movements. Reference specific MoM changes{' and YoY changes' if yoy_available else ''}. Use MoM and YoY shorthands throughout. Also briefly note the YTD trajectory if it tells a meaningful story. Write as many sentences as the data warrants. Do not pad with filler.",
  "quick_wins": [
    {{
      "title": "Short action title",
      "detail": "Specific, actionable recommendation referencing the actual page, channel, or event by name",
      "impact": "low|medium|high",
      "effort": "low|medium|high"
    }}
  ],
  "potential_warnings": [
    {{
      "title": "Short warning title",
      "detail": "What is declining, by how much, and why it matters for the business",
      "severity": "low|medium|high"
    }}
  ],
  "biggest_opportunities": [
    {{
      "title": "Short opportunity title",
      "detail": "Specific opportunity — name the channel, page, or event and describe the expected impact",
      "impact": "low|medium|high",
      "effort": "low|medium|high"
    }}
  ]
}}

Rules:
- Summary: write naturally — as few or as many sentences as the data warrants. Mention {cur_label}. Always use "MoM" and "YoY" shorthands (never spell out "month-over-month" or "year-over-year"). Do not invent YoY comparisons if no YoY data was provided.
- Apply the Event Glossary above when assessing event value. Weight mainsite_clicks, calls, book_now, and scheduling events as high-value conversions. Weight navigational_clicks as low-value. Weight outbound_clicks as moderate.
- Quick wins: near-term, low-effort actions (e.g. improve landing pages with high traffic but low engagement, investigate traffic channel shifts)
- Potential warnings: MoM session drops, engagement rate declines, high-value conversion event decreases, channel shifts away from organic{', or YoY regressions' if yoy_available else ''}
- Biggest opportunities: higher-effort, high-reward moves (e.g. capitalize on growing channels, fix high-traffic low-engagement pages, push underperforming high-value conversion events)
- Be specific — name actual pages, channels, and events from the data above
- Always use "MoM" and "YoY" shorthands in all output fields
- 3–5 items per category max; omit a category entirely if there is genuinely nothing to flag
- Keep detail fields to 1–2 sentences
"""
    return prompt


# ── Claude API call ────────────────────────────────────────────────────────────

def get_recommendations(client: anthropic.Anthropic, site: dict, model: str) -> dict:
    """Call Claude with the site's GA4 data and parse the JSON response."""
    prompt = build_prompt(site)

    message = client.messages.create(
        model=model,
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = message.content[0].text.strip()

    # Strip accidental markdown code fences
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    if raw.endswith("```"):
        raw = raw[: raw.rfind("```")].strip()

    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"      ⚠️  JSON parse error: {e}")
        print(f"      Raw response (first 500 chars): {raw[:500]}")
        return {
            "summary": "Analysis could not be parsed. Check raw response.",
            "quick_wins": [],
            "potential_warnings": [],
            "biggest_opportunities": [],
            "_raw": raw[:1000],
        }


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="GA4 AI Recommendations Generator")
    parser.add_argument("--site",  help="Process only this display_name (partial match)")
    parser.add_argument("--model", default=DEFAULT_MODEL,
                        help=f"Claude model to use (default: {DEFAULT_MODEL})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print first recommendation without writing file")
    args = parser.parse_args()

    print("\n🤖 GA4 Recommendations — Starting\n")

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("❌ ANTHROPIC_API_KEY not set.")
        print("   Add it to a .env file: ANTHROPIC_API_KEY=sk-ant-...")
        return

    if not GA4_DATA_PATH.exists():
        print(f"❌ GA4 data not found at {GA4_DATA_PATH}")
        print("   Run ga4_analyzer.py first.")
        return

    with open(GA4_DATA_PATH) as f:
        ga4_data = json.load(f)

    sites = [s for s in ga4_data.get("sites", []) if "error" not in s]
    if not sites:
        print("❌ No valid site data found in ga4_data.json")
        return

    if args.site:
        sites = [s for s in sites
                 if args.site.lower() in s.get("display_name", "").lower()]
        if not sites:
            print(f"❌ No sites matched --site '{args.site}'")
            return

    print(f"📋 Processing {len(sites)} sites with model: {args.model}")
    print(f"   Estimated cost: ~${len(sites) * 0.003:.2f} (haiku) or ~${len(sites) * 0.020:.2f} (sonnet)\n")

    client  = anthropic.Anthropic(api_key=api_key)
    results = []
    errors  = []

    for i, site in enumerate(sites, 1):
        name = site.get("display_name", site.get("property_id", "Unknown"))
        print(f"[{i}/{len(sites)}] Analyzing: {name}")
        try:
            recs = get_recommendations(client, site, args.model)
            result = {
                "property_id":   site["property_id"],
                "display_name":  name,
                "analysis_date": datetime.utcnow().isoformat() + "Z",
                "model":         args.model,
                "window":        site["window"],
                "yoy_available": site.get("yoy_available", False),
                "totals":        site["totals"],
                "events":        site.get("events", []),
                "channels":      site.get("channels", []),
                "top_pages":     site.get("top_pages", []),
                "ytd":           site.get("ytd", {}),
                "recommendations": recs,
            }
            results.append(result)
            summary_preview = recs.get("summary", "")[:80]
            qw  = len(recs.get("quick_wins", []))
            pw  = len(recs.get("potential_warnings", []))
            bo  = len(recs.get("biggest_opportunities", []))
            print(f"  ✅ {qw} quick wins, {pw} warnings, {bo} opportunities")
            print(f"     \"{summary_preview}...\"\n")
        except Exception as e:
            print(f"  ❌ Failed: {e}\n")
            errors.append({"display_name": name, "error": str(e)})

        if i < len(sites):
            time.sleep(API_DELAY)

    if args.dry_run and results:
        print("🔍 Dry-run — first result:")
        print(json.dumps(results[0], indent=2)[:3000])
        return

    output = {
        "generated_at":       datetime.utcnow().isoformat() + "Z",
        "model":              args.model,
        "ga4_generated_at":   ga4_data.get("generated_at"),
        "sites":              results,
        "errors":             errors,
    }

    DATA_DIR.mkdir(exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"✅ Recommendations written to {OUTPUT_PATH}")
    print(f"   {len(results)} sites processed, {len(errors)} errors")

    if errors:
        print("\n⚠️  Errors:")
        for e in errors:
            print(f"   {e['display_name']}: {e['error']}")

    print("\n🏁 Done. Push data/ga4_recommendations.json to GitHub to update the dashboard.\n")


if __name__ == "__main__":
    main()
