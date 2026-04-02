"""
gsc_recommendations.py
======================
AI-Powered GSC Recommendations Generator — Per-Site Output

Reads per-site data from data/sites/{slug}/gsc.json (produced by gsc_analyzer.py),
sends each site's latest snapshot to Claude Haiku, and writes dual AI analyses to
data/sites/{slug}/gsc_analysis.json.

Each site gets two analyses per run:
  - rolling_30d         : based on the rolling 30-day snapshot
  - last_calendar_month : based on the last full calendar month snapshot

Usage:
    python gsc_recommendations.py
    python gsc_recommendations.py --site "ATI Physical Therapy"
    python gsc_recommendations.py --model claude-haiku-4-5-20251001

Requirements:
    pip install anthropic python-dotenv --break-system-packages
"""

import os
import json
import argparse
import time
from datetime import datetime, date
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
    path = SITES_DIR / slug / "gsc.json"
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)

def save_analysis(slug: str, data: dict):
    path = SITES_DIR / slug / "gsc_analysis.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# ── Prompt builder ─────────────────────────────────────────────────────────────

def build_prompt(site_name: str, window_data: dict, window_type: str) -> str:
    """
    Build a GSC analysis prompt for one window (rolling_30d or last_calendar_month).
    window_data is the rolling_30d or last_calendar_month dict from the snapshot.
    """
    t             = window_data["totals"]
    cur           = t["current"]
    yoy_available = window_data.get("yoy_available", False)
    signals       = window_data["signals"]
    buckets       = window_data["position_buckets"]

    cur_label = window_data["window"]["current"]["label"]
    mom_label = window_data["window"].get("mom", {}).get("label", "Prior Period")
    yoy_label = window_data["window"].get("yoy", {}).get("label", "Same Period LY")

    def fmt_pct(val):
        if val is None: return "N/A"
        return f"{'+' if val >= 0 else ''}{val}%"

    def fmt_pos_change(val):
        if val is None: return "N/A"
        direction = "↑ worse" if val > 0 else "↓ better" if val < 0 else "flat"
        return f"{val:+.1f} ({direction})"

    top_q_lines, p2_lines, dec_lines, low_ctr_lines, rising_lines = [], [], [], [], []

    for q in window_data.get("top_queries", [])[:15]:
        mom_chg = f"{q['mom_clicks_change_pct']:+.0f}% MoM" if q.get("mom_clicks_change_pct") is not None else "new MoM"
        yoy_chg = f"{q['yoy_clicks_change_pct']:+.0f}% YoY" if q.get("yoy_clicks_change_pct") is not None else "new YoY"
        top_q_lines.append(
            f"  - \"{q['query']}\": {q['clicks']} clicks, pos {q['position']}, "
            f"{q['ctr']}% CTR | {mom_chg} | {yoy_chg}"
        )

    for q in signals.get("page2_opportunities", [])[:8]:
        p2_lines.append(f"  - \"{q['query']}\": pos {q['position']}, {q['impressions']:,} impressions, {q['ctr']}% CTR")

    for q in signals.get("declining_queries", [])[:6]:
        dec_lines.append(f"  - \"{q['query']}\": {q.get('mom_clicks','?')} → {q['clicks']} clicks MoM, pos {q['position']}")

    for q in signals.get("low_ctr_queries", [])[:6]:
        low_ctr_lines.append(f"  - \"{q['query']}\": pos {q['position']}, {q['impressions']:,} impr, {q['ctr']}% CTR")

    for q in signals.get("rising_queries", [])[:6]:
        rising_lines.append(f"  - \"{q['query']}\": {q.get('mom_clicks','?')} → {q['clicks']} clicks MoM, pos {q['position']}")

    if yoy_available:
        perf_header  = f"| Metric | {cur_label} | MoM Change | YoY Change |"
        perf_divider = "|--------|------------|------------|------------|"
        perf_rows    = (
            f"| Clicks       | {cur.get('clicks',0):,} | {fmt_pct(t.get('mom_clicks_change_pct'))} | {fmt_pct(t.get('yoy_clicks_change_pct'))} |\n"
            f"| Impressions  | {cur.get('impressions',0):,} | {fmt_pct(t.get('mom_impressions_change_pct'))} | {fmt_pct(t.get('yoy_impressions_change_pct'))} |\n"
            f"| Avg Position | {cur.get('position','N/A')} | {fmt_pos_change(t.get('mom_position_change'))} | {fmt_pos_change(t.get('yoy_position_change'))} |\n"
            f"| CTR          | {cur.get('ctr','N/A')}% | {fmt_pct(t.get('mom_ctr_change'))} | {fmt_pct(t.get('yoy_ctr_change'))} |"
        )
        yoy_note    = f"- **YoY comparison:** vs. {yoy_label}"
        yoy_warning = ""
    else:
        perf_header  = f"| Metric | {cur_label} | MoM Change |"
        perf_divider = "|--------|------------|------------|"
        perf_rows    = (
            f"| Clicks       | {cur.get('clicks',0):,} | {fmt_pct(t.get('mom_clicks_change_pct'))} |\n"
            f"| Impressions  | {cur.get('impressions',0):,} | {fmt_pct(t.get('mom_impressions_change_pct'))} |\n"
            f"| Avg Position | {cur.get('position','N/A')} | {fmt_pos_change(t.get('mom_position_change'))} |\n"
            f"| CTR          | {cur.get('ctr','N/A')}% | {fmt_pct(t.get('mom_ctr_change'))} |"
        )
        yoy_note    = ""
        yoy_warning = "\n⚠️ Note: YoY data is not available for this property. Base analysis on MoM trends only. Do not reference YoY."

    prompt = f"""You are an expert SEO analyst writing a performance summary for {site_name}.
Analyze the Google Search Console data below and produce a concise insights report.
{yoy_warning}
## Reporting Period
- **Window:** {cur_label}
- **MoM comparison:** vs. {mom_label}
{yoy_note}

## Performance Summary
{perf_header}
{perf_divider}
{perf_rows}

## Keyword Position Distribution
- Top 3: {buckets['top_3']['count']} keywords, {buckets['top_3']['clicks']:,} clicks
- Positions 4–10: {buckets['pos_4_10']['count']} keywords, {buckets['pos_4_10']['clicks']:,} clicks
- Positions 11–20: {buckets['pos_11_20']['count']} keywords, {buckets['pos_11_20']['impressions']:,} impressions
- Positions 21–50: {buckets['pos_21_50']['count']} keywords, {buckets['pos_21_50']['impressions']:,} impressions

## Top Queries
{chr(10).join(top_q_lines) if top_q_lines else '  No query data'}

## Rising Queries (MoM Gainers)
{chr(10).join(rising_lines) if rising_lines else '  No significant gainers'}

## Page-2 Keywords (Positions 11–20)
{chr(10).join(p2_lines) if p2_lines else '  None identified'}

## Declining Queries (MoM Losers)
{chr(10).join(dec_lines) if dec_lines else '  No significant declines'}

## High-Impression, Low-CTR Queries
{chr(10).join(low_ctr_lines) if low_ctr_lines else '  None identified'}

---

Respond with this JSON structure ONLY — no markdown, no extra text:

{{
  "summary": "Narrative summary of organic search performance. Reference specific MoM changes{' and YoY changes' if yoy_available else ''}. Always use MoM and YoY shorthands — never spell out month-over-month or year-over-year.",
  "quick_wins": [
    {{"title": "...", "detail": "Specific action with keyword or page named", "impact": "low|medium|high", "effort": "low|medium|high"}}
  ],
  "potential_warnings": [
    {{"title": "...", "detail": "What is declining, how much, why it matters", "severity": "low|medium|high"}}
  ],
  "biggest_opportunities": [
    {{"title": "...", "detail": "Specific opportunity with keyword/page named", "impact": "low|medium|high", "effort": "low|medium|high"}}
  ]
}}

Rules:
- Always use MoM and YoY shorthands — never spell out month-over-month or year-over-year
- Be specific — name actual queries and pages from the data
- 3–6 items per category max; omit a category if nothing to flag
- 1–2 sentences per detail field
- Do not invent YoY comparisons if no YoY data was provided
"""
    return prompt


# ── Claude API call ────────────────────────────────────────────────────────────

def get_analysis(client: anthropic.Anthropic, site_name: str,
                 window_data: dict, window_type: str, model: str) -> dict:
    prompt = build_prompt(site_name, window_data, window_type)
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
    parser = argparse.ArgumentParser(description="GSC AI Recommendations — Per-Site Output")
    parser.add_argument("--site",    help="Process only this site (partial name/URL match)")
    parser.add_argument("--model",   default=DEFAULT_MODEL)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print first analysis without writing files")
    args = parser.parse_args()

    print("\n🤖 GSC Recommendations — Starting\n")

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("❌ ANTHROPIC_API_KEY not set. Add to .env file.")
        return

    if not PROPERTIES_PATH.exists():
        print(f"❌ Properties file not found: {PROPERTIES_PATH}")
        return

    with open(PROPERTIES_PATH) as f:
        all_properties = json.load(f)

    # Filter to requested site
    if args.site:
        all_properties = [p for p in all_properties
                          if args.site.lower() in p.get("display_name", "").lower()
                          or args.site.lower() in p["site_url"].lower()]
        if not all_properties:
            print(f"❌ No sites matched --site '{args.site}'")
            return

    # Load per-site data files
    sites_to_process = []
    for prop in all_properties:
        slug = site_slug(prop["site_url"])
        data = load_site_data(slug)
        if data is None:
            print(f"  ⏭️  Skipping {prop.get('display_name', prop['site_url'])} — no gsc.json found")
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
        print("❌ No sites with snapshot data found. Run gsc_analyzer.py first.")
        return

    print(f"📋 Processing {len(sites_to_process)} sites with model: {args.model}")
    print(f"   Each site = 2 analyses (rolling_30d + last_calendar_month)")
    print(f"   Estimated cost: ~${len(sites_to_process) * 0.004:.2f} (haiku)\n")

    ai_client = anthropic.Anthropic(api_key=api_key)
    processed = 0
    errors    = []

    for i, site in enumerate(sites_to_process, 1):
        name     = site["display_name"]
        slug     = site["slug"]
        snapshot = site["snapshot"]

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
                analysis = get_analysis(ai_client, name, window_data, window_key, args.model)
                result[window_key] = {
                    "window":          window_data["window"],
                    "yoy_available":   window_data.get("yoy_available", False),
                    "totals":          window_data["totals"],
                    "top_queries":     window_data.get("top_queries", [])[:20],
                    "top_pages":       window_data.get("top_pages", [])[:10],
                    "position_buckets": window_data.get("position_buckets", {}),
                    "summary":         analysis.get("summary", ""),
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
