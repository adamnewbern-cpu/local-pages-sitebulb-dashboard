"""
gsc_recommendations.py
======================
AI-Powered GSC Recommendations Generator

Reads data/gsc_data.json (produced by gsc_analyzer.py), sends each site's
data to the Claude API, and outputs structured recommendations to
data/gsc_recommendations.json for display in the dashboard.

Usage:
    python gsc_recommendations.py
    python gsc_recommendations.py --site "example.com"   # single site
    python gsc_recommendations.py --model claude-haiku-4-5-20251001  # faster/cheaper

Requirements:
    pip install anthropic python-dotenv --break-system-packages

Setup:
    Store your Anthropic API key in a .env file in this folder:
        ANTHROPIC_API_KEY=sk-ant-...

    Or set it as an environment variable before running:
        export ANTHROPIC_API_KEY=sk-ant-...
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
    pass  # dotenv optional; env var can be set directly

# ── Configuration ──────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent.resolve()
DATA_DIR = SCRIPT_DIR / "data"
GSC_DATA_PATH = DATA_DIR / "gsc_data.json"
OUTPUT_PATH = DATA_DIR / "gsc_recommendations.json"

# Default model — haiku is fast and cheap; upgrade to sonnet for deeper analysis
DEFAULT_MODEL = "claude-haiku-4-5-20251001"

# Delay between API calls (be respectful of rate limits)
API_DELAY = 1.0


# ── Prompt builder ─────────────────────────────────────────────────────────────

def build_prompt(site: dict) -> str:
    """
    Build a structured analysis prompt from the site's GSC data.
    Keeps it concise to stay within context limits and control cost.
    """
    t = site["totals"]
    cur = t["current"]
    prev = t["previous"]
    signals = site["signals"]
    buckets = site["position_buckets"]

    def fmt_pct(val):
        if val is None:
            return "N/A"
        return f"{'+' if val >= 0 else ''}{val}%"

    def fmt_pos_change(val):
        if val is None:
            return "N/A"
        direction = "▲ worse" if val > 0 else "▼ better" if val < 0 else "no change"
        return f"{val:+.1f} ({direction})"

    # Top queries summary
    top_q_lines = []
    for q in site["top_queries"][:15]:
        pos_chg = f"{q['position_change']:+.1f}" if q["position_change"] is not None else "new"
        top_q_lines.append(
            f"  - \"{q['query']}\": {q['clicks']} clicks, pos {q['position']} "
            f"({pos_chg}), {q['ctr']}% CTR, trend: {q['trend']}"
        )

    # Page-2 opportunities
    p2_lines = []
    for q in signals["page2_opportunities"][:8]:
        p2_lines.append(
            f"  - \"{q['query']}\": pos {q['position']}, {q['impressions']:,} impressions, {q['ctr']}% CTR"
        )

    # Declining queries
    dec_lines = []
    for q in signals["declining_queries"][:6]:
        prev_c = q.get("prev_clicks", "?")
        dec_lines.append(
            f"  - \"{q['query']}\": {prev_c} → {q['clicks']} clicks, pos {q['position']}"
        )

    # Low CTR signals
    low_ctr_lines = []
    for q in signals["low_ctr_queries"][:6]:
        low_ctr_lines.append(
            f"  - \"{q['query']}\": pos {q['position']}, {q['impressions']:,} impr, {q['ctr']}% CTR"
        )

    prompt = f"""You are an expert SEO analyst. Analyze the following Google Search Console data for {site['display_name']} and provide concise, actionable recommendations.

## Site Overview
- **Site:** {site['display_name']}
- **Analysis Period:** {site['window']['current']['start']} to {site['window']['current']['end']} vs. prior 30 days

## Performance Summary
| Metric | Current 30d | Prev 30d | Change |
|--------|------------|----------|--------|
| Clicks | {cur['clicks']:,} | {prev['clicks']:,} | {fmt_pct(t['clicks_change_pct'])} |
| Impressions | {cur['impressions']:,} | {prev['impressions']:,} | {fmt_pct(t['impressions_change_pct'])} |
| Avg Position | {cur['position']} | {prev['position']} | {fmt_pos_change(t['position_change'])} |
| CTR | {cur['ctr']}% | {prev['ctr']}% | {'+' if t.get('ctr_change', 0) and t['ctr_change'] >= 0 else ''}{t.get('ctr_change', 'N/A')}% |

## Keyword Position Distribution
- Top 3 positions: {buckets['top_3']['count']} keywords, {buckets['top_3']['clicks']:,} clicks
- Positions 4–10: {buckets['pos_4_10']['count']} keywords, {buckets['pos_4_10']['clicks']:,} clicks
- Positions 11–20: {buckets['pos_11_20']['count']} keywords, {buckets['pos_11_20']['impressions']:,} impressions
- Positions 21–50: {buckets['pos_21_50']['count']} keywords, {buckets['pos_21_50']['impressions']:,} impressions

## Top Performing Queries (Current Period)
{chr(10).join(top_q_lines) if top_q_lines else '  No query data available'}

## Page-2 Keywords (Positions 11–20, High Impressions — Biggest Opportunities)
{chr(10).join(p2_lines) if p2_lines else '  None identified'}

## Declining Queries (Potential Warnings)
{chr(10).join(dec_lines) if dec_lines else '  No significant declines'}

## High-Impression, Low-CTR Queries (Quick Wins — better title/meta could unlock clicks)
{chr(10).join(low_ctr_lines) if low_ctr_lines else '  None identified'}

---

Please provide your analysis in the following JSON structure ONLY — no markdown, no extra text, just valid JSON:

{{
  "summary": "2-3 sentence overall performance summary for this site",
  "quick_wins": [
    {{
      "title": "Short action title",
      "detail": "Specific, actionable recommendation with the keyword or page mentioned by name",
      "impact": "low|medium|high",
      "effort": "low|medium|high"
    }}
  ],
  "potential_warnings": [
    {{
      "title": "Short warning title",
      "detail": "What is declining and why it matters",
      "severity": "low|medium|high"
    }}
  ],
  "biggest_opportunities": [
    {{
      "title": "Short opportunity title",
      "detail": "Specific opportunity with keywords/pages named, expected impact",
      "impact": "low|medium|high",
      "effort": "low|medium|high"
    }}
  ]
}}

Rules:
- Quick wins: low-effort, near-term actions (title tag fixes, meta descriptions for low-CTR queries, internal linking)
- Potential warnings: declining trends, drops in traffic or rankings that need monitoring or action
- Biggest opportunities: higher-effort but high-reward moves (content creation, page-2 pushes, new keyword targets)
- Be specific — name actual queries and pages from the data above
- 3–6 items per category max
- Keep detail fields to 1–2 sentences
"""
    return prompt


# ── Claude API call ────────────────────────────────────────────────────────────

def get_recommendations(client: anthropic.Anthropic, site: dict, model: str) -> dict:
    """Call Claude with the site's GSC data and parse the JSON response."""
    prompt = build_prompt(site)

    message = client.messages.create(
        model=model,
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = message.content[0].text.strip()

    # Strip any accidental markdown code fences
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
    parser = argparse.ArgumentParser(description="GSC AI Recommendations Generator")
    parser.add_argument("--site", help="Process only this site (partial match)")
    parser.add_argument("--model", default=DEFAULT_MODEL,
                        help=f"Claude model to use (default: {DEFAULT_MODEL})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print first recommendation without writing file")
    args = parser.parse_args()

    print("\n🤖 GSC Recommendations — Starting\n")

    # Check API key
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("❌ ANTHROPIC_API_KEY not set.")
        print("   Add it to a .env file in this folder:")
        print("   ANTHROPIC_API_KEY=sk-ant-...")
        return

    # Load GSC data
    if not GSC_DATA_PATH.exists():
        print(f"❌ GSC data not found at {GSC_DATA_PATH}")
        print(f"   Run gsc_analyzer.py first.")
        return

    with open(GSC_DATA_PATH) as f:
        gsc_data = json.load(f)

    sites = gsc_data.get("sites", [])
    if not sites:
        print("❌ No site data found in gsc_data.json")
        return

    # Filter to single site if requested
    if args.site:
        sites = [s for s in sites
                 if args.site.lower() in s["site_url"].lower()
                 or args.site.lower() in s.get("display_name", "").lower()]
        if not sites:
            print(f"❌ No sites matched --site '{args.site}'")
            return

    print(f"📋 Processing {len(sites)} sites with model: {args.model}")
    print(f"   Estimated cost: ~${len(sites) * 0.002:.2f} (haiku) or ~${len(sites) * 0.015:.2f} (sonnet)\n")

    # Build Anthropic client
    client = anthropic.Anthropic(api_key=api_key)

    # Process each site
    results = []
    errors = []
    for i, site in enumerate(sites, 1):
        display_name = site.get("display_name", site["site_url"])
        print(f"[{i}/{len(sites)}] Analyzing: {display_name}")
        try:
            recs = get_recommendations(client, site, args.model)
            result = {
                "site_url": site["site_url"],
                "display_name": display_name,
                "analysis_date": datetime.utcnow().isoformat() + "Z",
                "model": args.model,
                "totals": site["totals"],
                "window": site["window"],
                "position_buckets": site["position_buckets"],
                "top_queries": site["top_queries"][:20],
                "top_pages": site["top_pages"][:10],
                "daily": site.get("daily", []),
                "recommendations": recs,
            }
            results.append(result)
            summary_preview = recs.get("summary", "")[:80]
            qw_count = len(recs.get("quick_wins", []))
            pw_count = len(recs.get("potential_warnings", []))
            bo_count = len(recs.get("biggest_opportunities", []))
            print(f"  ✅ {qw_count} quick wins, {pw_count} warnings, {bo_count} opportunities")
            print(f"     \"{summary_preview}...\"\n")
        except Exception as e:
            print(f"  ❌ Failed: {e}\n")
            errors.append({"site_url": site["site_url"], "display_name": display_name, "error": str(e)})

        if i < len(sites):
            time.sleep(API_DELAY)

    # Dry-run: just print first result
    if args.dry_run and results:
        print("🔍 Dry-run — first result:")
        print(json.dumps(results[0], indent=2)[:3000])
        return

    # Write output
    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "model": args.model,
        "gsc_generated_at": gsc_data.get("generated_at"),
        "sites": results,
        "errors": errors,
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

    print("\n🏁 Done. Push data/gsc_recommendations.json to GitHub to update the dashboard.\n")


if __name__ == "__main__":
    main()
