# Sitebulb SEO Audit Automation — Claude Operating Manual

> **This file must be read in full before executing any part of this project.**
> It governs how Claude navigates, parses, scores, and communicates findings from
> every monthly Sitebulb crawl across all managed sites.

---

## 1. Project Overview

Each month, Sitebulb crawls a set of managed websites and exports structured Google Sheets
data into this folder. Claude's role is to act as the analytical and communication layer:
reading the crawl exports, reviewing audit hints, prioritizing issues, and producing clear
action items for Adam Newbern's review before anything is sent to Trello.

The pipeline is:
  Sitebulb crawl → Google Sheets export in this folder →
  Claude parses + scores → Adam reviews → Trello cards created

---

## 2. Folder Structure

Each project (client site) has its own subfolder here. **Only folders matching one
of the following prefixes should ever be parsed.** All other folders must be ignored:

  - `GPO Local Pages |`
  - `GPO Pages |`
  - `GPO Dealer Pages |`

Typical naming conventions within those prefixes:
  `GPO Local Pages | [Client Name] | [Domain]`
  `GPO Pages | [Client Name] | [Domain]`
  `GPO Dealer Pages | [Client Name] | [Domain]`

Inside each project folder:
```
[Project Folder]/
├── Historical Audit Data - [domain].gsheet     ← month-over-month crawl metrics
├── Historical Hint Data - [domain].gsheet      ← hint trends over time
└── Audit [YYYY-MM-DD HH:MM:SS]/                ← one folder per crawl run
    ├── Audit Summary.gsheet                    ← top-level crawl health scores
    ├── Internal URLs.gsheet
    ├── External URLs.gsheet
    ├── Indexability.gsheet
    ├── On Page.gsheet
    ├── Redirect Chains.gsheet
    ├── Redirected URLs.gsheet
    ├── Resource URLs.gsheet
    ├── Security Issues.gsheet
    ├── HTML Templates/
    └── Hints/
        ├── All Hints.gsheet                    ← PRIMARY TARGET: all hints in one sheet
        └── [Individual Hint Name].gsheet       ← one sheet per hint type
```

---

## 3. Audit Folder Navigation Rules

**RULE: Only process folders that begin with an approved prefix.**

Before doing anything else, filter the project list. Only process folders whose name
starts with one of these exact prefixes (case-sensitive):
  - `GPO Local Pages |`
  - `GPO Pages |`
  - `GPO Dealer Pages |`

Any folder that does not begin with one of these prefixes must be skipped entirely,
regardless of its contents. Do not log, report, or act on skipped folders.

**RULE: Always use the most recently dated Audit folder.**

Audit folders are named with an ISO-style timestamp: `Audit YYYY-MM-DD HH:MM:SS`.
When multiple audit folders exist for a project, compare timestamps and always select
the one with the latest date. Never process an older audit if a newer one exists.

Steps:
1. List all subfolders inside the project folder.
2. Filter for folders whose name begins with `Audit `.
3. Parse the date portion of each folder name.
4. Select the folder with the most recent date.
5. Open `Hints/All Hints.gsheet` within that folder as the primary data source.
6. Also open `Audit Summary.gsheet` within that same folder for overall health metrics.

---

## 4. Data to Read Per Project

For each project, read the following in this order:

### 4a. Audit Summary
Open `[Most Recent Audit Folder]/Audit Summary.gsheet`.
Record the overall health/score metrics. These provide context for how severe the
hints list is in aggregate.

### 4b. All Hints
Open `[Most Recent Audit Folder]/Hints/All Hints.gsheet`.
This sheet contains one row per hint type triggered during the crawl.

**Columns in All Hints:**
| Column | Name | Description |
|--------|------|-------------|
| A | Section | Category: Accessibility, Internal URLs, Links, On Page, Organic Search, Rendered, Security |
| B | Hint | The specific hint/issue name |
| C | Coverage | % of total crawled URLs affected |
| D | URLs | Count of affected URLs in this crawl |
| E | Previous URLs | Count from the prior crawl (delta tracking built-in) |
| F | Importance | `0-None`, `1-Low`, `2-Medium`, `3-High` |
| G | Warning Type | `1-Insight`, `2-Opportunity`, `3-Potential Issue`, `4-Issue` |
| H | Learn More | Link to Sitebulb or external documentation for this hint |
| I | Sheet URL | Direct link to the individual hint detail sheet |

---

## 5. Scoring & Prioritization

Hints must always be sorted and presented with the highest-priority issues first.

**Primary sort: Importance (Column F) — descending**
  3-High → 2-Medium → 1-Low → 0-None

**Secondary sort: Warning Type (Column G) — descending**
  4-Issue → 3-Potential Issue → 2-Opportunity → 1-Insight

**Composite priority score formula:**
  `priority = (importance_value * 10) + warning_type_value`
  Example: Importance 3-High + Warning Type 4-Issue = score of 34 (highest urgency)
  Example: Importance 1-Low + Warning Type 1-Insight = score of 11 (lowest urgency)

**Trend flags (compare URLs vs. Previous URLs):**
- `NEW` — issue appeared this crawl with 0 in Previous URLs
- `WORSENING` — URLs count increased vs. Previous URLs
- `IMPROVING` — URLs count decreased vs. Previous URLs
- `STABLE` — URLs count unchanged
- `RESOLVED` — URLs count dropped to 0 (celebrate this in the report)

Issues marked NEW or WORSENING should be called out prominently in the action items summary.

---

## 6. Historical Comparison & Data Storage

After processing each project's audit, store a JSON snapshot of the parsed hints in a
`.audit_cache` subfolder **inside each project's Google Drive folder**:
  `[Drive Project Folder]/.audit_cache/[YYYY-MM-DD].json`

The script automatically creates the `.audit_cache` folder in Drive if it does not exist,
and updates the file if a cache for that date was already written.

This cache enables:
- Accurate trend detection even when Previous URLs column is empty or stale
- Multi-month trend analysis (e.g., "this issue has been present for 3 months")
- Identification of issues that resolved and then re-appeared

Cache file format (per project, per audit date):
```json
{
  "project": "Project folder name",
  "domain": "domain.com",
  "audit_date": "YYYY-MM-DD",
  "hints": [
    {
      "section": "Security",
      "hint": "X-Frame-Options HTTP header is missing or invalid",
      "coverage": 100.0,
      "urls": 356,
      "importance": "0-None",
      "warning_type": "1-Insight",
      "priority_score": 1,
      "trend": "STABLE"
    }
  ],
  "summary_metrics": {}
}
```

---

## 7. Language & Tone Guidelines

All output — summaries, action items, Trello card descriptions — must follow these rules:

1. **Be direct and concise.** No filler phrases, no embellishment. Say exactly what the
   issue is, how many URLs are affected, and what needs to happen.

2. **State problems plainly.** Do not soften or hedge issue descriptions. If 356 URLs are
   missing HSTS headers, say "356 URLs are missing HSTS headers" — not "some pages may
   benefit from improved security headers."

3. **Always include URL counts and links.**
   - If 12 or fewer URLs are affected: list every URL.
   - If more than 12 URLs are affected: include a representative sample of 5–10 URLs
     and link to the full hint detail sheet (Sheet URL column).

4. **Always link to the Learn More URL** for each hint so developers have immediate
   context on what the issue means and how to fix it.

5. **Lead with the most important issues.** Never bury a High/Issue finding below
   lower-priority items.

6. **Call out trends explicitly.** If an issue is NEW or WORSENING, flag it clearly
   at the start of that item's description.

7. **Celebrate resolved issues.** If an issue from the previous crawl is now at 0 URLs,
   include it in a separate "Resolved Since Last Crawl" section.

---

## 8. Notification Protocol

Claude must notify Adam Newbern (adam.newbern@gpo.com) at the following stages:

| Stage | Notification |
|-------|-------------|
| Pipeline starts | "Monthly SEO audit pipeline has started. Processing [N] projects." |
| Each project completes | "[Project Name] audit complete. [N] action items identified." |
| A project has NEW/WORSENING High issues | Immediate flag: "⚠️ [Project]: [Hint Name] has worsened since last crawl." |
| All projects complete | "All [N] projects processed. Summary of action items ready for your review." |
| Awaiting Adam's approval | "Please review the action items below and confirm before I send to Trello." |

---

## 9. Definition of a Completed Project

A project is considered **complete** only when ALL of the following have occurred:

- [ ] The most recently dated Audit folder has been identified and confirmed
- [ ] `Audit Summary.gsheet` has been opened and its metrics recorded
- [ ] `Hints/All Hints.gsheet` has been opened and all rows parsed
- [ ] Each hint has been scored using the priority formula in Section 5
- [ ] Trend flags have been applied by comparing to previous audit data (Section 6)
- [ ] A `.audit_cache/[date].json` snapshot has been saved for this audit
- [ ] A structured, prioritized action items summary has been generated
- [ ] Adam has been notified and presented with the action items for review
- [ ] Adam has confirmed approval before any Trello cards are created

**Do not create Trello cards until Adam explicitly approves the action items.**

---

## 10. Script Entry Points

The following scripts in this folder drive the automation:

| Script | Purpose |
|--------|---------|
| `sitebulb_analyzer.py` | Main analysis pipeline — reads all projects from Google Drive, scores hints, generates report |
| `trello_publisher.py` | Creates Trello cards from approved action items (run only after Adam's approval) |
| `requirements.txt` | Python dependencies — install with `pip install -r requirements.txt` |

**To run the monthly pipeline:**
```bash
cd "[path to this folder]"
pip install -r requirements.txt --break-system-packages
python sitebulb_analyzer.py
```

**To process a single project:**
```bash
python sitebulb_analyzer.py --project "ATI Physical Therapy"
```

**To do a dry run (no cache writes, output to console):**
```bash
python sitebulb_analyzer.py --dry-run
```

**Data source:** The script reads all project folders directly from Google Drive
(Sitebulb Server Exports folder ID: `1-3LZeNARxdUP7mAyvSoZ1DjXVjrjbdMZ`).
No local copy of the export data is required. Audit cache JSON files are also
written back to Drive inside each project's `.audit_cache` subfolder.
The consolidated action items report is saved locally alongside the script.

---

## 11. Authentication

The scripts require Google API credentials to read Google Drive folders, read Google Sheets,
and write audit cache files back to Drive.

- Credentials file: `credentials/google_credentials.json` (service account or OAuth)
- Token cache: `credentials/token.json` (auto-generated on first OAuth run)
- Trello API key + token: stored in `credentials/trello_config.json`

**Required Google API scopes (both are needed):**
```
https://www.googleapis.com/auth/drive
https://www.googleapis.com/auth/spreadsheets.readonly
```

The `drive` scope is required (not `drive.readonly`) because the script writes audit
cache JSON files back into each project's Drive folder.

**Format of `credentials/trello_config.json`:**
```json
{
  "api_key": "YOUR_TRELLO_API_KEY",
  "token": "YOUR_TRELLO_TOKEN",
  "board_id": "YOUR_BOARD_ID",
  "default_list_id": "YOUR_DEFAULT_LIST_ID",
  "assignee_member_id": "ADAMS_TRELLO_MEMBER_ID"
}
```

Never commit credentials files to version control.

---

---

## 12. GSC Pipeline (Google Search Console)

Scripts: `gsc_analyzer.py` → `gsc_recommendations.py`
Output: `data/gsc_data.json` → `data/gsc_recommendations.json`

Site list: `credentials/gsc_properties.json` — each entry has `site_url`, `display_name`, and `ga4_property_id`.

**Date windows (calendar month):**
- Current: last fully completed calendar month
- MoM: the month before current
- YoY: same calendar month one year prior
- `yoy_available: false` is set automatically for new properties with no YoY data

**AI analysis conventions:**
- Always use "MoM" and "YoY" shorthands — never spell out "month-over-month" or "year-over-year"
- Output JSON structure: `summary`, `quick_wins`, `potential_warnings`, `biggest_opportunities`

**To run:**
```bash
python gsc_analyzer.py
python gsc_recommendations.py
```

**Single site:**
```bash
python gsc_analyzer.py --site "ATI Physical Therapy"
python gsc_recommendations.py --site "ATI Physical Therapy"
```

---

## 13. GA4 Pipeline (Google Analytics 4)

Scripts: `ga4_analyzer.py` → `ga4_recommendations.py`
Output: `data/ga4_data.json` → `data/ga4_recommendations.json`

Reads site list from the same `credentials/gsc_properties.json`. Sites without a `ga4_property_id` are skipped. CORT Global is intentionally skipped (no service account access).

**To discover/update GA4 property IDs:**
```bash
python discover_ga4_properties.py
```

**Required Google Cloud APIs (both must be enabled):**
- Google Analytics Admin API
- Google Analytics Data API

**Date windows:** Same calendar-month logic as GSC (current, MoM, YoY).

**Data pulled per site:**
- Traffic & engagement KPIs: Sessions, Engaged Sessions, Engagement Rate
- Custom events (auto-collected/generic events filtered out — see glossary below)
- Traffic channel breakdown by `sessionDefaultChannelGroup`
- Top 10 landing pages with sessions, engaged sessions, engagement rate
- YTD monthly session totals vs prior year (with quarterly subtotals)

**To run:**
```bash
python ga4_analyzer.py
python ga4_recommendations.py
```

---

## 14. GA4 Event Glossary

The following event definitions are used by the AI analysis to correctly interpret and weight custom events. This context is injected into every `ga4_recommendations.py` prompt.

| Event Name | Type | Value | Description |
|------------|------|-------|-------------|
| `calls` | Conversion | High | Phone call initiation |
| `book_now` / `schedule_*` / `appointment_*` | Conversion | High | Booking or scheduling action |
| `mainsite_clicks` | Conversion | High | Click to the client's main website — user moving deeper into the funnel |
| `store_locate` / `find_location` | Intent | Medium–High | Location finder interaction — high local intent |
| `outbound_clicks` | Engagement | Medium | Any click to an external URL off the local pages site |
| `navigational_clicks` | Engagement | Low | Internal clicks within the local pages site (page-to-page navigation) |

**Filtered out (never shown in dashboard):** `first_visit`, `session_start`, `page_view`, `scroll`, `user_engagement`, `click`, `all_clicks`, `gtm.*`, `js_error`, `form_start`, `video_*`, `file_download`

Any event not in the above lists is treated as a custom conversion or engagement event and displayed as-is. The AI analysis infers intent from the event name.

**AI analysis conventions (both GSC and GA4):**
- Always use "MoM" and "YoY" shorthands — never spell out in full
- Output JSON structure: `summary`, `quick_wins`, `potential_warnings`, `biggest_opportunities`
- Omit a category entirely if there is genuinely nothing to flag
- 3–5 items per category maximum

---

*Last updated: 2026-04-01 | Maintained by Adam Newbern, GPO*
*Updated 2026-04-01: Added GSC pipeline (Sections 12), GA4 pipeline (Section 13), and GA4 event glossary (Section 14).*
