"""
discover_ga4_properties.py
==========================
Discovers all GA4 properties accessible to the service account and
outputs credentials/ga4_properties.json for use by ga4_analyzer.py.

Usage:
    python discover_ga4_properties.py           # list + write ga4_properties.json
    python discover_ga4_properties.py --dry-run # list only, no file written

Requirements:
    pip install google-analytics-admin google-auth --break-system-packages

Setup:
    1. Enable "Google Analytics Admin API" in Google Cloud Console
    2. Add the service account email as a Viewer on each GA4 property
       (GA4 Admin → Property → Property Access Management → Add users)
    3. Run this script to auto-discover all property IDs

Output: credentials/ga4_properties.json
    [
      { "property_id": "123456789", "display_name": "ATI Physical Therapy" },
      ...
    ]
"""

import json
import argparse
import warnings
from pathlib import Path

# Suppress Python version FutureWarnings from Google SDK
warnings.filterwarnings("ignore", category=FutureWarning)

from google.oauth2 import service_account
from google.analytics.admin import AnalyticsAdminServiceClient
from google.analytics.admin_v1alpha.types import ListPropertiesRequest

# ── Configuration ──────────────────────────────────────────────────────────────

SCRIPT_DIR        = Path(__file__).parent.resolve()
CREDENTIALS_DIR   = SCRIPT_DIR / "credentials"
OUTPUT_PATH       = CREDENTIALS_DIR / "ga4_properties.json"
SERVICE_ACCOUNT_PATH = CREDENTIALS_DIR / "google_service_account.json"

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]


# ── Auth ───────────────────────────────────────────────────────────────────────

def get_admin_client():
    if not SERVICE_ACCOUNT_PATH.exists():
        raise FileNotFoundError(
            f"Service account not found at {SERVICE_ACCOUNT_PATH}\n"
            f"Copy your google_service_account.json file there."
        )
    creds = service_account.Credentials.from_service_account_file(
        str(SERVICE_ACCOUNT_PATH), scopes=SCOPES
    )
    return AnalyticsAdminServiceClient(credentials=creds)


# ── Discovery ──────────────────────────────────────────────────────────────────

def discover_properties(client):
    """
    Returns a list of dicts: { property_id, display_name, account, create_time }
    Iterates all accessible accounts, then all GA4 properties under each.
    """
    properties = []

    # List all accounts the service account can see
    accounts = list(client.list_accounts())

    if not accounts:
        print("⚠️  No GA4 accounts found. Make sure the service account has been")
        print("   granted Viewer access in GA4 Admin → Property Access Management.")
        return properties

    for account in accounts:
        account_name = account.name  # e.g. "accounts/12345"
        account_display = account.display_name

        # List all GA4 properties under this account
        request = ListPropertiesRequest(filter=f"parent:{account_name}", page_size=200)
        props = list(client.list_properties(request=request))

        for prop in props:
            # prop.name is like "properties/123456789"
            property_id = prop.name.split("/")[-1]
            properties.append({
                "property_id":  property_id,
                "display_name": prop.display_name,
                "account":      account_display,
                "create_time":  prop.create_time.strftime("%Y-%m-%d") if prop.create_time else None,
            })

    return properties


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Discover GA4 properties for service account")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print results but do not write ga4_properties.json")
    args = parser.parse_args()

    print("🔍 Connecting to Google Analytics Admin API...")
    client = get_admin_client()

    print("📋 Discovering GA4 properties...\n")
    properties = discover_properties(client)

    if not properties:
        print("No properties found. Check service account access and try again.")
        return

    # ── Print results table ────────────────────────────────────────────────────
    col_w = max(len(p["display_name"]) for p in properties) + 2
    print(f"{'Property ID':<14} {'Display Name':<{col_w}} {'Account':<30} {'Created'}")
    print("-" * (14 + col_w + 30 + 12))
    for p in properties:
        print(f"{p['property_id']:<14} {p['display_name']:<{col_w}} {p['account']:<30} {p['create_time'] or '—'}")

    print(f"\n✅ Found {len(properties)} propert{'y' if len(properties) == 1 else 'ies'}")

    # ── Write output file ──────────────────────────────────────────────────────
    if args.dry_run:
        print("\n[dry-run] Skipping file write.")
        return

    # Strip internal-only fields before saving
    output = [
        {"property_id": p["property_id"], "display_name": p["display_name"]}
        for p in properties
    ]

    CREDENTIALS_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n💾 Written to {OUTPUT_PATH}")
    print("\nNext step: run ga4_analyzer.py to pull monthly data for all properties.")


if __name__ == "__main__":
    main()
