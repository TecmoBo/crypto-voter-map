#!/usr/bin/env python3
"""
sync_from_api.py
Rebuilds swc_embedded_data.json directly from the SWC public API.

No scraping needed. The API endpoint returns all candidates with scores,
party, state, district, slug, and photo in a single JSON payload.

Candidates with null scores are included and stored as -1 (shown on the
map as "Not Yet Rated" in gray).

Usage:
    python3 sync_from_api.py [--dry-run]
"""

import json
import sys
import urllib.request
from collections import Counter
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────────────────
API_URL  = "https://www.standwithcrypto.org/api/us/public/partners/races/all-people"
OUT_FILE = Path(__file__).parent / "swc_embedded_data.json"
HEADERS  = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": "https://www.standwithcrypto.org/",
}

PARTY_MAP = {
    "US_REPUBLICAN": "R",
    "US_DEMOCRAT":   "D",
    "US_INDEPENDENT": "I",
}

DRY_RUN = "--dry-run" in sys.argv


def fetch_api() -> list[dict]:
    print(f"Fetching {API_URL} …")
    req = urllib.request.Request(API_URL, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
    people = data["people"]
    print(f"  {len(people)} candidates received")
    return people


def build_record(p: dict):
    """Convert one API person object to compact array, or None to skip."""
    role = p.get("primaryRole") or {}
    category = role.get("roleCategory", "")

    # Only US Congress / Senate / Governor (only Congress+Senate for now)
    if category not in ("CONGRESS", "SENATE"):
        return None

    country = role.get("primaryCountryCode", "US")
    if country != "US":
        return None

    # Name
    first    = p.get("firstNickname") or p.get("firstName") or ""
    last     = p.get("lastName") or ""
    suffix   = p.get("nameSuffix") or ""
    name     = f"{first} {last}".strip()
    if suffix:
        name += f" {suffix}"

    # Party
    party = PARTY_MAP.get(p.get("politicalAffiliationCategoryV2") or "", "?")

    # Score: manual override takes priority; null → -1
    score = p.get("manuallyOverriddenStanceScore")
    if score is None:
        score = p.get("computedStanceScore")
    if score is None:
        score = -1

    # State / chamber / district
    state    = role.get("primaryState") or ""
    chamber  = "S" if category == "SENATE" else "H"
    district = ""
    if chamber == "H":
        d = role.get("primaryDistrict") or ""
        district = d if d else "AL"

    # Incumbent flag
    incumbent = 1 if role.get("status") == "HELD" else 0

    # Senate-run flag: 1 if this House HELD member also has a RUNNING_FOR SENATE role
    senate_run = 0
    if chamber == "H" and incumbent:
        for r in p.get("roles") or []:
            if (r.get("roleCategory") == "SENATE"
                    and r.get("status") == "RUNNING_FOR"):
                senate_run = r.get("primaryState") or state
                break

    slug  = p.get("slug") or ""
    photo = p.get("profilePictureUrl") or None

    return [name, party, score, state, chamber, district, incumbent, senate_run, slug, photo, 1]


def main():
    people  = fetch_api()
    records = []
    skipped = 0

    for p in people:
        rec = build_record(p)
        if rec is None:
            skipped += 1
        else:
            records.append(rec)

    # Stats
    total     = len(records)
    rated     = sum(1 for r in records if r[2] != -1)
    unrated   = total - rated
    held      = sum(1 for r in records if r[6] == 1)
    running   = total - held
    senate    = sum(1 for r in records if r[4] == "S")
    house     = total - senate
    parties   = Counter(r[1] for r in records)

    print(f"\n{'='*50}")
    print(f"Records to write: {total}  (skipped {skipped} non-Congress)")
    print(f"  Incumbents (HELD):      {held}")
    print(f"  Challengers (RUNNING):  {running}")
    print(f"  Senate:                 {senate}")
    print(f"  House:                  {house}")
    print(f"  Rated (has score):      {rated}")
    print(f"  Unrated (score = -1):   {unrated}")
    print(f"  Party breakdown:        R={parties['R']} D={parties['D']} I={parties['I']} ?={parties['?']}")

    if DRY_RUN:
        print("\n[DRY RUN] Not writing file.")
        return

    with open(OUT_FILE, "w") as f:
        json.dump(records, f, separators=(",", ":"))

    size_kb = OUT_FILE.stat().st_size / 1024
    print(f"\nSaved → {OUT_FILE}  ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
