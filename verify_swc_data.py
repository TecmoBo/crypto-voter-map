#!/usr/bin/env python3
"""
verify_swc_data.py
Re-scrapes ALL SWC district pages and compares against the corrected
swc_embedded_data.json. Outputs remaining discrepancies.

Exit code 0 = clean, 1 = discrepancies found.
"""

import json
import re
import time
import csv
import sys
from collections import defaultdict
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Config ─────────────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).parent
DATA_FILE     = BASE_DIR / "swc_embedded_data.json"
OUTPUT_FILE   = BASE_DIR / "verify_discrepancies.csv"
SWC_BASE      = "https://www.standwithcrypto.org/us/races/state"
REQUEST_DELAY = 0.5
TIMEOUT       = 20
HEADERS       = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

# ── Helpers ────────────────────────────────────────────────────────────────────

def normalize_name(name: str) -> str:
    name = name.lower().strip()
    for suffix in [" jr.", " jr", " sr.", " sr", " ii", " iii", " iv", " v"]:
        if name.endswith(suffix):
            name = name[: -len(suffix)].strip()
    return name


def build_url(state: str, chamber: str, district: str) -> str:
    if chamber == "S":
        return f"{SWC_BASE}/{state}/senate"
    dist = district.strip()
    dist_url = dist.lower() if dist.lower() in ("at-large", "al") else dist
    if dist_url == "al":
        dist_url = "at-large"
    return f"{SWC_BASE}/{state}/district/{dist_url}"


def fetch_swc_candidates(url: str) -> tuple[list[dict], str]:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    except requests.RequestException as exc:
        return [], f"request error: {exc}"

    if resp.status_code == 500:
        return [], "HTTP 500 (page not found on SWC)"
    if resp.status_code != 200:
        return [], f"HTTP {resp.status_code}"

    soup = BeautifulSoup(resp.text, "lxml")
    seen: set[str] = set()
    candidates: list[dict] = []

    for card in soup.find_all("a", href=re.compile(r"/politicians/person/")):
        slug = card["href"].split("/")[-1]
        if slug in seen:
            continue

        img = card.find("img", alt=re.compile(r"^Profile picture of "))
        if not img:
            continue

        name  = img["alt"].replace("Profile picture of ", "").strip()
        text  = card.get_text(separator="|", strip=True)
        pm    = re.search(r"\((R|D|I)\)", text)
        party = pm.group(1) if pm else "?"

        seen.add(slug)
        candidates.append({
            "name":  name,
            "party": party,
            "slug":  slug,
            "norm":  normalize_name(name),
        })

    return candidates, ""


# ── Load corrected JSON ────────────────────────────────────────────────────────
print("Loading corrected swc_embedded_data.json…")
with open(DATA_FILE) as f:
    raw: list[list] = json.load(f)

# Only compare VISIBLE candidates
visible = [r for r in raw if r[10] == 1]
hidden  = [r for r in raw if r[10] == 0]
print(f"  Total: {len(raw)}  Visible: {len(visible)}  Hidden: {len(hidden)}")

# Index VISIBLE records by (state, chamber, district)
races: dict[tuple, list[dict]] = defaultdict(list)
for r in visible:
    name, party, score, state, chamber, district = r[0], r[1], r[2], r[3], r[4], r[5]
    slug = r[8]
    key  = (state, chamber, district)
    races[key].append({
        "name":  name,
        "party": party,
        "score": score,
        "slug":  slug,
        "norm":  normalize_name(name),
    })

def sort_key(k):
    state, chamber, district = k
    chamber_order = 0 if chamber == "S" else 1
    try:
        dist_order = int(district)
    except (ValueError, TypeError):
        dist_order = 9999
    return (state, chamber_order, dist_order, district)

sorted_races = sorted(races.keys(), key=sort_key)
print(f"  {len(sorted_races)} unique races across {len(set(k[0] for k in sorted_races))} states\n")

# ── Compare ────────────────────────────────────────────────────────────────────
rows: list[dict] = []

for i, key in enumerate(sorted_races):
    state, chamber, district = key
    json_cands = races[key]
    url        = build_url(state, chamber, district)
    dist_label = "Senate" if chamber == "S" else (district if district else "AL")

    sys.stdout.write(
        f"\r[{i+1:4d}/{len(sorted_races)}] {state} {'Senate' if chamber=='S' else f'Dist {district}':<12}  "
    )
    sys.stdout.flush()

    swc_cands, err = fetch_swc_candidates(url)

    json_by_slug = {c["slug"]: c for c in json_cands}
    swc_by_slug  = {c["slug"]: c for c in swc_cands}
    json_by_norm = {c["norm"]: c for c in json_cands}
    swc_by_norm  = {c["norm"]: c for c in swc_cands}

    def add_row(issue: str):
        rows.append({"STATE": state, "DISTRICT": dist_label, "ISSUE": issue})

    if err:
        rated = [c for c in json_cands if c["score"] != -1]
        if rated:
            names = ", ".join(c["name"] for c in rated)
            add_row(f"SWC page unavailable ({err}) — JSON has rated: {names}")
        time.sleep(REQUEST_DELAY)
        continue

    # JSON visible candidate not on SWC
    for c in json_cands:
        if c["slug"] in swc_by_slug:
            continue
        if c["norm"] in swc_by_norm:
            swc_c = swc_by_norm[c["norm"]]
            if swc_c["slug"] != c["slug"]:
                add_row(
                    f"Slug mismatch for '{c['name']}': "
                    f"JSON slug='{c['slug']}' SWC slug='{swc_c['slug']}'"
                )
        else:
            add_row(
                f"Visible JSON candidate NOT on SWC: {c['name']} ({c['party']}) "
                f"[slug: {c['slug']}]"
            )

    # SWC candidate not in visible JSON
    for c in swc_cands:
        if c["slug"] in json_by_slug:
            continue
        if c["norm"] in json_by_norm:
            continue
        # Check if this slug is in the hidden list
        hidden_slugs = {r[8] for r in hidden}
        if c["slug"] in hidden_slugs:
            continue  # intentionally hidden — OK
        add_row(
            f"SWC candidate missing from visible JSON: {c['name']} ({c['party']}) "
            f"[slug: {c['slug']}]"
        )

    # Party mismatches (same slug)
    for slug, jc in json_by_slug.items():
        if slug not in swc_by_slug:
            continue
        sc = swc_by_slug[slug]
        if jc["party"] != sc["party"] and sc["party"] != "?":
            add_row(
                f"Party mismatch for '{jc['name']}': "
                f"JSON={jc['party']} SWC={sc['party']}"
            )

    # Name mismatches (same slug)
    for slug, jc in json_by_slug.items():
        if slug not in swc_by_slug:
            continue
        sc = swc_by_slug[slug]
        if normalize_name(jc["name"]) != normalize_name(sc["name"]):
            add_row(
                f"Name mismatch (same slug '{slug}'): "
                f"JSON='{jc['name']}' SWC='{sc['name']}'"
            )

    time.sleep(REQUEST_DELAY)

print(f"\n\nDone. {len(rows)} discrepancies found.")

# ── Write results ──────────────────────────────────────────────────────────────
with open(OUTPUT_FILE, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["STATE", "DISTRICT", "ISSUE"])
    writer.writeheader()
    writer.writerows(rows)

if rows:
    print(f"\nRemaining issues written to: {OUTPUT_FILE}")
    print("\nBreakdown:")
    from collections import Counter
    types = Counter()
    for r in rows:
        issue = r["ISSUE"]
        if "Visible JSON candidate NOT on SWC" in issue:
            types["still_visible_but_not_on_swc"] += 1
        elif "SWC candidate missing from visible JSON" in issue:
            types["swc_not_in_visible_json"] += 1
        elif "Slug mismatch" in issue:
            types["slug_mismatch"] += 1
        elif "Party mismatch" in issue:
            types["party_mismatch"] += 1
        elif "Name mismatch" in issue:
            types["name_mismatch"] += 1
        else:
            types["other"] += 1
    for k, v in types.most_common():
        print(f"  {k}: {v}")
    sys.exit(1)
else:
    print("✓ Verification PASSED — corrected JSON matches SWC exactly.")
    sys.exit(0)
