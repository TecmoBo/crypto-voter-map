#!/usr/bin/env python3
"""
Sweep visible candidates with score=-1, check SWC district pages for actual ratings,
flag mismatches, and apply fixes to local swc_embedded_data.json.
"""

import json
import time
import re
import sys
import requests
from bs4 import BeautifulSoup

# SWC stance → numeric score mapping
STANCE_SCORES = {
    "strongly supports crypto": 100,
    "somewhat supports crypto": 75,
    "neutral": 50,
    "somewhat against crypto": 25,
    "strongly against crypto": 0,
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def get_swc_url(state, chamber, district):
    state_code = state.lower()
    if chamber == "S":
        return f"https://www.standwithcrypto.org/us/races/state/{state_code}/senate"
    else:
        return f"https://www.standwithcrypto.org/us/races/state/{state_code}/district/{district}"


def normalize_name(name):
    """Lowercase, strip punctuation for fuzzy matching."""
    return re.sub(r"[^a-z ]", "", name.lower()).strip()


_page_text_cache = {}  # url → normalized page text (or None on error)


def fetch_page_text(url, verbose=False):
    if url in _page_text_cache:
        return _page_text_cache[url]
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            if verbose:
                print(f"  HTTP {resp.status_code} for {url}")
            _page_text_cache[url] = None
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(" ", strip=True)
        _page_text_cache[url] = text
        return text
    except Exception as e:
        if verbose:
            print(f"  Request error for {url}: {e}")
        _page_text_cache[url] = None
        return None


def extract_score_from_page(url, candidate_name, verbose=False):
    """
    Fetch a SWC district/senate page and look for the candidate's stance.
    Returns (score, stance_text) or (None, None) if not found.
    """
    page_text = fetch_page_text(url, verbose=verbose)
    if page_text is None:
        return None, None

    # Check if the candidate's name appears on the page
    norm_candidate = normalize_name(candidate_name)
    norm_page = normalize_name(page_text)

    if norm_candidate not in norm_page:
        if verbose:
            print(f"  '{candidate_name}' not found on page")
        return None, None

    # Find all occurrences of the candidate name and check for stance within
    # a window of characters after each occurrence
    WINDOW = 600
    start = 0
    while True:
        idx_name = norm_page.find(norm_candidate, start)
        if idx_name == -1:
            break
        window_text = norm_page[idx_name: idx_name + WINDOW]
        for stance_key, score in STANCE_SCORES.items():
            if stance_key in window_text:
                return score, stance_key
        start = idx_name + 1

    # Also search backwards (stance sometimes precedes name in the markup)
    start = 0
    while True:
        idx_name = norm_page.find(norm_candidate, start)
        if idx_name == -1:
            break
        window_text = norm_page[max(0, idx_name - WINDOW): idx_name + len(norm_candidate)]
        for stance_key, score in STANCE_SCORES.items():
            if stance_key in window_text:
                return score, stance_key
        start = idx_name + 1

    if verbose:
        print(f"  Name found but no stance in ±{WINDOW} chars")
    return None, None


def main():
    # Load local JSON
    with open("swc_embedded_data.json") as f:
        data = json.load(f)

    # Collect visible candidates with score = -1
    unrated_visible = []
    for i, c in enumerate(data):
        score = c[2]
        visible = c[10]
        if visible == 1 and score == -1:
            unrated_visible.append((i, c))

    print(f"Found {len(unrated_visible)} visible candidates with score=-1\n")

    updates = []
    for idx, (i, c) in enumerate(unrated_visible):
        name, party, _, state, chamber, district = c[0], c[1], c[2], c[3], c[4], c[5]
        url = get_swc_url(state, chamber, district)

        print(f"[{idx+1}/{len(unrated_visible)}] {name} ({party}, {state} {chamber} {district})")
        print(f"  URL: {url}")

        first_fetch = url not in _page_text_cache
        score, stance = extract_score_from_page(url, name, verbose=True)
        if first_fetch:
            time.sleep(0.5)

        if score is not None:
            print(f"  MATCH: SWC shows '{stance}' → score {score}")
            updates.append((i, name, state, chamber, district, score, stance))
        else:
            print(f"  No rating found on SWC page")

    print(f"\n{'='*60}")
    print(f"Score mismatches found: {len(updates)}")
    print(f"{'='*60}\n")

    if not updates:
        print("No updates needed.")
        return

    # Apply updates
    for i, name, state, chamber, district, score, stance in updates:
        print(f"  Updating {name} ({state} {chamber} {district}): -1 → {score} ({stance})")
        data[i][2] = score

    # Save updated JSON
    with open("swc_embedded_data.json", "w") as f:
        json.dump(data, f, separators=(",", ":"))

    print(f"\nSaved {len(updates)} updates to swc_embedded_data.json")
    return len(updates)


if __name__ == "__main__":
    main()
