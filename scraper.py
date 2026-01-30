"""
Arcaea Fandom scraper — Songs by Level only.

- Songs by Level: scrape Songs_by_Level (Song, Artist, Difficulty, Chart Constant, Level, Version).

Uses MediaWiki API to fetch the parsed page content to avoid basic blocks.
"""

import argparse
import csv
import requests
from bs4 import BeautifulSoup

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

API_URL = "https://arcaea.fandom.com/api.php"
HEADERS = {
    "User-Agent": "ArcaeaChartsFetcher/1.0 (https://github.com/your-repo; gentle bot)",
    "Accept": "application/json",
}

SONGS_BY_LEVEL_PAGE = "Songs_by_Level"
REQUEST_TIMEOUT = 30


# -----------------------------------------------------------------------------
# CSV Helper
# -----------------------------------------------------------------------------

def save_to_csv(data, filename):
    """Save list of dicts to a CSV file."""
    if not data:
        print("No data to save.")
        return
    with open(filename, "w", newline="", encoding="utf-8") as out_file:
        writer = csv.DictWriter(out_file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"Saved {len(data)} rows to {filename}")


# -----------------------------------------------------------------------------
# MediaWiki API
# -----------------------------------------------------------------------------

def fetch_page_via_api(page_title):
    """Fetch parsed HTML for a wiki page using the MediaWiki API."""
    params = {
        "action": "parse",
        "page": page_title,
        "prop": "text",
        "format": "json",
        "redirects": "1",
    }
    response = requests.get(
        API_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT
    )
    response.raise_for_status()
    data = response.json()
    if "error" in data:
        raise ValueError(data["error"].get("info", str(data["error"])))
    return data["parse"]["text"]["*"]


# -----------------------------------------------------------------------------
# Songs by Level (Songs_by_Level page)
# -----------------------------------------------------------------------------

def parse_songs_by_level_html(html):  # pylint: disable=too-many-locals,too-many-branches
    """Parse the Songs by Level wiki page HTML into a list of row dicts.

    Table columns: Song, Artist, Difficulty, Chart Constant, Level, Version.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    # Fandom can use wikitable sortable, article-table sortable, or plain wikitable
    selectors = [
        "table.wikitable.sortable",
        "table.article-table.sortable",
        "table.wikitable",
        "table.sortable",
    ]
    tables = []
    for sel in selectors:
        tables = soup.select(sel)
        if tables:
            break
    if not tables:
        # Fallback: any table with 6+ columns in first data row
        for table in soup.select("table"):
            for row in table.select("tbody tr"):
                tds = row.select("td")
                if len(tds) >= 6:
                    tables = [table]
                    break
            if tables:
                break
    for table in tables:
        for row in table.select("tbody tr"):
            tds = row.select("td")
            if len(tds) < 6:
                continue
            # Song: often <a href="/wiki/...">Display name</a>
            song_cell = tds[0]
            song_link = song_cell.select_one("a")
            if song_link:
                song_title = song_link.get_text(strip=True)
            else:
                song_title = song_cell.get_text(strip=True)
            artist = tds[1].get_text(strip=True)
            difficulty = tds[2].get_text(strip=True)
            chart_constant = tds[3].get_text(strip=True)
            level = tds[4].get_text(strip=True)
            version = tds[5].get_text(strip=True)
            if not song_title:
                continue
            rows.append({
                "song": song_title,
                "artist": artist,
                "difficulty": difficulty,
                "chart_constant": chart_constant,
                "level": level,
                "version": version,
            })
    return rows


def scrape_songs_by_level(save_path=None):
    """Scrape the Songs by Level page via API and return (and optionally save) rows.

    Args:
        save_path: If set, save rows to this CSV file.

    Returns:
        List of dicts with keys: song, artist, difficulty, chart_constant, level, version.
    """
    print(f"Fetching {SONGS_BY_LEVEL_PAGE} via API...")
    html = fetch_page_via_api(SONGS_BY_LEVEL_PAGE)
    rows = parse_songs_by_level_html(html)
    print(f"Parsed {len(rows)} rows from Songs by Level.")
    if save_path:
        save_to_csv(rows, save_path)
    return rows


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Arcaea Fandom scraper (Songs by Level only)"
    )
    # Default behavior is just scraping songs by level if run directly
    parser.add_argument(
        "--output", "-o",
        help="Output CSV (default: songs_by_level.csv)",
        default="songs_by_level.csv"
    )
    args = parser.parse_args()
    scrape_songs_by_level(save_path=args.output)


if __name__ == "__main__":
    main()
