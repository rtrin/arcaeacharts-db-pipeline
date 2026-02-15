#!/usr/bin/env python3
"""
Full sync pipeline: scrape Songs by Level, build CSV, upsert into songs table.
No image downloading or uploading.

Credentials from env: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (required for writes).
"""

import argparse
import csv
import logging
import os
import sys
from pathlib import Path

from supabase import create_client, Client  # pylint: disable=import-error
from scraper import scrape_songs_by_level, fetch_song, scrape_news_links, filter_song_pages

# -----------------------------------------------------------------------------
# Env & Config
# -----------------------------------------------------------------------------

def _load_env() -> None:
    
    """Load .env via python-dotenv when available."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

def _get_supabase_credentials() -> tuple[str, str]:
    """Return (url, key) from env."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set."
        )
    return url, key

_load_env()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

SONGS_BY_LEVEL_CSV = "songs_by_level.csv"
EXPORT_CSV = "songs_export.csv"


def get_supabase_client() -> Client:
    """Create and return Supabase client using env credentials."""
    url, key = _get_supabase_credentials()
    return create_client(url, key)


def run_pipeline(skip_scrape: bool = False) -> None:
    """Run sync: scrape songs by level, upsert to DB (metadata only)."""
    supabase = get_supabase_client()
    project_root = Path(__file__).resolve().parent
    csv_path = project_root / SONGS_BY_LEVEL_CSV

    # 1. Scrape Songs by Level
    if skip_scrape and csv_path.exists():
        with open(csv_path, newline="", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            rows = list(reader)
        logger.info("Using existing %s (%d rows).", SONGS_BY_LEVEL_CSV, len(rows))
    else:
        rows = scrape_songs_by_level(save_path=str(csv_path))
        if not rows:
            logger.error("No rows from scrape. Exiting.")
            return

    # 1b. Gap Check (News Section Songs vs Songs_by_Level)
    # Automatically scrape song links from the News section and check for missing songs.
    
    logger.info("Scraping News section for new songs...")
    try:
        # Get all page links from News section
        news_page_titles = scrape_news_links()
        
        # Filter to only song pages using API
        song_titles = filter_song_pages(news_page_titles)
        
        if not song_titles:
            logger.info("No song pages found in News section.")
        else:
            # Normalize existing titles for comparison
            existing_titles_csv = set((r.get("song") or "").strip().lower() for r in rows)
            
            # Find missing songs
            missing_titles = []
            for title in song_titles:
                # Normalize for comparison (replace underscores, lowercase)
                norm_title = title.replace("_", " ").strip().lower()
                if norm_title not in existing_titles_csv:
                    missing_titles.append(title)
            
            logger.info(f"Found {len(missing_titles)} new songs from News section.")
            
            # Fetch missing songs
            fetched_count = 0
            for m_title in missing_titles:
                new_entries = fetch_song(m_title)
                if new_entries:
                    logger.info(f"Added new song: {m_title}")
                    rows.extend(new_entries)
                    fetched_count += 1
                else:
                    logger.warning(f"Could not parse data for {m_title}")
                    
            logger.info(f"Added {fetched_count} new songs from News section.")
        
    except Exception as e:
        logger.error(f"News section scraping failed: {e}")
        # We continue with what we have


    # 2. Build rows for export/upsert (Metadata Only)
    unique_rows = {} # (title, artist, difficulty) -> row_dict
    export_rows = []
    
    for row in rows:
        # Standardize fields
        const_val = row.get("chart_constant")
        if const_val in [None, "", "-"]:
             const_val = None
        else:
             try:
                 const_val = float(const_val)
             except (ValueError, TypeError):
                 const_val = None

        # Exclude songs with constant > 13 per user request
        if const_val is not None and const_val > 13:
            continue

        r = {
            "title": (row.get("song") or "").strip(),
            "artist": (row.get("artist") or "").strip(),
            "difficulty": (row.get("difficulty") or "").strip(),
            "constant": const_val,
            "level": (row.get("level") or "").strip(),
            "version": (row.get("version") or "").strip(),
            # imageUrl removed
        }
        # Unique key for deduplication
        key = (r["title"], r["artist"], r["difficulty"])
        unique_rows[key] = r # Latest entry wins

    db_rows = list(unique_rows.values())
    
    # Rebuild export rows from the unique set to match DB
    export_rows = []
    for r in db_rows:
        export_rows.append({
            "song": r["title"],
            "artist": r["artist"],
            "difficulty": r["difficulty"],
            "chart_constant": r["constant"],
            "level": r["level"],
            "version": r["version"]
        })

    # 3. Write export CSV
    export_path = project_root / EXPORT_CSV
    fieldnames = [
        "song", "artist", "difficulty",
        "chart_constant", "level", "version",
    ]
    with open(export_path, "w", newline="", encoding="utf-8") as out_file:
        writer = csv.DictWriter(out_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(export_rows)
    logger.info("Wrote %d rows to %s.", len(export_rows), EXPORT_CSV)

    # 4. Upsert into Supabase
    batch_size = 100
    total = 0
    for i in range(0, len(db_rows), batch_size):
        batch = db_rows[i : i + batch_size]
        supabase.table("songs").upsert(
            batch,
            on_conflict="title,artist,difficulty",
            ignore_duplicates=False, # Update existing
        ).execute()
        total += len(batch)
        logger.info("Upserted rows %d-%d", i + 1, total)
    logger.info("Done. Upserted %d rows into songs table.", total)


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Sync songs metadata to Supabase (no images)."
    )
    parser.add_argument(
        "--skip-scrape", action="store_true",
        help="Reuse existing songs_by_level.csv",
    )
    args = parser.parse_args()
    try:
        run_pipeline(skip_scrape=args.skip_scrape)
        return 0
    except Exception as err:
        logger.error("Pipeline failed: %s", err)
        raise SystemExit(1) from err


if __name__ == "__main__":
    raise SystemExit(main())
