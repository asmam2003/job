"""
Cron entry point.
On Render, deploy this as a separate Cron Job service.
Command: python cron.py
Schedule: 0 8,20 * * *  (runs at 8am and 8pm UTC)
"""

import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger(__name__)


def run():
    from models import init_db, get_session, Listing
    from scraper import run_all_scrapers
    from filters import passes
    from scorer import score_unscored
    from jd_fetcher import enrich_listings
    from sqlalchemy.exc import IntegrityError

    log.info("=== JobScout cron starting ===")

    init_db()
    session = get_session()

    # 1. Scrape all sources
    raw_listings = run_all_scrapers()
    log.info(f"Collected {len(raw_listings)} raw listings")

    # 2. First-pass keyword filter + dedup (cheap, no network calls)
    candidates = []
    seen_company_title = set()
    filtered = 0

    for data in raw_listings:
        ok, reason = passes(data)
        if not ok:
            log.debug(f"Filtered [{reason}]: {data.get('company')} - {data.get('title')}")
            filtered += 1
            continue

        dedup_key = (
            (data.get("company") or "").lower().strip(),
            (data.get("title") or "").lower().strip(),
        )
        if dedup_key in seen_company_title:
            filtered += 1
            continue
        seen_company_title.add(dedup_key)
        candidates.append(data)

    log.info(f"First-pass filter: {len(candidates)} candidates, {filtered} filtered")

    # 3. Fetch full JDs for candidates that passed keyword filter
    # This replaces truncated Adzuna snippets with full posting text
    # so clearance requirements, experience requirements, etc. are visible
    if candidates:
        log.info(f"Fetching full JDs for {len(candidates)} candidates...")
        candidates = enrich_listings(candidates, delay=0.5)

    # 4. Second-pass filter with full JD text
    passed = []
    second_filtered = 0
    for data in candidates:
        ok, reason = passes(data)
        if not ok:
            log.debug(f"Second-pass filtered [{reason}]: {data.get('company')} - {data.get('title')}")
            second_filtered += 1
            continue
        passed.append(data)

    log.info(f"Second-pass filter: {len(passed)} passed, {second_filtered} filtered on full JD")

    # 5. Insert into DB
    inserted = 0
    for data in passed:
        listing = Listing(
            source      = data["source"],
            company     = data["company"],
            title       = data["title"],
            location    = data.get("location", ""),
            url         = data["url"],
            date_posted = data.get("date_posted"),
            raw_jd      = data.get("raw_jd", ""),
            salary_min  = data.get("salary_min"),
            salary_max  = data.get("salary_max"),
            is_agency   = False,
        )
        try:
            session.add(listing)
            session.commit()
            inserted += 1
        except IntegrityError:
            session.rollback()

    log.info(f"Inserted {inserted} new listings")

    # 6. Score anything unscored
    scored = score_unscored(session)
    log.info(f"Scored {scored} listings")

    session.close()
    log.info("=== JobScout cron complete ===")


if __name__ == "__main__":
    run()
