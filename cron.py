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
    from sqlalchemy.exc import IntegrityError

    log.info("=== JobScout cron starting ===")

    init_db()
    session = get_session()

    # 1. Scrape all sources
    raw_listings = run_all_scrapers()
    log.info(f"Collected {len(raw_listings)} raw listings")

    # 2. Filter and insert new ones
    inserted = 0
    filtered = 0
    for data in raw_listings:
        ok, reason = passes(data)
        if not ok:
            log.debug(f"Filtered [{reason}]: {data.get('company')} - {data.get('title')}")
            filtered += 1
            continue

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
            session.rollback()  # duplicate URL, skip silently

    log.info(f"Inserted {inserted} new listings, filtered {filtered}")

    # 3. Score anything unscored
    scored = score_unscored(session)
    log.info(f"Scored {scored} listings")

    session.close()
    log.info("=== JobScout cron complete ===")


if __name__ == "__main__":
    run()
