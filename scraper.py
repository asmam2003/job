"""
Scrapers for each source.
Each scraper returns a list of dicts matching the Listing schema.
All scrapers are independently fault-tolerant: if one fails entirely,
run_all_scrapers() logs it and moves on.
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
import logging
import time
import os
import re

log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

# ─────────────────────────────────────────────
# Adzuna API
# ─────────────────────────────────────────────

ADZUNA_APP_ID  = os.environ.get("ADZUNA_APP_ID", "")
ADZUNA_APP_KEY = os.environ.get("ADZUNA_APP_KEY", "")
ADZUNA_BASE    = "https://api.adzuna.com/v1/api/jobs/us/search"

# Keywords to search -- each becomes a separate API call
ADZUNA_SEARCHES = [
    "threat intelligence analyst",
    "fraud analyst",
    "trust and safety analyst",
    "osint analyst",
    "security analyst",
    "detection engineer",
    "incident response analyst",
    "criminal intelligence analyst",
    "cyber intelligence analyst",
]

RESULTS_PER_SEARCH = 50  # Adzuna max per page is 50


def _parse_adzuna_date(s: str):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        return None


def scrape_adzuna() -> list[dict]:
    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
        log.warning("Adzuna credentials not set -- skipping")
        return []

    results = []
    seen_urls = set()

    for keyword in ADZUNA_SEARCHES:
        try:
            params = {
                "app_id":         ADZUNA_APP_ID,
                "app_key":        ADZUNA_APP_KEY,
                "results_per_page": RESULTS_PER_SEARCH,
                "what":           keyword,
                "content-type":   "application/json",
                "sort_by":        "date",
                # Only results from last 21 days
                "max_days_old":   21,
            }
            resp = requests.get(
                f"{ADZUNA_BASE}/1",
                params=params,
                headers=HEADERS,
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()

            for job in data.get("results", []):
                url = job.get("redirect_url", "")
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                # Parse salary
                sal_min = job.get("salary_min")
                sal_max = job.get("salary_max")
                sal_min = int(sal_min) if sal_min else None
                sal_max = int(sal_max) if sal_max else None

                location = ""
                loc = job.get("location", {})
                display_name = loc.get("display_name", "")
                area = loc.get("area", [])
                if display_name:
                    location = display_name
                elif area:
                    location = ", ".join(area[-2:])

                company = ""
                company_obj = job.get("company", {})
                if company_obj:
                    company = company_obj.get("display_name", "")

                results.append({
                    "source":      "adzuna",
                    "company":     company or "Unknown",
                    "title":       job.get("title", ""),
                    "location":    location,
                    "url":         url,
                    "date_posted": _parse_adzuna_date(job.get("created")),
                    "raw_jd":      job.get("description", ""),
                    "salary_min":  sal_min,
                    "salary_max":  sal_max,
                })

        except Exception as e:
            log.warning(f"Adzuna scrape failed for '{keyword}': {e}")

        time.sleep(0.5)

    log.info(f"Adzuna: {len(results)} listings collected")
    return results


# ─────────────────────────────────────────────
# Greenhouse fallback (hardcoded target companies)
# ─────────────────────────────────────────────

GREENHOUSE_TARGETS = [
    ("Recorded Future",    "recordedfuture"),
    ("SentiLink",          "sentilink"),
    ("Cloudflare",         "cloudflare"),
    ("Roblox",             "roblox"),
    ("Visa",               "visa"),
    ("CrowdStrike",        "crowdstrike"),
    ("Palo Alto Networks", "paloaltonetworks"),
    ("Chainalysis",        "chainalysis"),
    ("Flashpoint",         "flashpoint"),
    ("Abnormal Security",  "abnormalsecurity"),
    ("Coalition",          "coalitioninc"),
    ("Huntress",           "huntress"),
    ("Axonius",            "axonius"),
    ("Dragos",             "dragos"),
    ("Intel 471",          "intel471"),
    ("ZeroFox",            "zerofox"),
    ("Nisos",              "nisos"),
    ("Sift",               "sift"),
    ("Socure",             "socure"),
    ("Persona",            "persona"),
    ("Sardine",            "sardineai"),
    ("Unit21",             "unit21"),
    ("Mastercard",         "mastercard"),
    ("Stripe",             "stripe"),
    ("Brex",               "brex"),
    ("Marqeta",            "marqeta"),
    ("Plaid",              "plaid"),
    ("Affirm",             "affirm"),
    ("Robinhood",          "robinhood"),
    ("Coinbase",           "coinbase"),
    ("Gemini",             "gemini"),
    ("TikTok",             "tiktok"),
    ("Discord",            "discord"),
    ("Twitch",             "twitch"),
    ("Reddit",             "reddit"),
    ("Snap",               "snap"),
    ("Bumble",             "bumble"),
    ("Airbnb",             "airbnb"),
    ("Lyft",               "lyft"),
    ("DoorDash",           "doordash"),
    ("Instacart",          "instacart"),
    ("Databricks",         "databricks"),
    ("Palantir",           "palantir"),
    ("Recorded Future",    "recordedfuture"),
    ("Google",             "google"),
    ("Microsoft",          "microsoft"),
    ("Amazon",             "amazon"),
    ("Meta",               "meta"),
]


def _parse_gh_date(s: str):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        return None


def scrape_greenhouse() -> list[dict]:
    results = []
    seen_slugs = set()

    for company_name, slug in GREENHOUSE_TARGETS:
        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)

        url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.debug(f"Greenhouse: skipping {company_name} ({slug}): {e}")
            time.sleep(0.5)
            continue

        for job in data.get("jobs", []):
            jd_html = job.get("content", "") or ""
            jd_text = BeautifulSoup(jd_html, "html.parser").get_text(separator=" ")

            location = ""
            offices = job.get("offices", [])
            if offices:
                location = offices[0].get("name", "")

            results.append({
                "source":      "greenhouse",
                "company":     company_name,
                "title":       job.get("title", ""),
                "location":    location,
                "url":         job.get("absolute_url", ""),
                "date_posted": _parse_gh_date(job.get("updated_at")),
                "raw_jd":      jd_text.strip(),
                "salary_min":  None,
                "salary_max":  None,
            })

        time.sleep(0.3)

    log.info(f"Greenhouse: {len(results)} listings from {len(seen_slugs)} companies")
    return results


# ─────────────────────────────────────────────
# TSPA Job Board
# ─────────────────────────────────────────────

TSPA_URL = "https://www.tspa.org/explore/job-board"


def scrape_tspa() -> list[dict]:
    results = []
    try:
        resp = requests.get(TSPA_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        for card in soup.select(".job-listing, .job-card, article.job"):
            title_el    = card.select_one("h2, h3, .job-title")
            company_el  = card.select_one(".company, .employer")
            link_el     = card.select_one("a[href]")
            location_el = card.select_one(".location")

            if not title_el or not link_el:
                continue

            href = link_el["href"]
            if not href.startswith("http"):
                href = "https://www.tspa.org" + href

            results.append({
                "source":      "tspa",
                "company":     company_el.get_text(strip=True) if company_el else "Unknown",
                "title":       title_el.get_text(strip=True),
                "location":    location_el.get_text(strip=True) if location_el else "",
                "url":         href,
                "date_posted": date.today(),
                "raw_jd":      card.get_text(separator=" ", strip=True),
                "salary_min":  None,
                "salary_max":  None,
            })

    except Exception as e:
        log.warning(f"TSPA scrape failed: {e}")

    log.info(f"TSPA: {len(results)} listings collected")
    return results


# ─────────────────────────────────────────────
# OSINT-Jobs
# ─────────────────────────────────────────────

OSINT_JOBS_URL = "https://osint-jobs.com"


def scrape_osint_jobs() -> list[dict]:
    results = []
    try:
        resp = requests.get(OSINT_JOBS_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        for card in soup.select(".job, .listing, article"):
            title_el    = card.select_one("h2, h3, .title")
            company_el  = card.select_one(".company, .employer, .org")
            link_el     = card.select_one("a[href]")
            location_el = card.select_one(".location, .geo")

            if not title_el or not link_el:
                continue

            href = link_el["href"]
            if not href.startswith("http"):
                href = OSINT_JOBS_URL + href

            results.append({
                "source":      "osint-jobs",
                "company":     company_el.get_text(strip=True) if company_el else "Unknown",
                "title":       title_el.get_text(strip=True),
                "location":    location_el.get_text(strip=True) if location_el else "",
                "url":         href,
                "date_posted": date.today(),
                "raw_jd":      card.get_text(separator=" ", strip=True),
                "salary_min":  None,
                "salary_max":  None,
            })

    except Exception as e:
        log.warning(f"OSINT-Jobs scrape failed: {e}")

    log.info(f"OSINT-Jobs: {len(results)} listings collected")
    return results


# ─────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────

def run_all_scrapers() -> list[dict]:
    all_listings = []

    for scraper_fn in [
        scrape_adzuna,
        scrape_greenhouse,
        scrape_tspa,
        scrape_osint_jobs,
    ]:
        try:
            batch = scraper_fn()
            all_listings.extend(batch)
        except Exception as e:
            log.error(f"{scraper_fn.__name__} crashed entirely: {e}")

    log.info(f"Total raw listings collected: {len(all_listings)}")
    return all_listings
