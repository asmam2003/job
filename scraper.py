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
# Greenhouse slug list
# ─────────────────────────────────────────────

# Community-maintained JSON list of Greenhouse board slugs.
# Format expected: list of objects with at least a "name" and "token"/"slug" field.
# If this fetch fails for any reason, falls back to GREENHOUSE_FALLBACK.
SLUG_LIST_URL = (
    "https://raw.githubusercontent.com/nicholasgasior/gsfull/master/boards.json"
)

# Fallback hardcoded list used if the community slug list is unavailable.
GREENHOUSE_FALLBACK = [
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
]

# Company name substrings suggesting a relevant industry.
# Filters the full slug list before scraping.
INDUSTRY_KEYWORDS = [
    "security", "cyber", "intelligence", "fraud", "risk",
    "fintech", "finance", "bank", "payment", "insurance",
    "tech", "software", "data", "cloud", "network",
    "defense", "analytics", "research", "investigation",
    "trust", "identity", "compliance", "audit",
]

# Hard cap on slugs scraped per run.
# At 0.5s/request this is ~17 min for 2000 companies.
MAX_GREENHOUSE_SLUGS = 2000


def _fetch_slug_list() -> list[tuple[str, str]]:
    """
    Fetch community slug list and return as list of (company_name, slug) tuples.
    Returns GREENHOUSE_FALLBACK if fetch or parse fails for any reason.
    """
    try:
        resp = requests.get(SLUG_LIST_URL, timeout=20, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()

        slugs = []
        for item in data:
            if isinstance(item, str):
                slugs.append((item, item))
            elif isinstance(item, dict):
                name = item.get("name") or item.get("company") or ""
                slug = (
                    item.get("slug")
                    or item.get("token")
                    or item.get("board_token")
                    or ""
                )
                if slug:
                    slugs.append((name or slug, slug))

        if not slugs:
            log.warning("Slug list fetched but empty -- using fallback")
            return GREENHOUSE_FALLBACK

        # Filter to industry-relevant companies
        filtered = [
            (name, slug) for name, slug in slugs
            if any(kw in name.lower() for kw in INDUSTRY_KEYWORDS)
        ]

        # If keyword filter is too aggressive, relax it
        if len(filtered) < 50:
            log.warning(
                f"Industry filter left only {len(filtered)} slugs -- relaxing filter"
            )
            filtered = slugs

        filtered = filtered[:MAX_GREENHOUSE_SLUGS]
        log.info(
            f"Slug list: using {len(filtered)} companies (from {len(slugs)} total)"
        )
        return filtered

    except Exception as e:
        log.warning(f"Slug list fetch failed ({e}) -- using fallback")
        return GREENHOUSE_FALLBACK


# ─────────────────────────────────────────────
# Greenhouse scraper
# ─────────────────────────────────────────────

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

    targets = _fetch_slug_list()

    for company_name, slug in targets:
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

        time.sleep(0.5)

    log.info(
        f"Greenhouse: {len(results)} listings from {len(seen_slugs)} companies"
    )
    return results


# ─────────────────────────────────────────────
# Simplify scraper (HTML -- may need selector updates if their layout changes)
# ─────────────────────────────────────────────

# Search terms sent to Simplify. Each becomes a separate request.
SIMPLIFY_SEARCHES = [
    "threat intelligence analyst",
    "fraud analyst",
    "trust and safety",
    "osint analyst",
    "security analyst",
    "detection engineer",
    "incident response analyst",
]

SIMPLIFY_BASE = "https://simplify.jobs/jobs"


def _parse_salary(text: str) -> tuple[int | None, int | None]:
    """Extract salary range from strings like '$80K - $120K' or '$95,000'."""
    matches = re.findall(r"\$(\d[\d,]*)[Kk]?", text)
    nums = []
    for m in matches:
        n = int(m.replace(",", ""))
        if n < 1000:
            n *= 1000
        nums.append(n)
    if len(nums) >= 2:
        return min(nums), max(nums)
    if len(nums) == 1:
        return nums[0], None
    return None, None


def scrape_simplify() -> list[dict]:
    results = []
    seen_urls = set()

    for keyword in SIMPLIFY_SEARCHES:
        try:
            resp = requests.get(
                SIMPLIFY_BASE,
                params={"search": keyword, "experience": "Entry Level,Junior"},
                headers=HEADERS,
                timeout=20,
            )
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # Primary selector targets -- update if Simplify changes their markup
            cards = soup.select(
                "div[data-testid='job-card'], .job-card, li.job, article.job"
            )

            # Fallback: find any parent containers of job links
            if not cards:
                cards = []
                for a in soup.select("a[href*='/jobs/']"):
                    parent = a.find_parent("div") or a.find_parent("li")
                    if parent and parent not in cards:
                        cards.append(parent)

            for card in cards:
                link_el     = card.select_one("a[href*='/jobs/']")
                title_el    = card.select_one(
                    "h2, h3, [data-testid='job-title'], .job-title"
                )
                company_el  = card.select_one(
                    "[data-testid='company-name'], .company, .employer"
                )
                location_el = card.select_one(
                    "[data-testid='location'], .location"
                )
                salary_el   = card.select_one(
                    "[data-testid='salary'], .salary, .compensation"
                )

                if not link_el or not title_el:
                    continue

                href = link_el.get("href", "")
                if not href.startswith("http"):
                    href = "https://simplify.jobs" + href
                if href in seen_urls:
                    continue
                seen_urls.add(href)

                sal_text = salary_el.get_text(strip=True) if salary_el else ""
                sal_min, sal_max = _parse_salary(sal_text)

                results.append({
                    "source":      "simplify",
                    "company":     company_el.get_text(strip=True) if company_el else "Unknown",
                    "title":       title_el.get_text(strip=True),
                    "location":    location_el.get_text(strip=True) if location_el else "",
                    "url":         href,
                    "date_posted": date.today(),
                    "raw_jd":      card.get_text(separator=" ", strip=True),
                    "salary_min":  sal_min,
                    "salary_max":  sal_max,
                })

        except Exception as e:
            # Per-keyword failure: log and continue to next keyword
            log.warning(f"Simplify scrape failed for '{keyword}': {e}")

        time.sleep(1)

    log.info(f"Simplify: {len(results)} listings collected")
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
    """
    Runs all scrapers. Each is independent -- a complete failure in one
    does not block the others. Results are pooled and returned together.
    """
    all_listings = []

    for scraper_fn in [
        scrape_greenhouse,
        scrape_simplify,    # HTML-based, may return 0 results if Simplify changes markup
        scrape_tspa,
        scrape_osint_jobs,
    ]:
        try:
            batch = scraper_fn()
            all_listings.extend(batch)
        except Exception as e:
            # Belt-and-suspenders catch -- each scraper handles its own errors
            # internally, but this ensures nothing propagates up to cron.py
            log.error(f"{scraper_fn.__name__} crashed entirely: {e}")

    log.info(f"Total raw listings collected: {len(all_listings)}")
    return all_listings
