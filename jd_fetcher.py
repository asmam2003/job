"""
Full JD fetcher.
Attempts to retrieve the complete job description from the posting URL.
Falls back to the original snippet if the fetch fails or times out.
"""

import requests
from bs4 import BeautifulSoup
import logging
import time

log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

TIMEOUT = 10  # seconds per request

# CSS selectors tried in order for each ATS/job board
# First match wins
JD_SELECTORS = [
    # Greenhouse
    "#content",
    ".content",
    "#app_body",
    # Lever
    ".posting-categories + div",
    ".section-wrapper",
    # Workday
    "[data-automation-id='jobPostingDescription']",
    # Indeed
    "#jobDescriptionText",
    ".jobsearch-JobComponent-description",
    # Generic fallbacks
    "[class*='job-description']",
    "[class*='jobDescription']",
    "[class*='description']",
    "article",
    "main",
]

# Tags to strip from extracted text
STRIP_TAGS = ["script", "style", "nav", "header", "footer", "aside"]


def _extract_text(html: str) -> str:
    """Extract clean job description text from raw HTML."""
    soup = BeautifulSoup(html, "html.parser")

    # Remove noise tags
    for tag in soup(STRIP_TAGS):
        tag.decompose()

    # Try each selector
    for selector in JD_SELECTORS:
        el = soup.select_one(selector)
        if el:
            text = el.get_text(separator=" ", strip=True)
            if len(text) > 200:  # ignore tiny matches
                return text

    # Last resort: full page body text
    body = soup.find("body")
    if body:
        return body.get_text(separator=" ", strip=True)

    return soup.get_text(separator=" ", strip=True)


def fetch_full_jd(url: str, fallback: str = "") -> str:
    """
    Fetch full JD text from a posting URL.
    Returns the full text if successful, or the fallback snippet if not.
    """
    if not url:
        return fallback

    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        resp.raise_for_status()

        text = _extract_text(resp.text)

        # Sanity check -- if we got less than the fallback, keep the fallback
        if len(text) < len(fallback or ""):
            return fallback

        log.debug(f"Fetched full JD ({len(text)} chars) from {url}")
        return text

    except Exception as e:
        log.debug(f"Full JD fetch failed for {url}: {e}")
        return fallback


def enrich_listings(listings: list[dict], delay: float = 0.5) -> list[dict]:
    """
    For each listing, attempt to fetch the full JD and replace the snippet.
    Modifies listings in place and returns them.
    """
    total = len(listings)
    for i, listing in enumerate(listings):
        url      = listing.get("url", "")
        fallback = listing.get("raw_jd", "") or ""

        full_jd = fetch_full_jd(url, fallback)
        listing["raw_jd"] = full_jd

        if (i + 1) % 10 == 0:
            log.info(f"JD enrichment: {i + 1}/{total}")

        time.sleep(delay)

    log.info(f"JD enrichment complete: {total} listings processed")
    return listings
