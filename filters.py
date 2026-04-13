"""
Hard filters that run cheaply before any LLM call.
A listing must pass ALL filters to be queued for scoring.
"""

from datetime import date, timedelta
import re

# Phrases that indicate a staffing agency posting
AGENCY_SIGNALS = [
    "our client",
    "confidential company",
    "undisclosed company",
    "staffing",
    "on behalf of",
    "contract-to-hire",
    "c2h",
    "w2 only",
    "1099",
]

# Titles that are clearly wrong track regardless of keywords
TITLE_BLOCKLIST = [
    "sales",
    "account executive",
    "marketing",
    "hr ",
    "human resources",
    "recruiter",
    "physical security",
    "safety officer",
    "facilities",
    "janitor",
    "administrative assistant",
]

# Must see at least one of these in title or JD to pass
RELEVANCE_SIGNALS = [
    "threat intel",
    "threat intelligence",
    "osint",
    "fraud",
    "trust and safety",
    "trust & safety",
    "incident response",
    "detection engineer",
    "security analyst",
    "intelligence analyst",
    "investigations",
    "soc analyst",
    "cyber",
    "information security",
    "infosec",
]

# Hard no -- clearance-required signals
CLEARANCE_SIGNALS = [
    "ts/sci",
    "secret clearance",
    "top secret",
    "security clearance required",
    "active clearance",
    "sf-86",
    "polygraph",
]

MAX_AGE_DAYS = 21


def is_agency(text: str) -> bool:
    text_lower = text.lower()
    return any(signal in text_lower for signal in AGENCY_SIGNALS)


def requires_clearance(text: str) -> bool:
    text_lower = text.lower()
    return any(signal in text_lower for signal in CLEARANCE_SIGNALS)


def is_relevant(title: str, jd: str) -> bool:
    combined = (title + " " + (jd or "")).lower()
    return any(signal in combined for signal in RELEVANCE_SIGNALS)


def title_blocked(title: str) -> bool:
    title_lower = title.lower()
    return any(block in title_lower for block in TITLE_BLOCKLIST)


def is_recent(posted_date) -> bool:
    if posted_date is None:
        return True  # give benefit of the doubt if date unknown
    cutoff = date.today() - timedelta(days=MAX_AGE_DAYS)
    return posted_date >= cutoff


def passes(listing: dict) -> tuple[bool, str]:
    """
    Returns (True, "") if listing passes all filters.
    Returns (False, reason) if it fails.
    """
    title = listing.get("title", "")
    jd    = listing.get("raw_jd", "") or ""
    full_text = title + " " + jd

    if title_blocked(title):
        return False, "title blocklist"

    if is_agency(full_text):
        return False, "agency posting"

    if requires_clearance(full_text):
        return False, "requires clearance"

    if not is_relevant(title, jd):
        return False, "not relevant"

    if not is_recent(listing.get("date_posted")):
        return False, "too old"

    return True, ""
