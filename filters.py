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

# Title substrings that indicate wrong track or seniority level
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
    # Seniority/management blocklist
    "senior",
    "sr.",
    "sr ",
    "principal",
    "staff ",
    "lead ",
    "team lead",
    "manager",
    "director",
    "head of",
    "vp ",
    "vice president",
    "distinguished",
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

# Patterns indicating required experience of 3+ years
# Matches things like "3+ years required", "5 years of experience required",
# "requires 4 years", "minimum 3 years" etc.
# Only triggers on required/minimum framing, not "preferred" or "nice to have"
EXPERIENCE_REQUIRED_PATTERNS = [
    r"\b([3-9]|\d{2,})\+?\s*years?\s+of\s+(?:relevant\s+)?experience\s+(?:is\s+)?required",
    r"\brequires?\s+([3-9]|\d{2,})\+?\s*years?",
    r"\bminimum\s+(?:of\s+)?([3-9]|\d{2,})\+?\s*years?",
    r"\bat\s+least\s+([3-9]|\d{2,})\+?\s*years?",
    r"\b([3-9]|\d{2,})\+\s*years?\s+(?:of\s+)?(?:professional\s+|relevant\s+|work\s+)?experience",
]

# US location signals -- if location is present, must contain one of these
US_SIGNALS = [
    "united states",
    "usa",
    "u.s.",
    ", al", ", ak", ", az", ", ar", ", ca", ", co", ", ct",
    ", dc", ", de", ", fl", ", ga", ", hi", ", id", ", il",
    ", in", ", ia", ", ks", ", ky", ", la", ", me", ", md",
    ", ma", ", mi", ", mn", ", ms", ", mo", ", mt", ", ne",
    ", nv", ", nh", ", nj", ", nm", ", ny", ", nc", ", nd",
    ", oh", ", ok", ", or", ", pa", ", ri", ", sc", ", sd",
    ", tn", ", tx", ", ut", ", vt", ", va", ", wa", ", wv",
    ", wi", ", wy",
    "new york", "san francisco", "los angeles", "chicago",
    "seattle", "austin", "boston", "dallas", "denver",
    "atlanta", "miami", "washington", "remote",
    "remote - usa", "remote - us", "us remote",
    "anywhere in the us", "anywhere in the united states",
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


def requires_too_much_experience(jd: str) -> bool:
    """
    Returns True if JD explicitly requires 3+ years as a hard requirement.
    Ignores 'preferred', 'nice to have', 'a plus' framing.
    """
    if not jd:
        return False
    jd_lower = jd.lower()

    # Skip if the years mention is in a preferred/nice-to-have context
    # by checking surrounding context
    for pattern in EXPERIENCE_REQUIRED_PATTERNS:
        matches = re.finditer(pattern, jd_lower)
        for match in matches:
            # Check 60 chars after match for softening language
            end = match.end()
            context_after = jd_lower[end:end+60]
            if any(soft in context_after for soft in ["preferred", "nice to", "a plus", "bonus"]):
                continue
            return True
    return False


def is_us_location(location: str) -> bool:
    """
    Returns True if location is in the US or unspecified (benefit of the doubt).
    Returns False if location is clearly outside the US.
    """
    if not location or location.strip() == "":
        return True  # no location listed, don't filter out

    loc_lower = location.lower()

    # Explicitly non-US signals
    non_us = [
        "canada", "united kingdom", "uk", "london", "india", "australia",
        "germany", "france", "singapore", "ireland", "netherlands",
        "poland", "spain", "brazil", "mexico", "japan", "china",
        "portugal", "sweden", "denmark", "norway", "finland",
        "gurugram", "bangalore", "toronto", "vancouver", "sydney",
        "melbourne", "berlin", "paris", "amsterdam", "dublin",
        "gothenburg", "stockholm", "gothenburg"
    ]
    if any(sig in loc_lower for sig in non_us):
        return False

    # Check for US signals
    if any(sig in loc_lower for sig in US_SIGNALS):
        return True

    # If location is present but ambiguous, let it through
    return True


def is_recent(posted_date) -> bool:
    if posted_date is None:
        return True
    cutoff = date.today() - timedelta(days=MAX_AGE_DAYS)
    return posted_date >= cutoff


def passes(listing: dict) -> tuple[bool, str]:
    """
    Returns (True, "") if listing passes all filters.
    Returns (False, reason) if it fails.
    """
    title    = listing.get("title", "")
    jd       = listing.get("raw_jd", "") or ""
    location = listing.get("location", "") or ""
    full_text = title + " " + jd

    if title_blocked(title):
        return False, "title blocklist / seniority"

    if is_agency(full_text):
        return False, "agency posting"

    if requires_clearance(full_text):
        return False, "requires clearance"

    if not is_relevant(title, jd):
        return False, "not relevant"

    if not is_recent(listing.get("date_posted")):
        return False, "too old"

    if requires_too_much_experience(jd):
        return False, "requires 3+ years experience"

    if not is_us_location(location):
        return False, "non-US location"

    return True, ""
