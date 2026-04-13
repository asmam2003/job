"""
Hard filters that run cheaply before any LLM call.
A listing must pass ALL filters to be queued for scoring.
"""

from datetime import date, timedelta
import re

# ─────────────────────────────────────────────
# Agency signals
# ─────────────────────────────────────────────

AGENCY_SIGNALS = [
    "our client",
    "confidential company",
    "undisclosed company",
    "on behalf of",
    "contract-to-hire",
    "c2h",
    "w2 only",
    "1099",
]

# ─────────────────────────────────────────────
# Title blocklist -- wrong track OR seniority
# ─────────────────────────────────────────────

# These are checked as whole-word or prefix matches against the lowercased title
TITLE_BLOCKLIST_EXACT = [
    "sales",
    "account executive",
    "marketing",
    "human resources",
    "recruiter",
    "physical security",
    "safety officer",
    "facilities",
    "administrative assistant",
    "technical writer",
    "technical trainer",
    "trainer",
    "training coordinator",
    "product manager",
    "product operations",
    "program manager",
    "software engineer",
    "software developer",
    "data engineer",
    "data scientist",
    "machine learning engineer",
    "ml engineer",
    "api engineer",
    "quality analyst",
    "qa engineer",
    "solutions engineer",
    "forward deployed engineer",
    "vulnerability management",
    "knowledge manager",
    "operations coordinator",
    "support specialist",
    "support engineer",
    "customer support",
    "business analyst",   # too generic -- catches fraud model analyst etc.
]

# Seniority/management -- checked as substring anywhere in title
SENIORITY_BLOCKLIST = [
    "senior",
    " sr.",
    " sr ",
    "principal",
    "staff ",
    " lead",
    "team lead",
    "manager",
    "director",
    "head of",
    " vp ",
    "vice president",
    "distinguished",
    "subject matter expert",
    " sme",
    "level iii",
    "level 3",
    " iii ",
    ", iii",
    "journeyman",   # DoD/contracting mid-level term
    "expert",
]

# ─────────────────────────────────────────────
# Relevance -- must match at least one
# ─────────────────────────────────────────────

RELEVANCE_SIGNALS = [
    "threat intel",
    "threat intelligence",
    "osint",
    "fraud analyst",
    "fraud investigat",
    "fraud risk",
    "fraud strategy",
    "trust and safety",
    "trust & safety",
    "incident response analyst",
    "detection engineer",
    "security analyst",
    "intelligence analyst",
    "criminal intelligence",
    "cyber intelligence",
    "soc analyst",
    "information security analyst",
    "insider threat",
    "anti-fraud",
]

# ─────────────────────────────────────────────
# Clearance -- hard no
# ─────────────────────────────────────────────

CLEARANCE_SIGNALS = [
    "ts/sci",
    "top secret",
    "secret clearance",
    "security clearance required",
    "security clearance needed",
    "active clearance",
    "clearance required",
    "clearance eligible",
    "sf-86",
    "sf86",
    "polygraph",
    "security clearance",   # broad catch -- most gov contractor roles
    "dod clearance",
    "government clearance",
    "federal clearance",
    "clearance preferred",
    "must be clearable",
]

# ─────────────────────────────────────────────
# Language -- hard no if requires a language Asma doesn't speak
# Arabic and basic Russian are fine. Chinese, French, etc. are not.
# ─────────────────────────────────────────────

LANGUAGE_BLOCKLIST = [
    "chinese language",
    "mandarin",
    "cantonese",
    "japanese language",
    "korean language",
    "french language",
    "german language",
    "spanish language",
    "portuguese language",
    "hindi language",
    "farsi",
    "persian language",
    "hebrew language",
    "turkish language",
    "proficiency in chinese",
    "proficiency in japanese",
    "proficiency in french",
    "proficiency in spanish",
    "proficiency in german",
    "proficiency in korean",
    "fluency in chinese",
    "fluency in japanese",
    "fluency in french",
    "fluency in spanish",
    "fluency in german",
    "fluency in korean",
    "native chinese",
    "native japanese",
    "native french",
    "native spanish",
    "working proficiency in chinese",
    "working proficiency in japanese",
    "working proficiency in french",
    "working proficiency in spanish",
]

# ─────────────────────────────────────────────
# Non-US locations
# ─────────────────────────────────────────────

NON_US_SIGNALS = [
    "canada", "united kingdom", " uk ", "london", "india", "australia",
    "germany", "france", "singapore", "ireland", "netherlands",
    "poland", "spain", "brazil", "mexico", "japan", "china",
    "portugal", "sweden", "denmark", "norway", "finland",
    "gurugram", "bangalore", "bengaluru", "toronto", "vancouver",
    "sydney", "melbourne", "berlin", "paris", "amsterdam", "dublin",
    "gothenburg", "stockholm", "stockholm", "ljubljana", "são paulo",
    "sao paulo", "zurich", "prague", "warsaw", "budapest",
    "remote - uk", "remote - india", "remote - canada",
    "remote - australia", "remote - germany", "hybrid - bangalore",
]

US_SIGNALS = [
    "united states", "usa", "u.s.", "remote", "us remote",
    "remote - usa", "remote - us", "remote us", "anywhere in the us",
    "new york", "san francisco", "los angeles", "chicago", "seattle",
    "austin", "boston", "dallas", "denver", "atlanta", "miami",
    "washington", "houston", "phoenix", "san diego", "portland",
    "minneapolis", "charlotte", "pittsburgh", "menlo park",
    "mountain view", "santa clara", "san mateo", "san jose",
    "kirkland", "bellevue", "reston", "herndon", "arlington",
    "plano", "irving", "fort worth", ", tx", ", ca", ", ny",
    ", wa", ", ma", ", va", ", ga", ", co", ", il", ", fl",
    ", nc", ", mn", ", pa", ", md", ", oh", ", or", ", ut",
    ", nj", ", az", "lux hub",  # coinbase internal tag for US remote
]

# ─────────────────────────────────────────────
# Experience -- required 3+ years
# ─────────────────────────────────────────────

EXPERIENCE_REQUIRED_PATTERNS = [
    r"\b([3-9]|\d{2,})\+?\s*years?\s+of\s+(?:relevant\s+|related\s+|professional\s+)?experience\s+(?:is\s+)?required",
    r"\brequires?\s+([3-9]|\d{2,})\+?\s*years?",
    r"\bminimum\s+(?:of\s+)?([3-9]|\d{2,})\+?\s*years?",
    r"\bat\s+least\s+([3-9]|\d{2,})\+?\s*years?",
    r"\b([3-9]|\d{2,})\+\s*years?\s+(?:of\s+)?(?:professional\s+|relevant\s+|work\s+)?experience",
    r"\b([3-9]|\d{2,})\s*\+?\s*years?\s+experience\s+(?:in|with)\b",
]

MAX_AGE_DAYS = 21


# ─────────────────────────────────────────────
# Filter functions
# ─────────────────────────────────────────────

def is_agency(text: str) -> bool:
    t = text.lower()
    return any(s in t for s in AGENCY_SIGNALS)


def requires_clearance(text: str) -> bool:
    t = text.lower()
    return any(s in t for s in CLEARANCE_SIGNALS)


def requires_foreign_language(text: str) -> bool:
    t = text.lower()
    return any(s in t for s in LANGUAGE_BLOCKLIST)


def title_blocked(title: str) -> bool:
    t = title.lower().strip()
    # Exact/prefix match for wrong-track roles
    for block in TITLE_BLOCKLIST_EXACT:
        if t == block or t.startswith(block):
            return True
    # Substring match for seniority
    for block in SENIORITY_BLOCKLIST:
        if block in t:
            return True
    return False


def is_relevant(title: str, jd: str) -> bool:
    combined = (title + " " + (jd or "")).lower()
    return any(s in combined for s in RELEVANCE_SIGNALS)


def is_us_location(location: str) -> bool:
    if not location or location.strip() == "":
        return True  # no location = benefit of the doubt
    loc = location.lower()
    # Non-US check first
    if any(s in loc for s in NON_US_SIGNALS):
        return False
    # US check
    if any(s in loc for s in US_SIGNALS):
        return True
    # Ambiguous -- let through
    return True


def requires_too_much_experience(jd: str) -> bool:
    if not jd:
        return False
    jd_lower = jd.lower()
    for pattern in EXPERIENCE_REQUIRED_PATTERNS:
        for match in re.finditer(pattern, jd_lower):
            end = match.end()
            context_after = jd_lower[end:end+80]
            if any(soft in context_after for soft in ["preferred", "nice to", "a plus", "bonus", "desired"]):
                continue
            return True
    return False


def is_recent(posted_date) -> bool:
    if posted_date is None:
        return True
    cutoff = date.today() - timedelta(days=MAX_AGE_DAYS)
    return posted_date >= cutoff


# ─────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────

def passes(listing: dict) -> tuple[bool, str]:
    title    = listing.get("title", "") or ""
    jd       = listing.get("raw_jd", "") or ""
    location = listing.get("location", "") or ""
    full_text = title + " " + jd

    if title_blocked(title):
        return False, "title/seniority blocklist"

    if is_agency(full_text):
        return False, "agency posting"

    if requires_clearance(full_text):
        return False, "requires clearance"

    if requires_foreign_language(full_text):
        return False, "requires language Asma doesn't speak"

    if not is_relevant(title, jd):
        return False, "not relevant"

    if not is_recent(listing.get("date_posted")):
        return False, "too old"

    if requires_too_much_experience(jd):
        return False, "requires 3+ years experience"

    if not is_us_location(location):
        return False, "non-US location"

    return True, ""
