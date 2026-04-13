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
    "business analyst",
    "imagery analyst",
    "geospatial analyst",
    "signals intelligence",
    "cyberspace targeting",
    "targeting analyst",
    "intelligence operations specialist",
]

# Checked as substring anywhere in lowercased title
SENIORITY_BLOCKLIST = [
    "senior",
    " sr.",
    "sr ",        # catches "Sr OSINT" at start
    "sr.",
    "principal",
    "staff ",
    " lead",
    "lead,",      # catches "Lead, Cyber..."
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
    " iii",       # catches "Analyst III", "Engineer III"
    "journeyman",
    " expert",
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
    "security clearance",
    "active clearance",
    "clearance required",
    "clearance eligible",
    "clearance preferred",
    "sf-86",
    "sf86",
    "polygraph",
    "dod clearance",
    "government clearance",
    "federal clearance",
    "must be clearable",
    "public trust clearance",
    "public trust required",
]

# ─────────────────────────────────────────────
# Military/DoD contractor signals -- not relevant to track
# ─────────────────────────────────────────────

MILITARY_SIGNALS = [
    "department of defense",
    "dod ",
    "disa ",
    "uscybercom",
    "national training center",
    "fort irwin",
    "fort liberty",
    "fort meade",
    "peterson air force",
    "shaw afb",
    "afcent",
    "arcyber",
    "acert",
    "arng",
    "army national guard",
    "rotational training",
    "warfighter",
    "joint task force",
    "jtf ",
    "campaign plan",
    "operational planning",
    "military affiliated",
    "softcopy imagery",
    "geospatial intelligence",
    "imagery analysis",
    "satellite imagery",
    "pai targeting",
    "targeting analyst",
    "contingent upon contract award",
    "correctional facility",
    "cellular interdiction",
]

# ─────────────────────────────────────────────
# Language requirements
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
    "english, spanish",     # catches bilingual job titles/requirements
    "spanish, english",
    "english and spanish",
    "spanish and english",
    "latin american policy",  # proxy for Spanish requirement in Google roles
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
    "gothenburg", "stockholm", "ljubljana", "são paulo",
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
    ", nj", ", az", "lux hub",
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


def is_military_dod(text: str) -> bool:
    t = text.lower()
    return any(s in t for s in MILITARY_SIGNALS)


def requires_foreign_language(text: str) -> bool:
    t = text.lower()
    return any(s in t for s in LANGUAGE_BLOCKLIST)


def title_blocked(title: str) -> bool:
    t = title.lower().strip()
    for block in TITLE_BLOCKLIST_EXACT:
        if t == block or t.startswith(block):
            return True
    for block in SENIORITY_BLOCKLIST:
        if block in t:
            return True
    return False


def is_relevant(title: str, jd: str) -> bool:
    combined = (title + " " + (jd or "")).lower()
    return any(s in combined for s in RELEVANCE_SIGNALS)


def is_us_location(location: str) -> bool:
    if not location or location.strip() == "":
        return True
    loc = location.lower()
    if any(s in loc for s in NON_US_SIGNALS):
        return False
    if any(s in loc for s in US_SIGNALS):
        return True
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

    if is_military_dod(full_text):
        return False, "military/DoD contractor role"

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
