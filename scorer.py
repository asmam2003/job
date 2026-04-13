"""
LLM scoring layer.
Calls Anthropic API once per unscored listing and writes structured output back to DB.
"""

import anthropic
import json
import logging
import os
import time

log = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# ─────────────────────────────────────────────
# Resume track summaries -- update these as your resumes evolve
# ─────────────────────────────────────────────

TRACKS = {
    "Threat Intel": """
        OSINT investigations, intelligence reporting, TTP mapping, MITRE ATT&CK,
        MENA regional expertise, Arabic-language source analysis, Recorded Future,
        adversary pivot analysis, Storm-1295 PhaaS briefing, Telegram underground research.
        Estee Lauder: domain typosquatting investigation, WHOIS/registrar/hosting correlation.
        State Department: bilingual dataset of 200+ sources, PRC activity in MENA.
    """,
    "Fraud/T&S": """
        Criminal facilitation network mapping, behavioral threshold design,
        coordinated abuse detection, Hive SQL at scale, enforcement signal generation,
        cross-functional coordination with legal and policy teams, Storm-1295 project.
        TikTok: human smuggling detection, user-level aggregation thresholds,
        distinguishing coordinated fraud from benign behavior.
    """,
    "Detection Engineering": """
        Python automation, Hive SQL detection pipelines, feature engineering,
        false positive reduction (300+ FP eliminated at Estee Lauder),
        behavioral signal design, alert triage and enrichment.
        Detection logic built at TikTok for large-scale behavioral datasets.
    """,
    "Incident Ops": """
        Incident response workflows, NIST framework, alert triage, stakeholder communication,
        escalation procedures, domain abuse incident handling, ITIL-adjacent process work.
        Estee Lauder: end-to-end incident lifecycle, coordination with legal/security/engineering.
        Mastercard: tech risk and compliance, process documentation.
    """,
}

SYSTEM_PROMPT = """
You are a job fit scoring assistant. Given a job description and a candidate's resume tracks,
output ONLY a valid JSON object with no preamble, no markdown, no explanation.

JSON format:
{
  "best_track": "<one of: Threat Intel | Fraud/T&S | Detection Engineering | Incident Ops>",
  "fit_score": <integer 1-10>,
  "gaps": ["<gap 1>", "<gap 2>", "<gap 3>"],
  "jd_phrases": ["<phrase from JD not covered by resume 1>", "<phrase 2>", "<phrase 3>"]
}

Scoring guide:
9-10: Strong match, apply immediately with minimal edits
7-8: Good match, apply with targeted resume tweaks
5-6: Partial match, apply if volume allows
1-4: Poor match, skip unless desperate

gaps: specific skills or experience the JD asks for that the candidate lacks
jd_phrases: exact short phrases from the JD the resume does not address
Keep both lists to 3 items max. Be blunt and specific.
"""


def score_listing(title: str, company: str, raw_jd: str) -> dict | None:
    """
    Returns scoring dict or None if the API call fails.
    """
    tracks_text = "\n\n".join(
        f"## {name}\n{desc.strip()}" for name, desc in TRACKS.items()
    )

    user_message = f"""
JOB TITLE: {title}
COMPANY: {company}

JOB DESCRIPTION:
{raw_jd[:4000]}  

CANDIDATE RESUME TRACKS:
{tracks_text}

Score this listing.
"""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )
        raw = response.content[0].text.strip()
        # Strip any accidental markdown fences
        raw = raw.replace("```json", "").replace("```", "").strip()
        return json.loads(raw)
    except Exception as e:
        log.warning(f"Scoring failed for {company} - {title}: {e}")
        return None


def score_unscored(session) -> int:
    """
    Score all listings in DB that haven't been scored yet.
    Returns count of listings scored this run.
    """
    from models import Listing

    unscored = (
        session.query(Listing)
        .filter(Listing.scored == False, Listing.dismissed == False)
        .all()
    )

    count = 0
    for listing in unscored:
        if not listing.raw_jd:
            listing.scored = True  # nothing to score
            continue

        result = score_listing(listing.title, listing.company, listing.raw_jd)
        if result:
            listing.best_track = result.get("best_track")
            listing.fit_score  = result.get("fit_score")
            listing.gaps       = result.get("gaps", [])
            listing.jd_phrases = result.get("jd_phrases", [])

        listing.scored = True
        count += 1
        session.commit()
        time.sleep(0.3)  # gentle rate limiting

    log.info(f"Scored {count} listings this run")
    return count
