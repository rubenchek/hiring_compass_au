from bs4 import BeautifulSoup
from collections.abc import Iterator
import re
import hashlib


# ---- Constants / patterns ----

SEEK_TRACKING_PREFIX = "email.s.seek.com.au/uni/ss/c/"
TITLE_STYLE_MARKER = ("color:#2e3849", "font-size:16px","font-weight:700")
COMPANY_STYLE_MARKER = ("color:#5a6881", "font-size:14px", "font-weight:400")   

POSTED_ON_RE = re.compile(r"^Posted on \d{1,2} [A-Za-z]{3,9} \d{4}$")

CTA_TITLE_RE = re.compile(r"\b(view|apply|details|see|open)\b", re.I)
STATE_RE = re.compile(r"\b(NSW|VIC|QLD|SA|WA|TAS|ACT|NT)\b")

STATE_SET = {"NSW","VIC","QLD","SA","WA","TAS","ACT","NT"}
LOC_RE = re.compile(
    r"^(?:(?P<suburb>.+?),\s*)?(?P<city>.+?)\s+(?P<state>NSW|VIC|QLD|SA|WA|TAS|ACT|NT)$"
    )

MONEY_RE = re.compile(r"\$[\d,]+")
MONEY_NUM_RE = re.compile(r"\$\s*([0-9]+(?:,[0-9]{3})*(?:\.[0-9]+)?)")

SALARY_HINT_RE = re.compile(
    r"\b(salary|super|package|per\s+year|p\.a\.|bonus|incentive|discount)\b", re.I,
    )

RATE_YEAR_RE = re.compile(r"(per\s+year|per\s+annum|p\.?\s*a\.?|pa)\b", re.I)
RATE_DAY_RE  = re.compile(r"(per\s+day|p\.?\s*d\.?|pd)\b", re.I)
RATE_HOUR_RE = re.compile(r"(per\s+hour|p\.?\s*h\.?|ph|hourly)\b", re.I)


# ---- Small utils ----

def _style_lower(tag) -> str:
    return (tag.get("style") or "").lower()


def _norm_space(s: str) -> str:
    return " ".join(s.split())


def is_noise_line(s: str) -> bool:
    s = s.strip()
    return bool(POSTED_ON_RE.match(s)) or s.lower().startswith("posted on ")


def norm(s: str | None) -> str:
    return _norm_space((s or "").lower())

# ---- Anchor selection ----

def is_seek_job_anchor(a) -> bool:
    """Heuristic filter for SEEK job-card anchors.

    Returns True if the <a> tag looks like a SEEK tracking link AND contains
    enough short <div> text blocks to resemble a job card (vs. header/footer links).
    """
    
    href = a.get("href", "")
    if SEEK_TRACKING_PREFIX not in href:
        return False
    
    div_texts = [d.get_text(" ", strip=True) for d in a.find_all("div")]
    div_texts = [t for t in div_texts if t]
    return len(div_texts) >= 3


def collect_candidate_texts(a, max_len: int = 120):
    """Collect short text snippets inside a job-card anchor.

    Returns a list of (div_tag, text) extracted from <div> elements within the anchor.
    Long blocks are ignored to reduce noise from email boilerplate.
    """
    cands = []
    for d in a.find_all("div"):
        txt = d.get_text(" ", strip=True)
        if not txt:
            continue
        if len(txt) > max_len:
            continue
        cands.append((d, txt))
    return cands


# ---- Field extraction ----

def extract_title(cands):
    """Extract the job title from candidate snippets using inline style markers.

    Looks for a <div> whose style contains the configured TITLE_STYLE_MARKER.
    Returns None if no suitable title is found.
    """
    for d, txt in cands:
        style = _style_lower(d)
        if all(m in style for m in TITLE_STYLE_MARKER):
            if is_noise_line(txt):
                continue
            return txt
    return None


def extract_company(cands):
    """Extract the company name from candidate snippets using inline style markers.

    Looks for a <div> whose style contains the configured COMPANY_STYLE_MARKER.
    Returns None if no suitable company is found.
    """
    for d, txt in cands:
        style = _style_lower(d)
        if all(m in style for m in COMPANY_STYLE_MARKER):
            if is_noise_line(txt):
                continue
            return txt
    return None
    

def best_location(texts, title=None, company=None):
    """Pick the most likely location line among candidate texts.

    Filters lines containing an Australian state code, then selects the one with the
    lowest penalty (shorter, fewer words, not containing title/company, not salary-like).
    Returns None if no location-looking line is found.
    """
    locs = [t for t in texts if STATE_RE.search(t)]
    if not locs:
        return None

    def penalty(t: str) -> int:
        p = 0
        p += len(t)                 # longer is worse
        p += 5 * len(t.split())     # more words is worse
        if title and title in t:
            p += 500
        if company and company in t:
            p += 500
        if MONEY_RE.search(t):      # likely salary, not pure location
            p += 500
        return p

    return min(locs, key=penalty)


def extract_location(texts, title=None, company=None):
    """Parse a location string into structured fields.

    Uses best_location() to select a candidate, then attempts to parse
    'Suburb, City STATE' or 'City STATE'. Falls back to state-only extraction when needed.

    Returns a dict with keys: suburb, city, state, location_raw (all may be None).
    """
    location_text = best_location(texts, title, company)
    
    result = {"suburb": None, "city": None, "state": None, "location_raw": None}
    
    if not location_text:
        return result

    s = _norm_space(location_text)
    
    m = LOC_RE.match(s)
    if not m:
        parts = s.split()
        state = parts[-1] if parts and parts[-1] in STATE_SET else None
        result.update({"state": state, "location_raw": s})
        return result
    
    result.update(m.groupdict())
    result["location_raw"] = s
    
    return result


def best_salary(texts, location_raw=None):
    """Pick the most likely salary/compensation line among candidate texts.

    Prefers lines containing explicit dollar amounts; otherwise falls back to lines
    matching salary-related keywords. Optionally excludes the location line.
    Returns None if no salary-looking line is found.
    """
    money_lines = [t for t in texts if MONEY_RE.search(t)]
    if money_lines:
        return min(money_lines, key=len)

    hinted = [t for t in texts if SALARY_HINT_RE.search(t)]
    if not hinted:
        return None

    if location_raw:
        hinted = [t for t in hinted if t != location_raw]
    return min(hinted, key=len) if hinted else None


def detect_rate_type(s: str):
    """Infer salary period from text (hour/day/year) using simple keyword patterns.

    Returns one of: 'hour', 'day', 'year', or None if no period is detected.
    """   
    if RATE_HOUR_RE.search(s):
        return "hour"
    if RATE_DAY_RE.search(s):
        return "day"
    if RATE_YEAR_RE.search(s):
        return "year"
    return None


def extract_salary(texts, location_raw=None):   
    """Extract salary range and period from candidate texts.

    Selects a salary line via best_salary(), normalizes whitespace, detects period,
    and parses 1 or 2 monetary values when present.

    Returns a dict with keys: salary_min, salary_max, salary_period, salary_raw.
    """ 
    result = {
        "salary_min": None, 
        "salary_max": None,
        "salary_period": None,
        "salary_raw": None
    }
    
    salary_text = best_salary(texts, location_raw)

    if not salary_text:
        return result
    
    result["salary_raw"] = _norm_space(salary_text)
    result["salary_period"] = detect_rate_type(result["salary_raw"])

    if "$" not in result["salary_raw"]:
        return result
    
    nums = [float(n.replace(",", "")) for n in MONEY_NUM_RE.findall(result["salary_raw"])]
    if not nums:
        return result
    
    if result["salary_period"] is None and max(nums) >= 1000:
        result["salary_period"] = "year"

    if len(nums) >= 2:
        lo, hi = nums[0], nums[1]
        result["salary_min"] = min(lo, hi)
        result["salary_max"] = max(lo, hi)
        return result

    result["salary_min"] =  nums[0]
    result["salary_max"] = nums[0]
    return result


def compute_hit_confidence(hit : dict) -> int:
    """
    Confidence that this extracted payload is a real job-card and reasonably parsed.
    Returns int in [0, 100].
    Must-haves (per your decision): title, company, location_raw.
    """
    title = (hit.get("title") or "").strip()
    company = (hit.get("company") or "").strip()
    location_raw = (hit.get("location_raw") or "").strip()

    state = hit.get("state")
    city = hit.get("city")
    suburb = hit.get("suburb")

    salary_raw = (hit.get("salary_raw") or "").strip()
    salary_min = hit.get("salary_min")
    salary_max = hit.get("salary_max")
    salary_period = hit.get("salary_period")
    
    
    # --- Must-have gating ---
    missing_must = 0
    if not title:
        missing_must += 1
    if not company:
        missing_must += 1
    if not location_raw:
        missing_must += 1
    
    if missing_must == 3:
        return 0
    if missing_must == 2:
        base = 20
    elif missing_must == 1:
        base = 45
    else:
        base = 70
    score = base
    
    # --- Bonuses (structure & richness) ---
    if state:
        score += 8
    if city:
        score += 6
    
    # Salary: raw is the strongest signal, then parsed numbers, then period
    if salary_raw:
        score += 10
    if salary_min is not None:
        score += 3
    if salary_max is not None and salary_max != salary_min:
        score += 2
    if salary_period:
        score += 2

    # Debug_lines count as a weak sanity signal
    debug_lines = hit.get("debug_lines") or []
    if isinstance(debug_lines, list) and debug_lines:
        norm = lambda s: " ".join(str(s).split()).strip().lower()
        unique_cnt = len({norm(x) for x in debug_lines if norm(x)})
        total_cnt = len(debug_lines)
        dup_ratio = 1.0 - (unique_cnt / total_cnt)
        
        if 3 <= unique_cnt <= 7:
            score += 3
        elif unique_cnt < 3:
            score -= 5
        elif unique_cnt > 12:
            score -= 5

        if dup_ratio > 0.5:
            score -= 2
            
    # --- Penalties (sanity checks) ---
    if len(title) < 4:
        score -= 25
    if CTA_TITLE_RE.search(title):
        score -= 15
    if location_raw and MONEY_RE.search(location_raw):
        score -= 15
    if isinstance(debug_lines, list):
        if  len(debug_lines) <= 3 or len(debug_lines) >= 10:
            score -= 10

    score = max(0, min(100, score))
    return int(score)


def extract_job_from_anchor(a) -> dict:
    """Extract a single job ad payload from a SEEK job-card anchor.

    Builds candidate snippets, extracts title/company/location/salary heuristically,
    and returns a dict suitable for downstream storage/ranking.
    """
    hit = {}
    href = a.get("href", "")
    cands = collect_candidate_texts(a)
    texts = [txt for _, txt in cands]
    texts = [t for t in texts if not is_noise_line(t)]
    
    title = extract_title(cands)
    company = extract_company(cands)
    
    location_dict = extract_location(texts, title=title, company=company)
    location_raw = location_dict["location_raw"]
    
    salary_dict = extract_salary(texts, location_raw=location_raw)
    
    fingerprint = None
    if title and company and location_raw:
        fingerprint = hashlib.sha1(
            f"{norm(title)}|{norm(company)}|{norm(location_raw)}".encode("utf-8")
        ).hexdigest()[:16]

    hit.update(
        {   
            "title": title,
            "company": company,
            **location_dict,
            **salary_dict,
            "out_url": href,
            "debug_lines": texts,
            "fingerprint": fingerprint,
        }
    )
    
    #TODO ajouter le hit_context
    
    hit["hit_confidence"] = compute_hit_confidence(hit)

    return hit 


def parse_seek_email(html) -> Iterator[dict]:
    """Parse a SEEK job alert email HTML and yield extracted job ads.

    Scans the document for SEEK job-card anchors and yields one dict per job card.
    """
    soup = BeautifulSoup(html, "html.parser")
    
    for a in soup.find_all("a", href=True):
        if is_seek_job_anchor(a): 
            yield extract_job_from_anchor(a)