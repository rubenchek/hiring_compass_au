from __future__ import annotations

import re

STATE_SET = {"NSW", "VIC", "QLD", "SA", "WA", "TAS", "ACT", "NT"}

LOC_RE = re.compile(
    r"^(?:(?P<suburb>.+?),\s*)?(?P<city>.+?)\s+(?P<state>NSW|VIC|QLD|SA|WA|TAS|ACT|NT)$"
)

MONEY_DOLLAR_RE = re.compile(r"\$\s*([0-9]+(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*([kK])?")
MONEY_K_RE = re.compile(r"(?<![\w$])([0-9]+(?:\.[0-9]+)?)\s*[kK](?!\w)")

RATE_YEAR_RE = re.compile(r"(per\s+year|per\s+annum|p\.?\s*a\.?|pa)\b", re.I)
RATE_MONTH_RE = re.compile(r"(per\s+month|p\.?\s*m\.?|pm|monthly)\b", re.I)
RATE_DAY_RE = re.compile(r"(per\s+day|p\.?\s*d\.?|pd|daily)\b", re.I)
RATE_HOUR_RE = re.compile(r"(per\s+hour|p\.?\s*h\.?|ph|hourly)\b", re.I)


def normalize_space(s: str | None) -> str:
    return " ".join((s or "").split())


def parse_location_raw(location_raw: str | None) -> dict:
    """Parse a location string into structured fields.

    Returns a dict with keys: suburb, city, state, location_raw (all may be None).
    """
    result = {"suburb": None, "city": None, "state": None, "location_raw": None}
    if not location_raw:
        return result

    s = normalize_space(location_raw)
    if not s:
        return result

    m = LOC_RE.match(s)
    if not m:
        parts = s.split()
        state = parts[-1] if parts and parts[-1] in STATE_SET else None
        result.update({"state": state, "location_raw": s})
        return result

    result.update(m.groupdict())
    result["location_raw"] = s
    return result


def parse_amounts(s: str) -> list[float]:
    s = s.strip()
    nums: list[float] = []

    # $ amounts (optionally with k/K)
    for n_str, k_flag in MONEY_DOLLAR_RE.findall(s):
        v = float(n_str.replace(",", ""))
        if k_flag and v <= 2000:
            v *= 1000.0
        nums.append(v)

    # k/K amounts without $ (avoid double-counting $125k which is already captured above)
    if "$" not in s:
        for n_str in MONEY_K_RE.findall(s):
            nums.append(float(n_str) * 1000.0)

    if not nums or len(nums) <= 1:
        return nums

    # sanity check
    lo = min(nums)
    hi = max(nums)

    if len(nums) == 2 and lo != 0 and hi / lo >= 500:
        index = nums.index(lo)
        nums[index] = lo * 1000

    return nums


def detect_rate_type(s: str):
    """Infer salary period from text (hour/day/month/year) using simple keyword patterns."""
    if RATE_HOUR_RE.search(s):
        return "hour"
    if RATE_DAY_RE.search(s):
        return "day"
    if RATE_MONTH_RE.search(s):
        return "month"
    if RATE_YEAR_RE.search(s):
        return "year"
    return None


def parse_salary_raw(salary_raw: str | None) -> dict:
    """Extract salary range and period from a raw salary string.

    Returns a dict with keys: salary_min, salary_max, salary_period, salary_raw.
    """
    result = {"salary_min": None, "salary_max": None, "salary_period": None, "salary_raw": None}
    if not salary_raw:
        return result

    raw = normalize_space(salary_raw)
    if not raw:
        return result

    result["salary_raw"] = raw
    result["salary_period"] = detect_rate_type(raw)

    nums = parse_amounts(raw)
    if not nums:
        return result

    if result["salary_period"] is None and max(nums) >= 1000:
        result["salary_period"] = "year"

    if len(nums) >= 2:
        lo, hi = nums[0], nums[1]
        result["salary_min"] = min(lo, hi)
        result["salary_max"] = max(lo, hi)
        return result

    result["salary_min"] = nums[0]
    result["salary_max"] = nums[0]
    return result
