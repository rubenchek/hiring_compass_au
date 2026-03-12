from dataclasses import asdict, dataclass
from typing import Any

from hiring_compass_au.domain.normalizers.normalize_job_fields import (
    parse_location_raw,
    parse_salary_raw,
)


@dataclass(slots=True)
class JobAdData:
    source: str | None = None
    external_job_id: str | None = None
    fingerprint: str | None = None
    title: str | None = None
    company: str | None = None
    company_id: int | None = None
    suburb: str | None = None
    city: str | None = None
    state: str | None = None
    location_raw: str | None = None
    salary_min: int | None = None
    salary_max: int | None = None
    salary_period: str | None = None
    salary_raw: str | None = None
    description: str | None = None
    job_status: str | None = None
    canonical_url: str | None = None
    first_seen_at: str | None = None
    last_seen_at: str | None = None
    listing_date_utc: str | None = None

    def apply_location_normalization(self) -> None:
        """Populate suburb/city/state from location_raw if missing."""
        if not self.location_raw:
            return
        parsed = parse_location_raw(self.location_raw)
        if self.suburb is None:
            self.suburb = parsed.get("suburb")
        if self.city is None:
            self.city = parsed.get("city")
        if self.state is None:
            self.state = parsed.get("state")

    def apply_salary_normalization(self) -> None:
        """Populate salary_min/max/period from salary_raw if missing."""
        if not self.salary_raw:
            return
        parsed = parse_salary_raw(self.salary_raw)
        if self.salary_min is None:
            self.salary_min = parsed.get("salary_min")
        if self.salary_max is None:
            self.salary_max = parsed.get("salary_max")
        if self.salary_period is None:
            self.salary_period = parsed.get("salary_period")

    def apply_normalizations(self) -> None:
        """Apply all available normalizers (location + salary)."""
        self.apply_location_normalization()
        self.apply_salary_normalization()

    def to_patch(self) -> dict[str, Any] | None:
        data = {k: v for k, v in asdict(self).items() if v is not None}
        return data or None


@dataclass(slots=True)
class CompanyData:
    name: str | None = None
    industry: str | None = None
    description: list[str] | None = None
    size: str | None = None
    website_url: str | None = None
    seek_company_id: str | None = None
    seek_rating_value: float | None = None
    seek_review_count: int | None = None
    seek_company_url: str | None = None

    def to_patch(self) -> dict[str, Any] | None:
        data = {k: v for k, v in asdict(self).items() if v is not None}
        return data or None
