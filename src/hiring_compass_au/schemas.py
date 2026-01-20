from __future__ import annotations

from datetime import date, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, HttpUrl


class Source(StrEnum):
    SEEK = "seek"
    LINKEDIN = "linkedin"

class EmploymentType(StrEnum):
    FULL_TIME = "full-time"
    PART_TIME = "part-time"
    CONTRACT_TEMP  = "contract/temp"
    CASUAL_VACATION = "casual/vacation"
    
class RemoteOption(StrEnum):
    ON_SITE = "on-site"
    HYBRID = "hybrid"
    REMOTE = "remote"
    
class JobRole(StrEnum):
    DATA_SCIENTIST = "data-scientist"
    DATA_ENGINEER = "data-engineer"
    MLOPS = "mlops"
    ML_ENGINEER = "ml-engineer"
    ANALYTICS_ENGINEER = "analytics-engineer"
    DATA_ANALYST = "data-analyst"
    OTHER = "other"


class Sector(StrEnum): 
    FINANCE = "finance"
    ENVIRONMENT = "environment"
    HEALTH = "health"
    TECH = "tech"
    CONSULTING = "consulting"
    GOVERNMENT = "government"
    OTHER = "other"
    
class JobAd(BaseModel):
    # Provenance
    source: Source = Field(default=Source.SEEK)
    source_id: str = Field(min_length=1)
    
    # Core
    title: str = Field(min_length=1)
    company: str | None = None
    location: str = Field(min_length=1)
    description: str = Field(min_length=1)
    url: HttpUrl
    
    # Dates
    posted_at: date | None = None  # derived later from "posted X days ago"
    scraped_at: datetime = Field(default_factory=lambda: datetime.now(datetime.UTC))
    
    # Raw fields from source
    raw_salary: str | None = None
    employment_type: EmploymentType | None = None
    
    # Enrichment (LLM or rules): keep BOTH raw + normalized
    role_raw: str | None = None
    role: JobRole | None = None

    sector_raw: str | None = None
    sector: Sector | None = None

    # Debug: keep raw payload to reprocess
    raw: dict[str, Any] | None = None