from __future__ import annotations

import pytest
from pydantic import ValidationError

from hiring_compass_au.schemas import EmploymentType, JobAd, JobRole, Sector, Source


def _base_jobad_kwargs():
    return dict(
        source=Source.SEEK,
        source_id="12345",
        title="Data Engineer",
        location="Sydney",
        description="Some job description",
        url="https://www.seek.com.au/job/123",
    )

def test_jobad_minimal_is_valid():
    ad = JobAd(**_base_jobad_kwargs())
    assert ad.source == Source.SEEK
    assert ad.source_id == "12345"
    assert str(ad.url).startswith("https://")


def test_jobad_rejects_empty_title():
    with pytest.raises(ValidationError):
        JobAd(
            source=Source.SEEK,
            source_id="12345",
            title="",
            location="Sydney NSW",
            description="Some description",
            url="https://www.seek.com.au/job/12345",
        )

def test_jobad_parses_employment_type_enum():
    ad = JobAd(employment_type="contract/temp", **_base_jobad_kwargs())
    assert ad.employment_type == EmploymentType.CONTRACT_TEMP
    
    
def test_role_valid_enum():
    ad = JobAd(role=JobRole.DATA_ENGINEER, **_base_jobad_kwargs())
    assert ad.role == JobRole.DATA_ENGINEER


def test_role_valid_string_converted():
    ad = JobAd(role="data-engineer", **_base_jobad_kwargs())
    assert ad.role == JobRole.DATA_ENGINEER


def test_role_unknown_fallback_to_other():
    ad = JobAd(role="AI Wizard Ninja", **_base_jobad_kwargs())
    assert ad.role == JobRole.OTHER
    
def test_sector_unknown_fallback_to_other():
    ad = JobAd(sector="Space Mining", **_base_jobad_kwargs())
    assert ad.sector == Sector.OTHER
