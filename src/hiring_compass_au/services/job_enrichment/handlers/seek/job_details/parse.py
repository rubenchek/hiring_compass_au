from __future__ import annotations

from typing import Any

from hiring_compass_au.domain.models import CompanyData, JobAdData
from hiring_compass_au.services.job_enrichment.handlers.seek.source_models import SeekEnrichmentData
from hiring_compass_au.services.job_enrichment.models import (
    FetchResult,
    ParseResult,
    TerminalEnrichmentError,
)


def _build_job_ad(job: dict[str, Any], target) -> JobAdData:
    job_ad = JobAdData(
        external_job_id=job.get("id") or getattr(target, "external_job_id", None),
        title=job.get("title"),
        company=(job.get("advertiser") or {}).get("name"),
        location_raw=(job.get("location") or {}).get("label"),
        salary_raw=(job.get("salary") or {}).get("label"),
        listing_date_utc=(job.get("listedAt") or {}).get("dateTimeUtc"),
    )
    job_ad.apply_normalizations()
    return job_ad


def _build_seek_enrichment(job: dict[str, Any], job_details: dict[str, Any]) -> SeekEnrichmentData:
    tracking_info = (job.get("tracking") or {}).get("classificationInfo") or {}
    classification_ids = (
        [str(tracking_info.get("classificationId"))]
        if isinstance(tracking_info, dict) and tracking_info.get("classificationId")
        else None
    )
    classification_labels = (
        [str(tracking_info.get("classification"))]
        if isinstance(tracking_info, dict) and tracking_info.get("classification")
        else None
    )
    subclassification_ids = (
        [str(tracking_info.get("subClassificationId"))]
        if isinstance(tracking_info, dict) and tracking_info.get("subClassificationId")
        else None
    )
    subclassification_labels = (
        [str(tracking_info.get("subClassification"))]
        if isinstance(tracking_info, dict) and tracking_info.get("subClassification")
        else None
    )

    work_arrangements = job_details.get("workArrangements") or {}
    arrangements = work_arrangements.get("arrangements") or []
    work_arrangement_types = None
    if isinstance(arrangements, list):
        types = [a.get("type") for a in arrangements if isinstance(a, dict) and a.get("type")]
        if types:
            work_arrangement_types = types

    badges = job_details.get("badges") or {}
    badges_list = badges.get("badges") or []
    badges_values = None
    if isinstance(badges_list, list):
        vals = [b.get("badge") for b in badges_list if isinstance(b, dict) and b.get("badge")]
        if vals:
            badges_values = vals

    insights = job_details.get("insights") or []
    insights_volume_label = None
    insights_count = None
    if isinstance(insights, list):
        for entry in insights:
            if not isinstance(entry, dict):
                continue
            if "volumeLabel" in entry or "count" in entry:
                insights_volume_label = entry.get("volumeLabel")
                insights_count = entry.get("count")
                break

    matched_skills = (job_details.get("personalised") or {}).get("matchedSkills") or {}
    skills = None
    if isinstance(matched_skills, dict):
        items = matched_skills.get("unmatched")
        if isinstance(items, list):
            skills = [
                i.get("displayLabel")
                for i in items
                if isinstance(i, dict) and i.get("displayLabel")
            ]
            if not skills:
                skills = None

    return SeekEnrichmentData(
        seo_normalised_role_title=(job_details.get("seoInfo") or {}).get("normalisedRoleTitle"),
        classification_ids=classification_ids,
        classification_labels=classification_labels,
        subclassification_ids=subclassification_ids,
        subclassification_labels=subclassification_labels,
        work_types=(job.get("workTypes") or {}).get("label"),
        work_arrangement_types=work_arrangement_types,
        badges=badges_values,
        description_raw=job.get("content"),
        teaser=job.get("abstract"),
        bullet_points=(job.get("products") or {}).get("bullets"),
        questionnaire_questions=((job.get("products") or {}).get("questionnaire") or {}).get(
            "questions"
        ),
        skills=skills,
        insights_volume_label=insights_volume_label,
        insights_count=insights_count,
        expires_at_utc=(job.get("expiresAt") or {}).get("dateTimeUtc"),
        advertiser_id=(job.get("advertiser") or {}).get("id"),
        status=job.get("status"),
    )


def _build_company(job: dict[str, Any], job_details: dict[str, Any]) -> CompanyData:
    gfj_company = (job_details.get("gfjInfo") or {}).get("company") or {}
    company_profile = job_details.get("companyProfile") or {}
    overview = company_profile.get("overview") or {}
    description = overview.get("description") or {}
    size = overview.get("size") or {}
    website = overview.get("website") or {}
    reviews = company_profile.get("reviewsSummary") or {}
    overall = reviews.get("overallRating") or {}
    number_reviews = overall.get("numberOfReviews") or {}

    return CompanyData(
        name=(job.get("advertiser") or {}).get("name"),
        industry=overview.get("industry"),
        description=description.get("paragraphs"),
        website_url=website.get("url"),
        size=size.get("description"),
        seek_rating_value=overall.get("value"),
        seek_review_count=number_reviews.get("value"),
        seek_company_id=(job.get("advertiser") or {}).get("id"),
        seek_company_url=gfj_company.get("url"),
    )


def parse_job_details(fetch_result: FetchResult, target) -> ParseResult:
    """
    Transform raw API payload into:
    - job_ad_patch: common fields to update in job_ads
    - source_patch: source-specific fields to persist elsewhere
    - company_patch: company-level fields to persist in company table
    """
    payload = fetch_result.payload
    if not isinstance(payload, dict):
        raise TerminalEnrichmentError(
            "invalid payload type",
            http_status=fetch_result.http_status,
            error_code="parse_invalid_payload",
        )

    if payload.get("errors"):
        raise TerminalEnrichmentError(
            "graphql errors in payload",
            http_status=fetch_result.http_status,
            error_code="parse_graphql_errors",
        )

    try:
        data = payload.get("data") or {}
        job_details = data.get("jobDetails") or {}
        job = job_details.get("job") or {}

        if not job_details or not job:
            raise TerminalEnrichmentError(
                "missing jobDetails/job",
                http_status=fetch_result.http_status,
                error_code="parse_missing_job",
            )

        job_ad = _build_job_ad(job, target)
        source = _build_seek_enrichment(job, job_details)
        company = _build_company(job, job_details)
    except TerminalEnrichmentError:
        raise
    except Exception as exc:
        raise TerminalEnrichmentError(
            f"parse failed: {exc}",
            http_status=fetch_result.http_status,
            error_code="parse_exception",
        ) from exc

    return ParseResult(
        job_ad_patch=job_ad,
        source_patch=source,
        company_patch=company,
    )
