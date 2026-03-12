from dataclasses import asdict, dataclass
from typing import Any


@dataclass(slots=True)
class SeekEnrichmentData:
    # identifiers
    advertiser_id: str | None = None
    role_id: str | None = None
    # classifications (from JobSearch in most cases)
    classification_ids: list[str] | None = None
    classification_labels: list[str] | None = None
    subclassification_ids: list[str] | None = None
    subclassification_labels: list[str] | None = None
    # role + arrangement
    seo_normalised_role_title: str | None = None
    work_types: str | None = None
    work_arrangement_types: list[str] | None = None
    # badges
    badges: list[str] | None = None
    description_raw: str | None = None
    teaser: str | None = None
    bullet_points: list[str] | None = None
    questionnaire_questions: list[str] | None = None
    skills: list[str] | None = None
    # lifecycle
    expires_at_utc: str | None = None
    insights_volume_label: str | None = None
    insights_count: int | None = None
    status: str | None = None

    def to_patch(self) -> dict[str, Any] | None:
        data = {k: v for k, v in asdict(self).items() if v is not None}
        return data or None
