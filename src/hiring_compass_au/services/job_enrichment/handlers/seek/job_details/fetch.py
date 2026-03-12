from __future__ import annotations

import uuid

import requests

from hiring_compass_au.services.job_enrichment.handlers.seek.session import (
    build_seek_session,
)
from hiring_compass_au.services.job_enrichment.models import FetchResult

JOB_DETAILS_QUERY = """
query jobDetails(
  $jobId: ID!
  $jobDetailsViewedCorrelationId: String!
  $sessionId: String!
  $zone: Zone!
  $locale: Locale!
  $visitorId: UUID!
  $isAuthenticated: Boolean!
  $enableJdvBadge: Boolean!
) {
  jobDetails(
    id: $jobId
    tracking: {
      channel: "WEB"
      jobDetailsViewedCorrelationId: $jobDetailsViewedCorrelationId
      sessionId: $sessionId
    }
  ) {
    job {
      tracking {
        classificationInfo {
          classificationId
          classification
          subClassificationId
          subClassification
        }
      }
      id
      title
      advertiser {
        id
        name(locale: $locale)
      }
      location {
        label(locale: $locale, type: LONG)
      }
      salary {
        label
      }
      listedAt {
        dateTimeUtc
      }
      expiresAt {
        dateTimeUtc
      }
      workTypes {
        label(locale: $locale)
      }
      abstract
      content(platform: WEB)
      products {
        bullets
        questionnaire {
          questions
        }
      }
      status
    }
    personalised {
      matchedSkills {
        unmatched {
          displayLabel(locale: $locale)
        }
      }
    }
    badges(visitorId: $visitorId, platform: WEB, locale: $locale)
      @include(if: $enableJdvBadge) {
      badges {
        badge
      }
    }
    insights @include(if: $isAuthenticated) {
      ... on ApplicantCount {
        volumeLabel(locale: $locale)
        count
      }
    }
    workArrangements(visitorId: $visitorId, channel: "JDV", platform: WEB) {
      arrangements {
        type
      }
    }
    seoInfo {
      normalisedRoleTitle
    }
    gfjInfo {
      company {
        url(locale: $locale, zone: $zone)
      }
    }
    companyProfile(zone: $zone) {
      overview {
        description {
          paragraphs
        }
        industry
        size {
          description
        }
        website {
          url
        }
      }
      reviewsSummary {
        overallRating {
          value
          numberOfReviews {
            value
          }
        }
      }
    }
  }
}
""".strip()


def fetch_job_details(
    target,
    *,
    session: requests.Session | None = None,
    timeout_s: float = 15.0,
    locale: str = "en-AU",
    zone: str = "anz-1",
    is_authenticated: bool = True,
    enable_jdv_badge: bool = True,
) -> FetchResult:
    """
    Call SEEK GraphQL API for jobDetails and return raw response (headers + payload).
    """
    job_id = getattr(target, "external_job_id", None)
    if not job_id:
        raise ValueError("Seek jobDetails fetch requires target.external_job_id")

    if session is None:
        session = build_seek_session()

    session_id = getattr(session, "seek_session_id", None) or str(uuid.uuid4())
    visitor_id = getattr(session, "seek_visitor_id", None) or str(uuid.uuid4())

    payload = {
        "operationName": "jobDetails",
        "variables": {
            "jobId": str(job_id),
            "jobDetailsViewedCorrelationId": str(uuid.uuid4()),
            "sessionId": session_id,
            "zone": zone,
            "locale": locale,
            "visitorId": visitor_id,
            "isAuthenticated": is_authenticated,
            "enableJdvBadge": enable_jdv_badge,
        },
        "query": JOB_DETAILS_QUERY,
    }

    resp = session.post(
        "https://www.seek.com.au/graphql",
        json=payload,
        timeout=timeout_s,
    )

    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text}

    # Normalize GraphQL errors if present
    if isinstance(body, dict) and "errors" in body:
        body = {
            "errors": body.get("errors"),
            "data": body.get("data"),
        }

    return FetchResult(
        http_status=resp.status_code,
        headers=dict(resp.headers),
        payload=body,
    )
