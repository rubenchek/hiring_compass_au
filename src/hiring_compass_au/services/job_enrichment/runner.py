import logging
import random
import time

from hiring_compass_au.infra.storage.company_store import upsert_company_from_patch
from hiring_compass_au.infra.storage.enrichment_store import (
    compute_next_retry_at,
    get_pending_enrichment_counts,
    get_pending_enrichment_types,
    get_ready_enrichment_batch,
    mark_enrichment_failed,
    mark_enrichment_retry,
    mark_enrichment_success,
)
from hiring_compass_au.infra.storage.job_store import update_job_ad_from_patch
from hiring_compass_au.services.job_enrichment.dispatcher import (
    HandlerNotFoundError,
    build_session,
    dispatch_handler,
)
from hiring_compass_au.services.job_enrichment.models import (
    BatchSummary,
    EnrichmentTarget,
    MissingSessionError,
    RetryableEnrichmentError,
    TerminalEnrichmentError,
)

logger = logging.getLogger(__name__)


def run_enrichment_batch(
    conn,
    *,
    enrich_type: str,
    limit: int,
    sessions_cache: dict[str, object] | None = None,
    request_sleep_range: tuple[float, float] | None = None,
    throttle_state: dict[str, int | bool] | None = None,
    throttle_statuses: set[int] | None = None,
    throttle_sleep_range: tuple[float, float] | None = None,
    throttle_error_limit: int | None = None,
) -> BatchSummary:
    summary = BatchSummary()

    items = get_ready_enrichment_batch(conn=conn, limit=limit, enrich_type=enrich_type)
    if not items:
        return summary

    summary.selected += len(items)
    source = str(items[0]["source"])
    if sessions_cache is None:
        sessions_cache = {}
    if source not in sessions_cache:
        sessions_cache[source] = build_session(source)
        logger.info("session created for source=%s", source)
    session = sessions_cache[source]
    if session is None:
        logger.warning("session is None for source=%s", source)
    try:
        handler = dispatch_handler(
            source=source,
            enrich_type=enrich_type,
            session=session,
        )
    except HandlerNotFoundError as exc:
        error_code = "missing_handler"
        for item in items:
            mark_enrichment_failed(
                conn=conn,
                job_id=int(item["job_id"]),
                enrich_type=str(item["enrich_type"]),
                http_status=None,
                error_code=error_code,
                error_message=str(exc),
            )
        summary.skipped += len(items)
        return summary
    except MissingSessionError as exc:
        for item in items:
            mark_enrichment_failed(
                conn=conn,
                job_id=int(item["job_id"]),
                enrich_type=str(item["enrich_type"]),
                http_status=None,
                error_code="missing_session",
                error_message=str(exc),
            )
        summary.skipped += len(items)
        return summary

    for item in items:
        target = EnrichmentTarget(
            job_id=int(item["job_id"]),
            enrich_type=str(item["enrich_type"]),
            source=str(item["source"]),
            external_job_id=item["external_job_id"],
            canonical_url=item["canonical_url"],
        )

        result = handler.enrich(target)
        if not conn.in_transaction:
            conn.execute("BEGIN")

        try:
            parse_result = result.parse_result

            job_ad = parse_result.job_ad_patch
            company = parse_result.company_patch
            company_patch = company.to_patch() if company is not None else None
            company_id = None
            if company_patch:
                company_id = upsert_company_from_patch(conn, patch=company_patch)
            if company_id is not None and job_ad is not None:
                job_ad.company_id = company_id

            job_ad_patch = job_ad.to_patch() if job_ad is not None else None
            if job_ad_patch:
                update_job_ad_from_patch(
                    conn=conn,
                    job_id=target.job_id,
                    patch=job_ad_patch,
                )

            source = parse_result.source_patch
            source_patch = source.to_patch() if source is not None else None
            if source_patch:
                handler.persist_source_patch(
                    conn=conn,
                    job_id=target.job_id,
                    patch=source_patch,
                )
            post_persist = getattr(handler, "post_persist", None)
            if callable(post_persist):
                extra_success = post_persist(conn=conn, target=target, result=result)
                if isinstance(extra_success, int) and extra_success > 0:
                    summary.success += extra_success

            mark_enrichment_success(
                conn=conn,
                job_id=target.job_id,
                enrich_type=target.enrich_type,
                http_status=result.fetch_result.http_status,
            )

            conn.execute("COMMIT")
            summary.success += 1
            if request_sleep_range is not None:
                time.sleep(random.uniform(*request_sleep_range))

        except RetryableEnrichmentError as exc:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            _apply_throttle(
                exc.http_status,
                throttle_state=throttle_state,
                throttle_statuses=throttle_statuses,
                throttle_sleep_range=throttle_sleep_range,
                throttle_error_limit=throttle_error_limit,
            )

            next_retry_at_utc = compute_next_retry_at(
                attempt_count=int(item["attempt_count"]),
            )

            mark_enrichment_retry(
                conn=conn,
                job_id=int(item["job_id"]),
                enrich_type=str(item["enrich_type"]),
                http_status=exc.http_status,
                error_code=exc.error_code,
                error_message=str(exc),
                next_retry_at_utc=next_retry_at_utc,
            )
            summary.retry += 1

        except TerminalEnrichmentError as exc:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            _apply_throttle(
                exc.http_status,
                throttle_state=throttle_state,
                throttle_statuses=throttle_statuses,
                throttle_sleep_range=throttle_sleep_range,
                throttle_error_limit=throttle_error_limit,
            )

            mark_enrichment_failed(
                conn=conn,
                job_id=int(item["job_id"]),
                enrich_type=str(item["enrich_type"]),
                http_status=exc.http_status,
                error_code=exc.error_code,
                error_message=str(exc),
            )
            summary.failed += 1

        except Exception as exc:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            mark_enrichment_failed(
                conn=conn,
                job_id=int(item["job_id"]),
                enrich_type=str(item["enrich_type"]),
                http_status=None,
                error_code="unexpected_runner_error",
                error_message=repr(exc),
            )
            summary.failed += 1

    return summary


def run_enrichment(
    conn,
    *,
    limit: int = 50,
    max_batches: int | None = None,
    request_sleep_range: tuple[float, float] | None = (0.4, 1.2),
    batch_sleep_range: tuple[float, float] | None = (3.0, 10.0),
    batch_size_range: tuple[int, int] | None = (25, 50),
    throttle_statuses: set[int] | None = None,
    throttle_sleep_range: tuple[float, float] | None = (30.0, 90.0),
    throttle_error_limit: int | None = 3,
    log_every_batches: int = 2,
) -> BatchSummary:
    """
    Run successive enrichment batches until no work remains or max_batches reached.
    """
    total = BatchSummary()
    batches = 0

    sessions_cache: dict[str, object] = {}
    throttle_state: dict[str, int | bool] = {"count": 0, "stop": False}
    if throttle_statuses is None:
        throttle_statuses = {403, 429}

    counts = get_pending_enrichment_counts(conn)
    total_pending = sum(counts.values())
    if total_pending > 0:
        logger.info(
            "enrichment start: pending=%d by_type=%s",
            total_pending,
            counts,
        )

    while True:
        if max_batches is not None and batches >= max_batches:
            break
        pending = get_pending_enrichment_types(conn)
        if not pending:
            break
        for enrich_type in pending:
            batch_limit = limit
            if batch_size_range is not None:
                batch_limit = random.randint(*batch_size_range)
                if limit is not None:
                    batch_limit = min(batch_limit, limit)
            summary = run_enrichment_batch(
                conn,
                enrich_type=enrich_type,
                limit=batch_limit,
                sessions_cache=sessions_cache,
                request_sleep_range=request_sleep_range,
                throttle_state=throttle_state,
                throttle_statuses=throttle_statuses,
                throttle_sleep_range=throttle_sleep_range,
                throttle_error_limit=throttle_error_limit,
            )
            if summary.selected == 0:
                continue
            total.selected += summary.selected
            total.success += summary.success
            total.retry += summary.retry
            total.failed += summary.failed
            total.skipped += summary.skipped
            batches += 1
            if log_every_batches and batches % log_every_batches == 0:
                logger.info(
                    "enrichment progress: batches=%d selected=%d ok=%d retry=%d failed=%d",
                    batches,
                    total.selected,
                    total.success,
                    total.retry,
                    total.failed,
                )
            if batch_sleep_range is not None:
                time.sleep(random.uniform(*batch_sleep_range))
            if max_batches is not None and batches >= max_batches:
                break
            if throttle_state.get("stop"):
                logger.warning(
                    "enrichment stopped: too many throttle errors (count=%s)",
                    throttle_state.get("count"),
                )
                return total

    return total


def _apply_throttle(
    http_status: int | None,
    *,
    throttle_state: dict[str, int | bool] | None,
    throttle_statuses: set[int] | None,
    throttle_sleep_range: tuple[float, float] | None,
    throttle_error_limit: int | None,
) -> None:
    if (
        http_status is None
        or throttle_state is None
        or throttle_statuses is None
        or http_status not in throttle_statuses
    ):
        return
    throttle_state["count"] = int(throttle_state.get("count", 0)) + 1
    if throttle_sleep_range is not None:
        time.sleep(random.uniform(*throttle_sleep_range))
    if throttle_error_limit is not None and throttle_state["count"] >= throttle_error_limit:
        throttle_state["stop"] = True
