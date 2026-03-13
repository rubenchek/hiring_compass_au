from __future__ import annotations

from collections.abc import Callable

from hiring_compass_au.services.job_enrichment.handlers.seek.job_details.handler import (
    SeekJobDetailsHandler,
)
from hiring_compass_au.services.job_enrichment.handlers.seek.session import (
    build_seek_session,
)

SESSION_REGISTRY: dict[str, Callable[..., object]] = {
    "seek": build_seek_session,
    # "linkedin": build_linkedin_session,
}


class HandlerNotFoundError(ValueError):
    pass


HandlerFactory = Callable[..., object]
HANDLER_REGISTRY: dict[tuple[str, str], HandlerFactory] = {
    ("seek", "jobDetails"): SeekJobDetailsHandler
}


def build_session(source: str, **kwargs):
    factory = SESSION_REGISTRY.get(source)
    if factory is None:
        return None
    return factory(**kwargs)


def dispatch_handler(*, source: str, enrich_type: str, **kwargs):
    """
    Resolve and instantiate the handler for a given (source, enrich_type).
    Raises ValueError if no handler is registered.
    """
    key = (source, enrich_type)
    factory = HANDLER_REGISTRY.get(key)
    if factory is None:
        known = ", ".join([f"{k[0]}:{k[1]}" for k in sorted(HANDLER_REGISTRY.keys())])
        raise HandlerNotFoundError(
            f"No enrichment handler for source={source} enrich_type={enrich_type}. "
            f"Registered: [{known}]"
        )
    try:
        return factory(**kwargs)
    except TypeError:
        return factory()
