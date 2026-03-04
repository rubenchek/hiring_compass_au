# syntax=docker/dockerfile:1

FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# (Optionnel) si tu as des deps qui nécessitent compilation, ajoute build-essential ici
# RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src

RUN python -m pip install --upgrade pip \
 && python -m pip wheel --no-deps --wheel-dir /wheels "."


FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Create non-root user
RUN useradd -m appuser

COPY --from=builder /wheels /wheels
RUN python -m pip install --no-cache-dir --upgrade pip \
 && python -m pip install --no-cache-dir /wheels/*.whl \
 && rm -rf /wheels

USER appuser

# OAuth bootstrap only (when token missing). Safe to always declare.
EXPOSE 8080
