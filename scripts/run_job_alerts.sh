#!/usr/bin/env bash
set -euo pipefail

ENV_NAME="${1:-dev}"
shift || true

PIPELINE_ARGS=()
if [ "${1:-}" = "--" ]; then
  shift
  PIPELINE_ARGS=("$@")
fi

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

case "$ENV_NAME" in
  dev)
    COMPOSE_FILE="docker-compose.yml"
    RUN_SVC="job-alerts-dev"
    NOTIFY_SVC="notify-dev"
    ;;
  prod)
    COMPOSE_FILE="docker-compose.prod.yml"
    RUN_SVC="job-alerts-prod"
    NOTIFY_SVC="notify-prod"
    ;;
  *)
    echo "usage: $0 [dev|prod]"
    exit 2
    ;;
esac

compose() {
  docker compose -f "$COMPOSE_FILE" "$@" 
}

exit_code=0
attempt=1
max_attempts=3

# In dev we usually don't want retries
if [ "$ENV_NAME" = "dev" ]; then
  max_attempts=1
fi

while true; do
  set +e
  if [ "${#PIPELINE_ARGS[@]}" -gt 0 ]; then
    compose run --rm "$RUN_SVC" "${PIPELINE_ARGS[@]}"
  else
    compose run --rm "$RUN_SVC"
  fi
  exit_code=$?
  set -e

  if [ "$exit_code" -ne 75 ] || [ "$attempt" -ge "$max_attempts" ]; then
    break
  fi

  sleep $((attempt * 20))
  attempt=$((attempt + 1))
done


HC_EXIT_CODE="$exit_code" HC_ATTEMPTS="$attempt" \
  compose run --rm "$NOTIFY_SVC" || true

exit "$exit_code"
