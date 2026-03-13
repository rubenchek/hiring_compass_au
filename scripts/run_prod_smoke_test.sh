#!/usr/bin/env bash
set -euo pipefail

DB_PROD="./run/prod/data/local/state.sqlite"
DB_TEST="./run/prod/data/local/state.test.sqlite"

if [ ! -f "$DB_PROD" ]; then
  echo "Prod DB not found: $DB_PROD" >&2
  exit 2
fi

IMAGE_TAG="rubenchek/hiring-compass-au:smoke-test"

echo "[1/4] Build local prod image ($IMAGE_TAG)"
if docker image inspect "$IMAGE_TAG" >/dev/null 2>&1; then
  while true; do
    read -rp "Image exists. Type USE to reuse or REBUILD to rebuild: " choice
    if [ "$choice" = "USE" ]; then
      break
    fi
    if [ "$choice" = "REBUILD" ]; then
      docker build -t "$IMAGE_TAG" .
      break
    fi
  done
else
  docker build -t "$IMAGE_TAG" .
fi

echo "[2/4] Clone prod DB -> test DB"
cp "$DB_PROD" "$DB_TEST"

echo "[3/4] Run job-enrichment against test DB"
echo "      HC_DB_PATH=/app/data/local/state.test.sqlite"
HC_IMAGE_TAG=smoke-test \
docker compose -f docker-compose.yml -f docker-compose.prod.yml run --rm \
  -e HC_DB_PATH=/app/data/local/state.test.sqlite \
  job-enrichment --limit 10 --max-batches 1

echo "[4/4] Test DB ready: $DB_TEST"
echo "      Inspect now (notebook/sqlite/etc)."

echo ""
while true; do
  read -rp "Type DELETE to remove the test DB and finish: " confirm
  if [ "$confirm" = "DELETE" ]; then
    rm -f "$DB_TEST"
    echo "Test DB removed."
    if docker image inspect "$IMAGE_TAG" >/dev/null 2>&1; then
      while true; do
        read -rp "Type REMOVE to delete the smoke-test image or KEEP to retain it: " img_choice
        if [ "$img_choice" = "REMOVE" ]; then
          docker rmi "$IMAGE_TAG" || true
          echo "Smoke-test image removed."
          break
        fi
        if [ "$img_choice" = "KEEP" ]; then
          echo "Smoke-test image kept."
          break
        fi
      done
    fi
    break
  fi
  echo "Cleanup not confirmed. Test DB kept at: $DB_TEST"
done
