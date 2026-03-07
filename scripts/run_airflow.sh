#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=== Start Airflow (All Components) ==="
docker compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile airflow \
  up -d --build
