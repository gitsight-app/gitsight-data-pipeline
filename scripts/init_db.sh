#!/bin/bash
set -e
cd "$(dirname "$0")"

SERVICE_NAME="airflow-init"


echo "=== Run Airflow Init (Migration & User Creation) ==="
docker compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile init \
  run --rm $SERVICE_NAME
