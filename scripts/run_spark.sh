#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=== Start Spark (Master(1), Worker(1)) ==="
docker compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile spark \
  up -d --build
