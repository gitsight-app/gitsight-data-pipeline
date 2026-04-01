#!/bin/bash
set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

cd "$PROJECT_ROOT"

echo "=== Building Airflow Image: ${FULL_AIRFLOW_TAG:-default} ==="
docker build \
  . \
  -t "${FULL_AIRFLOW_TAG:-"gitsight/airflow:3.1.2-local"}" \
  -f ./docker/airflow/Dockerfile \

echo "=== Airflow Image Built Successfully: ${TAG} ==="
