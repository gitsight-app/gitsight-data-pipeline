#!/bin/bash
set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

cd "$PROJECT_ROOT"

echo "=== Building Spark Image: ${FULL_SPARK_TAG:-"gitsight/spark:3.5.1-local"} ==="
docker build \
  . \
  -t "${FULL_SPARK_TAG:-"gitsight/spark:3.5.1-local"}" \
  -f ./docker/spark/Dockerfile \

echo "=== Spark Image Built Successfully: ${FULL_IMAGE_TAG} ==="
