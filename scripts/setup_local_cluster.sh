#!/usr/bin/env bash

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

cd "$PROJECT_ROOT"



echo "=== Setting up Kubernetes Cluster ==="


echo "=== Creating Namespace: airflow, spark, spark-applications ==="

kubectl create namespace airflow --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace spark --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace spark-applications --dry-run=client -o yaml | kubectl apply -f -

echo "=== Adding Helm Repositories ==="

helm repo add apache-airflow https://airflow.apache.org || true
helm repo add spark-operator https://kubeflow.github.io/spark-operator || true
helm repo update

echo "=== Applying RBAC for Airflow to Spark ==="
kubectl apply -f k8s/airflow-spark-rbac.yaml
kubectl apply -f k8s/spark-driver-rbac.yaml

echo "Kubernetes setup complete."
