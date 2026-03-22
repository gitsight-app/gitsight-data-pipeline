IMAGE_VERSION = v1

-include .env.local



export TAG
export AF_IMAGE_NAME
export AF_VERSION
export SPARK_IMAGE_NAME
export SPARK_VERSION

export FULL_AIRFLOW_TAG = $(AF_IMAGE_NAME):$(AF_VERSION)-$(TAG)
export FULL_SPARK_TAG = $(SPARK_IMAGE_NAME):spark-$(SPARK_VERSION)-$(TAG)

.DEFAULT_GOAL := help

.PHONY: help build push deploy logs clean setup-cluster deploy-local

build-airflow:
	@echo "Building Airflow image..."
	@sh scripts/build-airflow.sh ${FULL_AIRFLOW_TAG}

build-spark:
	@echo "Building Spark image..."
	@sh scripts/build-spark.sh ${FULL_SPARK_TAG}


setup-cluster:
	@echo "Building Airflow and Spark images..."
	@make build-airflow
	@make build-spark
	@echo "Setting up local Kubernetes cluster with kind..."
	@sh scripts/setup_local_cluster.sh

deploy-local: setup-cluster
	@echo "Deploying to local cluster with Skaffold..."
	@skaffold dev --port-forward
