import os

import yaml
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.sdk import DAG
from pendulum import datetime

SPARK_IMAGE = os.getenv("SPARK_IMAGE", "docker.io/bitnami/spark:3.5")


application_file = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": "spark-pi2-{{ ts_nodash | lower }}",
        "namespace": "spark-applications",
    },
    "spec": {
        "type": "Python",
        "mode": "cluster",
        "image": "{{ var.value.spark_image }}",
        "imagePullPolicy": "IfNotPresent",
        "mainApplicationFile": "{{ var.value.spark_job_prefix }}/include/spark/jobs/sample_spark_job.py",  # noqa: E501
        "sparkVersion": "3.5.1",
        "restartPolicy": {"type": "Never"},
        "sparkConf": {
            "spark.driver.extraJavaOptions": "-Duser.name=spark",
            "spark.executor.extraJavaOptions": "-Duser.name=spark",
            "spark.driverEnv.HADOOP_USER_NAME": "spark",
            "spark.executorEnv.HADOOP_USER_NAME": "spark",
        },
        "driver": {
            "image": "{{ var.value.spark_image }}",
            "cores": 1,
            "coreLimit": "1200m",
            "memory": "512m",
            "labels": {"version": "3.5.1"},
            "serviceAccount": "airflow-spark",
            "env": [
                {"name": "HADOOP_USER_NAME", "value": "spark"},
                {"name": "USER", "value": "spark"},
                {"name": "LOGNAME", "value": "spark"},
            ],
        },
        "executor": {
            "image": "{{ var.value.spark_image }}",
            "cores": 1,
            "instances": 1,
            "memory": "512m",
            "labels": {"version": "3.5.1"},
            "serviceAccount": "airflow-spark",
            "env": [
                {"name": "HADOOP_USER_NAME", "value": "spark"},
                {"name": "USER", "value": "spark"},
                {"name": "LOGNAME", "value": "spark"},
            ],
        },
    },
}


with DAG(
    dag_id="sample_k8s_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@once",
    catchup=False,
) as dag:
    sample_spark_task = SparkKubernetesOperator(
        task_id="sample_spark_task",
        application_file=yaml.safe_dump(application_file),
        namespace="spark-applications",
    )
