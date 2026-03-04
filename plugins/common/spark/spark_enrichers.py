from abc import ABC, abstractmethod

from airflow.sdk import BaseHook


class BaseEnricher(ABC):
    @abstractmethod
    def build(self):
        raise NotImplementedError


class CatalogEnricher(BaseEnricher):
    def __init__(self, catalog_conn_id: str = "catalog_default", **kwargs):
        self.catalog_conn_id = catalog_conn_id
        super().__init__(**kwargs)

    def build(self):
        catalog_conn = BaseHook.get_connection(self.catalog_conn_id)
        extra = catalog_conn.extra_dejson
        return extra


class AwsEnricher(BaseEnricher):
    def __init__(self, aws_conn_id: str = "aws_default"):
        self.aws_conn_id = aws_conn_id

    def build(self):

        aws_conn = BaseHook.get_connection(self.aws_conn_id)
        endpoint = aws_conn.extra_dejson.get("endpoint_url")

        return {
            "spark.hadoop.fs.s3a.endpoint": endpoint
            or "s3.ap-northeast-2.amazonaws.com",
            "spark.hadoop.fs.s3a.access.key": aws_conn.login,
            "spark.hadoop.fs.s3a.secret.key": aws_conn.password,
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        }
