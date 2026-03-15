import uuid

from operators.common.base_spark import BaseSparkOperator
from operators.spark.base.lake_oltp import CommonLakeOltpSparkOperator


class LakeToOLTPStagingOperator(CommonLakeOltpSparkOperator):
    template_fields = (
        *BaseSparkOperator.template_fields,
        "staging_table_name",
    )

    def __init__(
        self,
        *,
        oltp_conn_id: str = "postgres_default",
        aws_conn_id: str = "aws_default",
        catalog_conn_id: str = "catalog_default",
        staging_table_name: str,
        application_args,
        **kwargs,
    ):
        self.staging_table_name = staging_table_name

        super().__init__(
            application_args=application_args,
            aws_conn_id=aws_conn_id,
            catalog_conn_id=catalog_conn_id,
            jdbc_conn_id=oltp_conn_id,
            **kwargs,
        )

    def execute(self, context) -> str:
        self.application_args = self.application_args or []
        table_subfix = uuid.uuid4().hex

        target_table_name = f"{self.staging_table_name}_{table_subfix}"

        self.application_args.extend(
            [
                "--target_table_name",
                target_table_name,
            ]
        )

        super().execute(context)

        return target_table_name


class LakeToOLTPStagingDailyOperator(CommonLakeOltpSparkOperator):
    template_fields = (
        *BaseSparkOperator.template_fields,
        "staging_table_name",
        "source_table_name",
        "date_condition_col_name",
        "target_date",
    )

    def __init__(
        self,
        *,
        oltp_conn_id: str = "postgres_default",
        aws_conn_id: str = "aws_default",
        catalog_conn_id: str = "catalog_default",
        application="/opt/airflow/include/spark/jobs/load_to_oltp_staging_daily_job.py",
        staging_table_name: str,
        source_table_name: str,
        date_condition_col_name: str,
        target_date: str = "{{ ds }}",
        application_args=None,
        **kwargs,
    ):
        self.staging_table_name = staging_table_name
        self.source_table_name = source_table_name
        self.date_condition_col_name = date_condition_col_name
        self.target_date = target_date

        super().__init__(
            application=application,
            application_args=application_args,
            aws_conn_id=aws_conn_id,
            catalog_conn_id=catalog_conn_id,
            jdbc_conn_id=oltp_conn_id,
            **kwargs,
        )

    def execute(self, context) -> str:
        self.application_args = self.application_args or []
        table_subfix = uuid.uuid4().hex

        target_table_name = f"{self.staging_table_name}_{table_subfix}"

        self.application_args.extend(
            [
                "--target_table_name",
                target_table_name,
                "--target_date",
                self.target_date,
                "--source_table_name",
                self.source_table_name,
                "--date_condition_col_name",
                self.date_condition_col_name,
            ]
        )

        super().execute(context)

        return target_table_name
