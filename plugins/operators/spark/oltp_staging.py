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
