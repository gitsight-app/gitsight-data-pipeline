from operators.spark.base.lake_oltp import CommonLakeOltpSparkOperator


class LoadToOltpOperator(CommonLakeOltpSparkOperator):
    template_fields = (
        *CommonLakeOltpSparkOperator.template_fields,
        "staging_table_name",
    )

    def __init__(
        self,
        *,
        staging_table_name: str,
        application: str,
        data_interval_start="{{ data_interval_start }}",
        data_interval_end="{{ data_interval_end }}",
        aws_conn_id="aws_default",
        catalog_conn_id="catalog_default",
        jdbc_conn_id="postgres_default",
        **kwargs,
    ):
        self.staging_table_name = staging_table_name

        application_args = []

        application_args.extend(
            [
                "--staging_table_name",
                staging_table_name,
                "--data_interval_start",
                data_interval_start,
                "--data_interval_end",
                data_interval_end,
            ]
        )

        super().__init__(
            application=application,
            application_args=application_args,
            aws_conn_id=aws_conn_id,
            catalog_conn_id=catalog_conn_id,
            jdbc_conn_id=jdbc_conn_id,
            **kwargs,
        )

    def execute(self, context) -> str:
        super().execute(context)

        return self.staging_table_name
