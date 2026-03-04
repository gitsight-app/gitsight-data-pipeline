from operators.spark.lake import CommonLakeSparkOperator


class ExtractMetaOperator(CommonLakeSparkOperator):
    """
    Extract meta information from source data and save to target location
    """

    template_fields = (
        *CommonLakeSparkOperator.template_fields,
        "data_interval_start",
        "data_interval_end",
    )

    def __init__(
        self,
        *,
        application: str,
        data_interval_start="{{ data_interval_start }}",
        data_interval_end="{{ data_interval_end }}",
        aws_conn_id="aws_default",
        catalog_conn_id="catalog_default",
        **kwargs,
    ):
        self.data_interval_start = data_interval_start
        self.data_interval_end = data_interval_end

        super().__init__(
            application=application,
            aws_conn_id=aws_conn_id,
            catalog_conn_id=catalog_conn_id,
            executor_memory="2g",
            **kwargs,
        )

    def execute(self, context) -> None:
        self.application_args = self.application_args or []

        self.application_args.extend(
            [
                "--data_interval_start",
                self.data_interval_start,
                "--data_interval_end",
                self.data_interval_end,
            ]
        )
        return super().execute(context)
