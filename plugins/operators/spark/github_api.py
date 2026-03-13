from airflow.sdk import BaseHook
from operators.spark.base.lake import CommonLakeSparkOperator


class FetchGithubAPISparkOperator(CommonLakeSparkOperator):
    template_fields = (*CommonLakeSparkOperator.template_fields, "github_api_conn_id")

    def __init__(
        self,
        *,
        github_api_conn_id="github_api",
        application,
        application_args=None,
        aws_conn_id="aws_default",
        catalog_conn_id="catalog_default",
        **kwargs,
    ):

        self.github_api_conn_id = github_api_conn_id
        super().__init__(
            application=application,
            application_args=application_args,
            aws_conn_id=aws_conn_id,
            catalog_conn_id=catalog_conn_id,
            **kwargs,
        )

    def execute(self, context) -> None:
        conn = BaseHook.get_connection(self.github_api_conn_id)

        self.application_args = self.application_args or []
        self.application_args.extend(
            [
                "--github_token",
                conn.password,
            ]
        )

        return super().execute(context)
