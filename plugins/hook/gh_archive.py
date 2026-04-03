import pendulum
import requests
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class GHArchiveHook(S3Hook):
    base_url = "https://data.gharchive.org"

    def __init__(
        self,
        aws_conn_id: str = "aws_default",
        bucket_name: str = "gitsight",
        timeout: int = 60,
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)
        self.bucket_name = bucket_name
        self.timeout = timeout
        self.session = requests.Session()

    def save_archive(self, date: pendulum.DateTime) -> str:
        """
        Download GHArchive hourly file (one-digit hour format)
        and upload directly to S3.
        """

        url = self._get_request_path(date)
        key = self._get_key(date)

        if self.check_for_key(key=key, bucket_name=self.bucket_name):
            self.log.info(f"File already exists in S3: s3://{self.bucket_name}/{key}")
            return f"s3a://{self.bucket_name}/{key}"

        self.log.info(f"Downloading {url}")

        response = self.session.get(url, stream=True, timeout=self.timeout)

        if response.status_code in (400, 404):
            self.log.info("status: %s", response.status_code)
            raise AirflowSkipException(f"File not found: {url}")

        response.raise_for_status()

        self.load_file_obj(
            file_obj=response.raw, key=key, bucket_name=self.bucket_name, replace=True
        )

        self.log.info(f"Uploaded to s3://{self.bucket_name}/{key}")

        return f"s3a://{self.bucket_name}/{key}"

    def _get_request_path(self, date: pendulum.DateTime) -> str:
        return (
            f"{self.base_url}/"
            f"{date.year}-{date.month:02d}-{date.day:02d}-{date.hour}.json.gz"
        )

    @staticmethod
    def _get_key(date: pendulum.DateTime) -> str:
        return (
            f"raw/gharchive/"
            f"{date.year}-{date.month:02d}-{date.day:02d}-{date.hour:02d}.json.gz"
        )
