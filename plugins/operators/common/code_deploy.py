import os
import tempfile
import zipfile

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class CodeDeployOperator(BaseOperator):
    """
    Deploy Spark Utils Code for SparkSubmit (py_files={})
    under /include

    if write/rewrite code under include/*,
    have to pass
    """

    def __init__(
        self,
        *,
        s3_bucket: str,
        s3_key: str,
        folder_path: str = "/opt/airflow/include",
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.folder_path = folder_path
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        if not os.path.exists(self.folder_path):
            raise FileNotFoundError(f"Source not found: {self.folder_path}")

        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_file:
            temp_zip_path = tmp_file.name

        self.log.info(f"Created temporary file at: {temp_zip_path}")

        try:
            with zipfile.ZipFile(temp_zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                for root, _dirs, files in os.walk(self.folder_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(
                            file_path, os.path.dirname(self.folder_path)
                        )
                        zipf.write(file_path, arcname)

            self.log.info(f"Uploading to S3: s3a://{self.s3_bucket}/{self.s3_key}")

            s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
            s3_hook.load_file(
                filename=temp_zip_path,
                key=self.s3_key,
                bucket_name=self.s3_bucket,
                replace=True,
            )

        finally:
            if os.path.exists(temp_zip_path):
                os.remove(temp_zip_path)
                self.log.info(f"Cleaned up temporary file: {temp_zip_path}")

        return f"s3a://{self.s3_bucket}/{self.s3_key}"
