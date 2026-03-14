import enum

from pydantic import dataclasses
from pyspark.sql import SparkSession


class RefType(enum.Enum):
    BRANCH = ""
    TAG = "TAG"
    HASH = "HASH"


@dataclasses.dataclass
class Ref:
    ref_name: str
    ref_type: RefType


def change_branch(spark: SparkSession, branch_name: str, catalog="nessie"):
    spark.sql(f"""
    USE REFERENCE {branch_name} IN {catalog}
    """)
    pass


def create_branch(
    spark: SparkSession, branch_name: str, from_ref: Ref, catalog="nessie"
):
    spark.sql(f"""
    CREATE BRANCH {branch_name} IN {catalog} FROM {from_ref.ref_type.value} {from_ref.ref_name}
    """)  # noqa: E501


def merge_branch(
    spark: SparkSession, source_branch: str, target_branch: str, catalog="nessie"
):
    spark.sql(f"""
    MERGE BRANCH {source_branch} IN {catalog} INTO {target_branch}
    """)  # noqa: E501)
