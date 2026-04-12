from pyspark.sql.connect.session import SparkSession


def create_branch(
    spark: SparkSession, *, branch_name, from_branch_name=None, checkout=False
):
    base_branch = from_branch_name or spark.conf.get("spark.sql.catalog.nessie.ref")

    spark.sql(f"""
        CREATE BRANCH IF NOT EXISTS `{branch_name}` IN nessie FROM {base_branch};
    """)

    if checkout:
        use_branch(spark, branch_name=branch_name)


def use_branch(spark: SparkSession, *, branch_name):
    spark.sql(f"""
        USE REFERENCE `{branch_name}` IN nessie;
    """)


def merge_branch(spark: SparkSession, *, from_branch_name=None, to_branch_name):
    from_branch_name = from_branch_name or spark.conf.get(
        "spark.sql.catalog.nessie.ref"
    )
    spark.sql(f"MERGE BRANCH `{from_branch_name}` INTO `{to_branch_name}` IN nessie")


def delete_branch(spark: SparkSession, *, branch_name):
    if branch_name in ["dev", "main"]:
        raise RuntimeError(f"Cannot delete branch '{branch_name}'")

    spark.sql(f"""
    DROP BRANCH IF EXISTS `{branch_name}` IN nessie;
    """)


def current_branch(spark: SparkSession):
    return spark.sql("""
        SHOW REFERENCE In nessie
        """).collect()[0][1]
