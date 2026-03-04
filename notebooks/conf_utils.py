import os

from dotenv import load_dotenv


def get_extra_conf():
    load_dotenv(".env")
    return {
        "spark.hadoop.fs.s3a.endpoint": os.environ.get("AWS_S3_ENDPOINT"),
        "spark.hadoop.fs.s3a.access.key": os.environ.get("AWS_ACCESS_KEY_ID"),
        "spark.hadoop.fs.s3a.secret.key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
        "spark.app.db.user": os.environ.get("DB_USER"),
        "spark.app.db.password": os.environ.get("DB_PASSWORD"),
        "spark.app.db.driver": "org.postgresql.Driver",
        "spark.app.kafka.bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
        "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",  # noqa: E501
        "spark.sql.catalog.nessie.uri": os.environ.get("NESSIE_CATALOG_URI"),
        "spark.sql.catalog.nessie.ref": os.environ.get("NESSIE_CATALOG_REF"),
        "spark.sql.catalog.nessie.warehouse": os.environ.get("NESSIE_WAREHOUSE"),
    }
