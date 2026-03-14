import pendulum
from pyspark.sql import functions as F


def get_ingested_at_between_condition(
    start_ts: pendulum.DateTime,
    end_ts: pendulum.DateTime,
):

    return (F.col("ingested_at") >= F.lit(start_ts.to_iso8601_string())) & (
        F.col("ingested_at") < F.lit(end_ts.to_iso8601_string())
    )
