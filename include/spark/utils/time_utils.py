from pyspark.sql import Column
from pyspark.sql import functions as F


def get_timestamp_col(timestamp: str) -> Column:
    return F.to_timestamp(F.lit(timestamp))
