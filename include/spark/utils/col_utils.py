from typing import Tuple

from pyspark.sql import Column, DataFrame


def w_cols(df: DataFrame, *cols: Tuple[str, Column]) -> DataFrame:
    """
    Create Columns by cols arguments
    which have (column name, Spark Column expression) format.

    Example:
    w_cols(df, ("ingested_date", F.to_date("ingested_at")), ("ingested_hour", F.hour("ingested_at")))

    return the DataFrame with new columns "ingested_date" and "ingested_hour" created by the expressions.

    :param df:  DataFrame
    :param cols: list of tuple(column name, Spark Column expression)
    :return: DataFrame with new columns created by cols arguments
    """  # noqa: E501
    for col_name, col_expr in cols:
        df = df.withColumn(col_name, col_expr)

    return df
