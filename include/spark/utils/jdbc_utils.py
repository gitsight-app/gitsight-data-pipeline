from pydantic.dataclasses import dataclass
from pyspark.sql.conf import RuntimeConfig


@dataclass
class JdbcConfig:
    """
    Dataclass for JDBC connection properties.
    """

    url: str
    user: str
    password: str
    port: str
    driver: str = "org.postgresql.Driver"


def get_jdbc_config(conf: RuntimeConfig) -> JdbcConfig:
    """
    Get JDBC properties from Spark configuration.

    Args:
        conf (dict): Spark configuration dictionary.
    Returns:
        dict: JDBC properties including url, user, password, and driver.
    """

    return JdbcConfig(
        url=conf.get("spark.datasource.jdbc.url"),
        user=conf.get("spark.datasource.jdbc.user"),
        password=conf.get("spark.datasource.jdbc.password"),
        port=conf.get("spark.datasource.jdbc.port", "5432"),
        driver=conf.get("spark.datasource.jdbc.driver", "org.postgresql.Driver"),
    )
