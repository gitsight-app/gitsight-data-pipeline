import functools
import logging

from pyspark.sql import SparkSession


def spark_session_manager(func):
    """
    Inject logger and manage Spark Session to clean up resources after job execution
    :param func: like func(spark_session, logger, **kwargs)
    :return:
    """
    logger = logging.getLogger(func.__name__)
    logging.basicConfig(level=logging.INFO)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            logger.info(f"[SETUP] Starting Spark session for job: {func.__name__}")
            return func(*args, **kwargs, logger=logger)
        except Exception as e:
            logger.error(f"ERROR in {func.__name__}: {e}")
            raise
        finally:
            active_spark = SparkSession.getActiveSession()
            if active_spark:
                logger.info(
                    f"[CLEANUP] Stopping active Spark session for job: {func.__name__}"
                )
                active_spark.stop()

    return wrapper
