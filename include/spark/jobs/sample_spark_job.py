from pyspark.sql.connect.session import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Sample Spark").getOrCreate()

    print("Hello, Spark!")
    print("Spark version:", spark.version)
