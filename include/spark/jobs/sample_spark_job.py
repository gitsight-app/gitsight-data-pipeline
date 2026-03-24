from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Spark Session 생성 (표준 방식)
    spark = SparkSession.builder.appName("Sample Spark").getOrCreate()

    print("=" * 30)
    print("Hello, Spark!")
    print(f"Spark version: {spark.version}")
    print("=" * 30)

    # 간단한 연산 테스트 (옵션)
    df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "label"])
    df.show()

    spark.stop()
