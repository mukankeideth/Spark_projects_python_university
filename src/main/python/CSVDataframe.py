from pyspark.sql import SparkSession


def main() -> None:
    spark_session = SparkSession \
        .builder \
        .getOrCreate()

    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    data_frame = spark_session\
        .read\
        .format("csv")\
        .options(inferschema = "true", header = "true")\
        .load("./test,csv")

    data_frame.printSchema()
    data_frame.show()

if __name__ == "__main__":
    main()