import sys
from pyspark.sql import SparkSession


def main(directory) -> None:
    """ Program that reads temperatures in streaming from a directory, finding those that are higher than a given
    threshold.

    It is assumed that an external entity is writing files in that directory, and every file contains a
    temperature value.

    :param directory: streaming directory
    """
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("StreamingFindHighTemperature") \
        .getOrCreate()

    # Create DataFrame representing the stream of input lines from connection to localhost:9999
    lines = spark \
        .readStream \
        .format("text") \
        .load(directory)

    lines.printSchema()

    # Select those received temperatures higher than 20 degrees
    values = lines.select("value").where("value > 20")

    # Start running the query that prints the output in the screen
    query = values \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .start()

    query.awaitTermination()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: spark-submit StreamingFindHighTemperature <file>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])
