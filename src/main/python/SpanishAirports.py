import sys
from pyspark import SparkConf, SparkContext

def main(file_name: str) -> None:
    spark_conf = SparkConf()
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    output = spark_context \
        .textFile(sys.argv[1]) \
        .map(lambda line: line.split(',')) \
        .filter(lambda line: "ES" in line[8]) \
        .map(lambda line: (line[2], 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda pair: pair[1], ascending=False) \
        .take(20)

    with open("output.txt",'w+') as outputFile:
        for (word, count) in output:
            print("%s: %i" % (word, count))
            outputFile.write("%s: %i\n" % (word, count))
if __name__ == "__main__":
    """
    Python program that uses Apache Spark to sum a list of numbers stored in files
    """

    if len(sys.argv) != 2:
        print("Usage: spark-submit AddNumbersFromFilesWithTime.py <file>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])