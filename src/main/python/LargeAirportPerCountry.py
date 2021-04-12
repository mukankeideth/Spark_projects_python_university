import sys

from itertools import count

from pyspark import SparkConf, SparkContext, sql

def main(file_name: str,file_name2: str) -> None:
    spark_conf = SparkConf()
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    sqlContext = sql.SQLContext(spark_context)

    df_countycode_count_pair = sqlContext.read.csv(file_name, inferSchema = True, header = True).drop("name")
    df_countryname = sqlContext.read.csv(file_name2, inferSchema = True, header = True)

    df_full = df_countycode_count_pair.join(df_countryname, df_countycode_count_pair.iso_country==df_countryname.code)


    df_full.show(3)


    countycode_count_pair = df_full \
        .filter(df_full.type=="large_airport") \
        .rdd \
        .map(lambda line: (line.name, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda pair: pair[1], ascending=False) \
        .take(20)
        #.sortBy(lambda pair: pair[1], ascending=False) \
        #.take(20)


    for (word, count) in countycode_count_pair:
        print("%s: %i" % (word, count))

if __name__ == "__main__":
    """
    Python program that uses Apache Spark to sum a list of numbers stored in files
    """

    if len(sys.argv) < 2:
        print("Usage: spark-submit AddNumbersFromFilesWithTime.py <file>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1],sys.argv[2])