import sys

from itertools import count

from pyspark import SparkConf, SparkContext, sql
import pyspark.sql.types as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, BooleanType
from datetime import date
import dateutil.relativedelta


def main(argv) -> None:
    d1 = date.fromisoformat(argv[1])
    d2 = date.fromisoformat(argv[2])
    spark_conf = SparkConf()
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)
    st = SparkSession.builder.appName('Crimes Last 24 Months').getOrCreate()
    schema = StructType(
      [
        StructField("ID", IntegerType(), nullable=True),
        StructField("Case Number", StringType(), nullable=True),
        StructField("Date", StringType(),nullable = True),
        StructField("Block", StringType(),nullable = True),
        StructField("IUCR", StringType(),nullable = True),
        StructField("Primary Type", StringType(),nullable = True),
        StructField("Description", StringType(),nullable = True),
        StructField("Location Description", StringType(),nullable = True),
        StructField("Arrest",BooleanType(),nullable = True),
        StructField("Domestic",BooleanType(),nullable = True),
        StructField("Beat", IntegerType(),nullable = True),
        StructField("District", IntegerType(),nullable = True),
        StructField("Ward", IntegerType(),nullable = True),
        StructField("Community Area", IntegerType(),nullable = True),
        StructField("FBI Code", StringType(),nullable = True),
        StructField("X Coordinate", IntegerType(),nullable = True),
        StructField("Y Coordinate", IntegerType(),nullable = True),
        StructField("Year", IntegerType(),nullable = True),
        StructField("Updated On", StringType(),nullable = True),
        StructField("Latitude", DoubleType(),nullable = True),
        StructField("Longitude", DoubleType(),nullable = True),
        StructField("Location", StringType(),nullable = True),
        StructField("Historical Wards 2003-2015", IntegerType(),nullable = True),
        StructField("Zip Codes",IntegerType(),nullable = True),
        StructField("Community Areas",IntegerType(),nullable = True),
        StructField("Census Tracts",IntegerType(),nullable = True),
        StructField("Wards",IntegerType(),nullable = True),
        StructField("Boundaries - ZIP Codes",IntegerType(),nullable = True),
        StructField("Police Districts",IntegerType(),nullable = True),
        StructField("Police Beats",IntegerType(),nullable = True)
      ]
    )
    sqlContext = sql.SQLContext(spark_context)

    df = st.read.csv("Crimes_-_2001_to_present.csv", schema = schema, header = True)
    df = df.withColumn("Date",to_timestamp("Date","MM/dd/yyyy"))

    out = df.filter(df["Date"]>=d1)\
        .filter(df["Date"]<d2)\
        .groupBy("Year")\
        .count()\
        .orderBy("count",ascending=False)
    out.show(1)
    out.orderBy("count",ascending=True).show(1)
    out.select(F.avg("count")).show()

    """
      .filter(updatedTFDf("Date")>=dateStart)
      .groupBy("Primary Type")
      .count()
      .orderBy($"count".desc)
      .show(10)

"""
'''
    countycode_count_pair = df_full \
        .filter(df_full.type=="large_airport") \
        .rdd \
        .map(lambda line: (line.name, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda pair: pair[1], ascending=False) \
        .take(20)
        #.sortBy(lambda pair: pair[1], ascending=False) \
        #.take(20)
'''


if __name__ == "__main__":
    import timeit
    """
    Python program that uses Apache Spark to sum a list of numbers stored in files
    """
    print(str(timeit.timeit(lambda :main(sys.argv),number=1)) +" s")