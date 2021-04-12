import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import window
import pyspark.sql.functions as F


def main(file) -> None:
    spark = SparkSession \
        .builder \
        .appName("StreamingWindow") \
        .getOrCreate()

    # Create DataFrame representing the stream of input lines from connection to localhost:9999
    userSchema = StructType()\
        .add("poiID","integer")\
        .add("nombre","string")\
        .add("direccion","string")\
        .add("telefono","string")\
        .add("correoelectronico","string")\
        .add("latitude","string")\
        .add("longitude","string")\
        .add("altitud","string")\
        .add("capacidad","integer",True)\
        .add("capacidad_discapacitados","string")\
        .add("fechahora_ultima_actualizacion","timestamp")\
        .add("libres","integer",True)\
        .add("libres_discapacitados","string")\
        .add("nivelocupacion_naranja","string")\
        .add("nivelocupacion_rojo","string")\
        .add("smassa_sector_sare","string")



    lines = spark \
        .readStream \
        .format("csv") \
        .schema(userSchema)\
        .option("header", "true") \
        .load(file)

    values = lines.select(lines['nombre'],lines["fechahora_ultima_actualizacion"],
                          (1 - (lines["libres"]/lines["capacidad"])).alias("ocupacion"))\
        .groupBy(window("fechahora_ultima_actualizacion","8 minutes","2 minutes"), "nombre")\
        .agg(F.mean("ocupacion").alias("Ocupacion media"),F.max('ocupacion').alias("Maxima ocupacion"),
             F.min('ocupacion').alias('Minima ocupacion'))\
        .orderBy('window')

    # Generate running word count

    # Start running the query that prints the running counts to the console
    query = values \
        .writeStream \
        .format("console") \
        .option("truncate","false")\
        .option("numRows",'1000')\
        .outputMode("complete") \
        .start()

    query.awaitTermination()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: spark-submit StreamingWordCountFromFiles <dir>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])
