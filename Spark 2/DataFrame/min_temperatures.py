from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema = StructType([
                    StructField("station_id", StringType(), True),
                    StructField("date", IntegerType(), True),
                    StructField("measure_type", StringType(), True),
                    StructField("temperature", FloatType(), True)
                    ])

sdf = spark.read.schema(schema).csv("file:///ml-100k/1800.csv")
min_temps = sdf.filter(sdf.measure_type == "TMIN")

station_temps = min_temps.select("station_id", "temperature")

min_temps_by_station = station_temps.groupBy("station_id").min("temperature")
min_temps_by_station.show()

spark.stop()
