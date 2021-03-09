from pyspark.sql import SparkSession
from pyspark.sql import functions as func


spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()
sdf = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///ml-100k/fakefriends-header.csv")

friends_by_age = sdf.select("age", "friends")
friends_by_age.groupBy("age").agg(func.round(func.avg("friends"), 2)
                                  .alias("avg_friends")).sort("age").show()
spark.stop()
