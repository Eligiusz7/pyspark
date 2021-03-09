from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import functions as func


spark = SparkSession.builder.appName("CustomerOrders").getOrCreate()

schema = StructType([
                    StructField("customer_id", IntegerType(), True),
                    StructField("order_id", IntegerType(), True),
                    StructField("amount", FloatType(), True),
                    ])

sdf = spark.read.schema(schema).csv("file:///ml-100k/customer-orders.csv")

total_by_customers = sdf.groupBy("customer_id").agg(func.round(func.sum("amount"), 2).alias("total_amount"))
total_sorted = total_by_customers.sort("total_amount")
total_sorted.show(total_sorted.count())

spark.stop()
