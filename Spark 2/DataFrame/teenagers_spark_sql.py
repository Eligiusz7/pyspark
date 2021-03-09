from pyspark.sql import SparkSession
from pyspark.sql import Row


spark = SparkSession.builder.appName("Teenagers").getOrCreate()


def mapper(line):
    fields = line.split(',')
    return Row(id=int(fields[0]), name=str(fields[1].encode("utf-8")),
               age=int(fields[2]), friends=int(fields[3]))


lines = spark.sparkContext.textFile("file:///ml-100k/fakefriends.csv")
people = lines.map(mapper)
schema_people = spark.createDataFrame(people).cache()
schema_people.createOrReplaceTempView("people")

teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

for teenager in teenagers.collect():
    print(f'{teenager[0]}, {teenager[1]}, {teenager[2]}, {teenager[3]}')

schema_people.groupBy("age").count().orderBy("age").show()

spark.stop()
