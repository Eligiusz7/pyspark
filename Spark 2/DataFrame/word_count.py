from pyspark.sql import SparkSession
from pyspark.sql import functions as func


spark = SparkSession.builder.appName("WordCount").getOrCreate()
sdf = spark.read.text("file:///ml-100k/book.txt")

words = sdf.select(func.explode(func.split(sdf.value, "\\W+")).alias("word"))
words.filter(words.word != "")

lowercase_words = words.select(func.lower(words.word).alias("word"))
word_counts = lowercase_words.groupBy("word").count()
word_counts_sorted = word_counts.sort("count")
word_counts_sorted.show(word_counts_sorted.count())

spark.stop()
