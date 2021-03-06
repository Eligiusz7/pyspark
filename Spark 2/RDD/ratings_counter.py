from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sorted_results = dict(sorted(result.items()))
for key, value in sorted_results.items():
    print(f"{key} {value}")
