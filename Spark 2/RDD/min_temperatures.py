from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(',')
    station_id = fields[0]
    entry_type = fields[2]
    temperature = float(fields[3])*0.1
    return station_id, entry_type, temperature


lines = sc.textFile("file:///ml-100k/1800.csv")
rdd = lines.map(parse_line)
min_temps = rdd.filter(lambda x: "TMIN" in x[1])
station_temps = min_temps.map(lambda x: (x[0], x[2]))
min_temps = station_temps.reduceByKey(lambda x, y: min(x, y))
results = min_temps.collect()

for result in results:
    print(f'{result[0]}: {result[1]:.2f} C')
