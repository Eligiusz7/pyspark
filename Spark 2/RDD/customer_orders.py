from pyspark import SparkContext, SparkConf


conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(',')
    customer_id = fields[0]
    amount = fields[2]
    return int(customer_id), float(amount)


lines = sc.textFile("file:///ml-100k/customer-orders.csv")
rdd = lines.map(parse_line)
total_amount = rdd.reduceByKey(lambda x, y: x + y)
total_amount_sorted = total_amount.map(lambda x: (x[1], x[0])).sortByKey()
results = total_amount_sorted.collect()

for result in results:
    print(f'{result[1]}: {result[0]:.2f}')
