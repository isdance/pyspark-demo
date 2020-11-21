from pyspark import SparkConf, SparkContext


def _parseLine(line):
    fields = line.split(",")
    customer_id = int(fields[0])
    amount = float(fields[2])
    return (customer_id, amount)


conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

lines = sc.textFile("./customer-orders.csv")

results = lines.map(_parseLine).reduceByKey(lambda x, y: x + y)

# now i want to sort the result by spending amount
results = (
    lines.map(_parseLine)
    # now key is the customer_id, reduce by customer_id will get total spending per customer_id
    .reduceByKey(lambda x, y: x + y)
    # for sort by spending purpose, we need to swap the position of customer_id and total spending amount
    .map(lambda x: (x[1], x[0]))
    .sortByKey()
    .collect()
)

for result in results:
    print(f"amount is {result[0]:.2f}, customer id is {result[1]}")
