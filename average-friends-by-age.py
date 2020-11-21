from pyspark import SparkConf, SparkContext


def _parseLine(line):
    fields = line.split(",")
    age = int(fields[2])
    no_of_friends = int(fields[3])
    return (age, no_of_friends)


conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

lines = sc.textFile("./fakefriends.csv")

rdd = lines.map(_parseLine)

results = (
    # transform line data into something like (18, (322, 1)), (18, (44, 1))........
    rdd.mapValues(lambda v: (v, 1))
    # add lines if the age is the same, for example (18, (322, 1)), (18, (44, 1)) into (18, (366, 2))
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    # get the average for example (18, (366, 2)) into (18, 183)
    .mapValues(lambda v: v[0] / v[1]).sortByKey()
    # convert rdd into python object
    .collect()
)

for result in results:
    print(result)
