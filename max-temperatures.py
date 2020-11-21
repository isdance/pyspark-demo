from pyspark import SparkConf, SparkContext


def _parseLine(line):
    fields = line.split(",")
    station_id = fields[0]
    entry_type = fields[2]
    # from c temperature to f temperature
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (station_id, entry_type, temperature)


conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

lines = sc.textFile("./1800.csv")

rdd = lines.map(_parseLine)

results = (
    rdd.filter(lambda x: "TMAX" in x[1])
    # remove 'entry_type'
    .map(lambda x: (x[0], x[2]))
    # find min temperature for each station_id
    .reduceByKey(lambda x, y: max(x, y)).collect()
)

for result in results:
    print(f"station id: {result[0]}, max temperature is {result[1]:.2f}")
