### 8. Introduction to Spark

#### What is Spark

A fast and general engine for large-scale data processing, distribute big data into a large cluster of computers

### 9. The Resilient Distributed Dataset (RDD)

- The Spark context object
  - Created by your driver program
  - Is responsible for making RDD's resilient nd distributed
  - Creates RDD's
  - The Spark shell creates a "sc" object for you, "sc" stands for Spark Context

### 11. Key/Value RDD's, and the Average Friends by Age Example

- reduceByKey() - combine values with the same key, using some functions. for example, rdd.reduceByKey(lambda x, y: x + y) will add them up
- groupByKey() - Group values with the same key
- ortByKey() - Sort RDD by key values
- keys(), values() - Create an RDD of just keys, or just values

You can do SQL-like joins on 2 key/value RDD's

- join, rightOuterJoin, leftOuterJoin, cogroup, subtractByKey

If you your transformation doesn't affect the keys, use mapValues() and flatMapValues(), it's more efficient (because the partitions of data remains the same, no shuffle around which is more expensive)

### 12. [Activity] Running the Average Friends by Age Example

```py
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

```

### 15. [Activity] Running the Maximum Temperature by Location Example

```py
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

```

### 16. [Activity] Counting Word Occurrences using flatmap()

```py
from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

lines = sc.textFile("./book")

# use flatMap to break lines into words, and count occurrences
words_count = lines.flatMap(lambda x: x.split()).countByValue()

for word, count in words_count.items():
    clean_word = word.encode("ascii", "ignore")
    print(f"word {clean_word} appears {count} times")

```

There are some further cleaning needs to be done. case sensitivity and punctuation.

### 17. [Activity] Improving the Word Count Script with Regular Expressions

```py
import re
from pyspark import SparkConf, SparkContext


def _normalize_words(text):
    return re.compile(r"\W+", re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

lines = sc.textFile("./book")

# use flatMap to break lines into words, and count occurrences
words_count = lines.flatMap(_normalize_words).countByValue()

for word, count in words_count.items():
    clean_word = word.encode("ascii", "ignore")
    if clean_word:
        print(f"word {clean_word} appears {count} times")

```

### 18. [Activity] Sorting the Word Count Results

```py
import re
from pyspark import SparkConf, SparkContext


def _normalize_words(text):
    return re.compile(r"\W+", re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

lines = sc.textFile("./book")

# use flatMap to break lines into words, and count occurrences
words_count = (
    lines.flatMap(_normalize_words)
    # got something like (you,1), (the,1) ...
    .map(lambda x: (x, 1))
    # add word count by key, like (you, 1878), (the, 1292)...
    .reduceByKey(lambda x, y: (x + y))
    # swap key with value, like (1878, you), (1292, the)...
    .map(lambda x: (x[1], x[0]))
    .sortByKey()
    .collect()
)

for result in words_count:
    count = str(result[0])
    clean_word = result[1].encode("ascii", "ignore")
    if clean_word:
        print(f"word {clean_word} appears {count} times")

```

### 19. [Exercise] Find the Total Amount Spent by Customer

### 20. [Excercise] Check your Results, and Now Sort them by Total Amount Spent

### 21. Check Your Sorted Implementation and Results Against Mine.

###

```py
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

```
