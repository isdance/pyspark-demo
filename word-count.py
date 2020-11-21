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
