### Installation (on Mac)

https://sundog-education.com/spark-python/

- Step 01: using Homebrew to install spark-python

```
brew install apache-spark
```

- Step 02: config log4j

Create a log4j.properties file via

```
cd /usr/local/Cellar/apache-spark/3.0.1/libexec/conf (substitute 3.0.1 for the version actually installed)
cp log4j.properties.template log4j.properties
```

Edit the log4j.properties file and change the log level from `INFO` to `ERROR` on log4j.rootCategory.It’s OK if Homebrew does not install Spark 3; the code in the course should work fine with recent 2.x releases as well.

- Step 03: config python version for pyspark, default is python2

in you .zshrc or .bashrc file, add below line

```
export PYSPARK_PYTHON=python3
```

#### Test your first Spark script

Firstly, download `MovieLens 100K Dataset` from `grouplens` website.

[MovieLens 100K Dataset](http://files.grouplens.org/datasets/movielens/ml-100k.zip)

Then create your first pyspark script

ratings-counter.py

```py
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster('local').setAppName('RatingsHistogram')
sc = SparkContext(conf=conf)

lines = sc.textFile('./ml-100k/u.data')
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))

for key, value, in sortedResults.items():
    print(f"{key} --> {value}")
```

then run `spark-submit ratings-counter.py`

result should be

```
1 --> 6110
2 --> 11370
3 --> 27145
4 --> 34174
5 --> 21201
```
