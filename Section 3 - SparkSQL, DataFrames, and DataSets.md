### 22. Introducing SparkSQL

SparkSQL extends RDD to a "DataFrame" object

DataFrames:

- contains row objects
- Can run SQL queries
- Can have a schema (leading to more efficient storage)
- Read and write to JSON, Hive, parquest, csv ...
- Communicates with JDBC/ODBC, Tableau

### 23. [Activity] Executing SQL commands and SQL-style functions on a DataFrame

```py
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()


def mapper(line):
    fields = line.split(",")
    return Row(
        ID=int(fields[0]),
        name=str(fields[1].encode("utf-8")),
        age=int(fields[2]),
        numFriends=int(fields[3]),
    )


lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
    print(teen)

# We can also use functions instead of SQL queries:
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()

```

### 24. Using DataFrames instead of RDD's

We can read CSV file without sparkContext object. However, using DataFrames with unstructured text data isn't a great fit. This is a case where RDD's would be more straightforward.

```py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("./fakefriends-header.csv")
)

print("Here is our inferred schema:")
# root
#  |-- userID: integer (nullable = true)
#  |-- name: string (nullable = true)
#  |-- age: integer (nullable = true)
#  |-- friends: integer (nullable = true)
people.printSchema()


print("Let's display the name column:")
people.select("name").show()

print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("age").count().show()

print("Make everyone 10 years older:")
people.select(people.name, people.age + 10).show()

spark.stop()
```

### 25. [Exercise] Friends by Age, with DataFrames

```py
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("./fakefriends-header.csv")
)

# now the schema is
# root
#  |-- userID: integer (nullable = true)
#  |-- name: string (nullable = true)
#  |-- age: integer (nullable = true)
#  |-- friends: integer (nullable = true)

results = people.select("age", "friends").groupBy("age").avg("friends").sort("age").show()

# use alias and 2 decimal places
results = (
    people.select("age", "friends")
    .groupBy("age")
    .agg(func.round(func.avg("friends"), 2).alias("friends_avg"))
    .sort("age")
    .show()
)

spark.stop()
```

### 27. [Activity] Word Count, with DataFrames

Using DataFrames with unstructured text data isn't a great fit.

Our initial DataFrame will just have Row objects, with a column named "value" for each line of text.

This is a case where RDD's would be more straightforward.

Remember RDD's can be converted to DataFrames, and vice versa.

```py
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of 'book' text file into a dataframe
inputDF = spark.read.text("./book")

# Split using a regular expression that extracts words, then convert each word into row, and give an alias 'word'
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = words.select(func.lower(words.word).alias("word"))

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort("count")

# Show the results.
wordCountsSorted.show(wordCountsSorted.count())

```

### 28. [Activity] Minimum Temperature, with DataFrames (using a custom schema)

This time we don't have a header row, so we need to use 'withColumn' to update or add a column

```py
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

# create a custom schema, StructField  can takes 4 argument, column name(String), column type (DataType), nullable column (Boolean) and metadata (MetaData)
schema = StructType(
    [
        StructField("stationID", StringType(), True),
        StructField("date", IntegerType(), True),
        StructField("measure_type", StringType(), True),
        StructField("temperature", FloatType(), True),
    ]
)

# // Read the file as dataframe
df = spark.read.schema(schema).csv("./1800.csv")
df.printSchema()

# Filter out all but TMIN entries
minTemps = df.filter(df.measure_type == "TMAX")

# Select only stationID and temperature
stationTemps = minTemps.select("stationID", "temperature")

# Aggregate to find minimum temperature for every station
maxTempsByStation = stationTemps.groupBy("stationID").max("temperature")
maxTempsByStation.show()

# Convert temperature to fahrenheit and sort the dataset, and update "temperature" column with the converted value
maxTempsByStationF = (
    maxTempsByStation.withColumn(
        "temperature", func.round(func.col("max(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2)
    )
    .select("stationID", "temperature")
    .sort("temperature")
)

# Collect, format, and print the results
results = maxTempsByStationF.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))

spark.stop()
```

### 29. [Exercise] Implement Total Spent by Customer with DataFrames

```py
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("totalSpendingByCustomer").getOrCreate()

# create a custom schema, StructField  can takes 4 argument, column name(String), column type (DataType), nullable column (Boolean) and metadata (MetaData)
schema = StructType(
    [
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("amount", FloatType(), True),
    ]
)

# custom schema:
#  |-- customer_id: integer (nullable = true)
#  |-- product_id: integer (nullable = true)
#  |-- amount: float (nullable = true)
df = spark.read.schema(schema).csv("./customer-orders.csv")
df.printSchema()

df = (
    df.select("customer_id", "amount")
    .groupBy("customer_id")
    .agg(func.round(func.sum("amount"), 2).alias("total_amount"))
    .sort("total_amount")
)

results = df.collect()

for result in results:
    print(f"customer id: {result[0]}, total amount is {result[1]:.2f}")

spark.stop()

```
