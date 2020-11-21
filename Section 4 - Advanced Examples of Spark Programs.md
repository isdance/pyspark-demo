### 31. [Activity] Find the Most Popular Movie

Below code is to use dataframe to calculate average points for each movie

```py
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    TimestampType,
)

spark = SparkSession.builder.appName("RatingsHistogram").getOrCreate()

# create a custom schema, StructField  can takes 4 argument, column name(String), column type (DataType), nullable column (Boolean) and metadata (MetaData)
schema = StructType(
    [
        StructField("user_id", IntegerType(), True),
        StructField("movie_id", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

#  |-- user_id: integer (nullable = true)
#  |-- movie_id: integer (nullable = true)
#  |-- rating: integer (nullable = true)
#  |-- timestamp: timestamp (nullable = true)

# u.data uses '\t' as separator
df = spark.read.option("sep", "\t").schema(schema).csv("./ml-100k/u.data")
df.printSchema()

df = (
    df.select("user_id", "movie_id", "rating")
    .groupBy("movie_id")
    # avg rating is total rating points, divided by number of users
    .agg(func.round((func.sum("rating") / func.count("user_id")), 2).alias("avg_rating"))
    .sort("avg_rating")
    .orderBy(func.desc("avg_rating"))
)
results = df.collect()

for result in results:
    print(f"movie id: {result[0]}, average rating is {result[1]:.2f}")

spark.stop()

```

### 32. [Activity] Use Broadcast Variables to Display Movie Names Instead of ID Numbers

- use sc.broadcast() function to broadcast objects to the executors, such that they're always there whenever needed
- Then use .value() function the get the broadcast object
- Use the broadcasted object whenever you want - map functions, UDF's

```py
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType
import codecs


# a lookup that will be broadcast to any running executor on every node:
def loadMovieNames():
    movieIdToNameLookup = {}
    with codecs.open("./ml-100k/u.item", "r", encoding="ISO-8859-1", errors="ignore") as f:
        for line in f:
            fields = line.split("|")
            movieIdToNameLookup.update({int(fields[0]): fields[1]})
    return movieIdToNameLookup


spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# broad cast the lookup function we just created
nameDict = spark.sparkContext.broadcast(loadMovieNames())

# Create schema when reading u.data
schema = StructType(
    [
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

# Load up movie data as dataframe
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("./ml-100k/u.data")

movieCounts = moviesDF.groupBy("movieID").count()

# Create a user-defined function to look up movie names from our broadcasted dictionary
def lookupName(movieID):
    # must use nameDict.value to retrieve the broadcast value
    return nameDict.value[movieID]


# convert the python function into a spark SQL function
lookupNameUDF = func.udf(lookupName)

# Add a movieTitle column using our new udf
moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))

# Sort the results
sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

# Grab the top 10
sortedMoviesWithNames.show(10, False)

# Stop the session
spark.stop()

```

### 34. [Activity] Run the Script - Discover Who the Most Popular Superhero is!

```py
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType(
    [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
)

names = spark.read.schema(schema).option("sep", " ").csv("./Marvel-Names.txt")

lines = spark.read.text("./Marvel-Graph.txt")

connections = (
    # the first column of each row is the id of a superhero we want to check
    lines.withColumn("id", func.split(func.col("value"), " ")[0])
    # - 1 means exclude the superhero him/herself
    .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1)
    .groupBy("id")
    .agg(func.sum("connections").alias("connections"))
)

mostPopular = connections.sort(func.col("connections").desc()).first()

mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(f"{mostPopularName[0]}  is the most popular superhero with {mostPopular[1]} co-appearances.")

# Stop the session
spark.stop()

```

### 35. [Exercise] Find the Most Obscure Superheroes

Note we have more than 1 superhero that only has 1 connection.

```py
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType(
    [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
)

names = spark.read.schema(schema).option("sep", " ").csv("./Marvel-Names.txt")

lines = spark.read.text("./Marvel-Graph.txt")

connections = (
    # the first column of each row is the id of a superhero we want to check
    lines.withColumn("id", func.split(func.col("value"), " ")[0])
    # - 1 means exclude the superhero him/herself
    .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1)
    .groupBy("id")
    .agg(func.sum("connections").alias("connections"))
)


minConnectionCount = connections.agg(func.min("connections")).first()[0]
# list all heroes who has the least amount of connections
mostObscureHeroes = connections.filter(func.col("connections") == minConnectionCount)

mostObscureHeroNames = names.join(mostObscureHeroes, "id", "inner")


print(
    f"The following characters have only {minConnectionCount} connection{ '' if minConnectionCount <=1 else 's' }"
)

mostObscureHeroNames.select("name").show()

# Stop the session
spark.stop()

```
