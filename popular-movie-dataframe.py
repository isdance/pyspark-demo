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
