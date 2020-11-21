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

