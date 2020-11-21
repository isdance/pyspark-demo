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

