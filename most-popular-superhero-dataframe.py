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
