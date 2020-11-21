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
