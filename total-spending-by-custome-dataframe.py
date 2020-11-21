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

