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

