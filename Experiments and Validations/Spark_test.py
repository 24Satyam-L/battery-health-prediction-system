from pyspark.sql import SparkSession

# Start Spark
spark = SparkSession.builder \
    .appName("BatterySystemTest") \
    .getOrCreate()

# Load sample data (use your existing CSV)
df = spark.read.csv("C:\\Projects\\Battery Engineering\\data\\00001.csv", header=True, inferSchema=True)

# Show data
df.show(5)

# Print schema
df.printSchema()