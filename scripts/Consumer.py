from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("zone", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("pm10", DoubleType()),
    StructField("timestamp", StringType())
])

spark = SparkSession.builder \
    .appName("SmartCitySpark") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# IMPORTANTE: Usiamo kafka:29092 perché Spark è dentro la rete 'tap'
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "city-sensors") \
    .load()

clean_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Qui Spark analizza: mostriamo solo se PM10 > 50 (Soglia UE)
critical_df = clean_df.filter(col("pm10") > 50)

query = critical_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()