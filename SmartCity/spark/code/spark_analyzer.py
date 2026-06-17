from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# 1. Definizione dello Schema
# Deve corrispondere esattamente alle chiavi che invii nel Producer Python
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("aqi", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

# 2. Inizializzazione Sessione Spark
spark = SparkSession.builder \
    .appName("SmartCityPollutionAnalysis") \
    .getOrCreate()

# Riduciamo il rumore dei log per vedere solo i nostri dati
spark.sparkContext.setLogLevel("WARN")

# 3. Connessione a Kafka
# Nota: usiamo 'kafka:29092' perché Spark è dentro la rete Docker
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "city-sensors") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Trasformazione dei dati
# Kafka manda dati in formato binario (Byte), noi li convertiamo in Stringa e poi in JSON
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. Output a console
# Questo creerà una tabella nel terminale che si aggiorna ogni volta che arriva un dato
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

print("🚀 Analisi Spark avviata... In attesa di dati da Kafka...")
query.awaitTermination()