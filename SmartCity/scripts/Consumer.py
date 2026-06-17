from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# 1. Definizione dello Schema (Deve corrispondere ai campi del tuo Producer)
city_schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("zone", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("cars_count", IntegerType()),
    StructField("pm10", DoubleType()),
    StructField("timestamp", StringType())
])

# 2. Inizializzazione Sessione Spark
# Nota: includiamo il pacchetto per connettere Spark a Kafka
spark = SparkSession.builder \
    .appName("SmartCitySparkConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR") # Riduce il log inutile nel terminale

# 3. Lettura dello Stream da Kafka
# Kafka invia dati in formato binario (key, value), noi leggiamo il 'value'
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "city-sensors") \
    .load()

# 4. Trasformazione: Casting da Binario a Stringa e Parsing JSON
json_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), city_schema).alias("data")) \
    .select("data.*")

# 5. Esempio di analisi: Filtriamo solo le zone con PM10 critico (> 70)
# (Puoi commentare questa riga se vuoi vedere tutti i dati)
critical_zones_df = json_df.filter(col("pm10") > 70)

# 6. Output sulla Console
# Questo stamperà i dati nel tuo terminale ogni volta che arrivano
query = critical_zones_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

print("🔥 Spark è in ascolto... Attendo dati da Kafka...")
query.awaitTermination()
