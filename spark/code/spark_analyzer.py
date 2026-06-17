from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, to_json, expr, to_timestamp, lit, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegressionModel

# 1. Schema aggiornato: deve combaciare esattamente con i campi inviati dal Producer (API 2.5)
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("temp", DoubleType(), True),
    StructField("hum", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("pressure", IntegerType(), True),
    StructField("rain", DoubleType(), True),
    StructField("hour", IntegerType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("is_weekend", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

spark = SparkSession.builder \
    .appName("SmartCityML-FullFeatures") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Caricamento Modello
# ATTENZIONE: Se ricevi un errore di "mismatch", dovrai ri-addestrare il modello 
# con lo stesso numero di colonne definite in feature_cols qui sotto.
model = LinearRegressionModel.load("/opt/spark/smart_city_model")

# 3. Assembler con i NUOVI attributi (Feature Engineering)
# Queste sono le variabili che influenzano la predizione del PM10
feature_cols = [
    "pm10", "temp", "hum", "wind_speed", "rain", 
    "pressure", "hour", "day_of_week", "is_weekend"
]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features",
    handleInvalid="keep"
)

# 4. Ingestion da Kafka
# Nota: Usiamo 'kafka' come hostname perché all'interno della rete Docker 
# il servizio si chiama così (riferimento al tuo docker-compose)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "city-sensors") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

def predict_batch(batch_df, batch_id):
    # Invece di cancellare le righe (drop), riempiamo i valori mancanti con 0 
    # per permettere al modello di processare tutto il batch senza crashare
    clean_df = batch_df.na.fill(0)
    
    if clean_df.count() > 0:
        print(f"\n>>> Elaborazione Batch {batch_id} - Righe ricevute: {clean_df.count()}")
        
        # Esecuzione Predizione ML
        featurized = assembler.transform(clean_df)
        predictions = model.transform(featurized)
        
        # Preparazione dati per Elasticsearch/Kibana
        final = predictions.withColumn("current_time", to_timestamp(col("timestamp"))) \
            .withColumnRenamed("prediction", "predicted_pm10") \
            .withColumn("@timestamp", col("current_time")) \
            .withColumn("status", 
                when(col("predicted_pm10") > 120, lit("CRITICAL"))
                .when(col("predicted_pm10") > 60, lit("WARNING"))
                .otherwise(lit("HEALTHY"))) \
            .drop("features")
            
        # Mostra un'anteprima nel terminale
        final.select("zone", "pm10", "predicted_pm10", "wind_speed", "status").show(5)

        # Invio dei risultati al topic delle predizioni su Kafka
        final.selectExpr("CAST(sensor_id AS STRING) AS key", "to_json(struct(*)) AS value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("topic", "city-predictions") \
            .save()
    else:
        print(f"Batch {batch_id} in attesa di dati dai sensori...")

# 5. Avvio dello Streaming
query = parsed_df.writeStream.foreachBatch(predict_batch).start()
query.awaitTermination()