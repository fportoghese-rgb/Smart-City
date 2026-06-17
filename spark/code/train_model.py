from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import col
import os

# Inizializza la sessione Spark
spark = SparkSession.builder.appName("TrainSmartCityModel").getOrCreate()

# Percorso interno al container (mappato sul tuo volume locale)
path_csv = "/tmp/data/history.csv" 

print(f"--- 🧠 Avvio addestramento modello multivariato ---")

try:
    if os.path.exists(path_csv):
        # 1. Caricamento dati con inferSchema
        raw_data = spark.read.csv(path_csv, header=True, inferSchema=True)
        
        # 2. PULIZIA DATI: Forziamo la conversione in Double e rimuoviamo i non-numerici
        # Questo risolve l'errore "Column label must be of type numeric"
        data = raw_data.withColumn("pm10", col("pm10").cast("double")) \
                       .withColumn("temp", col("temp").cast("double")) \
                       .withColumn("hum", col("hum").cast("double")) \
                       .withColumn("wind_speed", col("wind_speed").cast("double")) \
                       .withColumn("rain", col("rain").cast("double")) \
                       .withColumn("pressure", col("pressure").cast("double")) \
                       .withColumn("hour", col("hour").cast("double")) \
                       .withColumn("day_of_week", col("day_of_week").cast("double")) \
                       .withColumn("is_weekend", col("is_weekend").cast("double")) \
                       .withColumn("label", col("label").cast("double")) \
                       .dropna() # Elimina righe con valori nulli o header duplicati nel testo

        count = data.count()
        if count < 10:
            print(f"⚠️ Dati insufficienti ({count} righe valide). Aspetta che il Producer accumuli più dati!")
        else:
            # 3. Preparazione delle Features (Vettore di input)
            input_features = [
                "pm10", "temp", "hum", "wind_speed", "rain", 
                "pressure", "hour", "day_of_week", "is_weekend"
            ]
            assembler = VectorAssembler(inputCols=input_features, outputCol="features")
            train_data = assembler.transform(data)

            # 4. Addestramento Linear Regression
            # labelCol è il valore target (es. PM10 previsto)
            lr = LinearRegression(featuresCol="features", labelCol="label")
            model = lr.fit(train_data)

            # 5. Salvataggio del Modello
            # Sovrascrive il vecchio modello così l'Analyzer usa sempre il più recente
            model.write().overwrite().save("/opt/spark/smart_city_model")
            
            print(f"--- ✨ Risultati Training ---")
            print(f"✅ MODELLO AGGIORNATO! Record validi processati: {count}")
            print(f"📊 Intercetta: {round(model.intercept, 4)}")
            print(f"📈 Coefficienti: {model.coefficients}")
    else:
        print(f"❌ Errore: File {path_csv} non trovato. Verifica che il volume Docker sia montato.")

except Exception as e:
    print(f"❌ Errore critico durante il training: {e}")

finally:
    spark.stop()
    print("--- 🏁 Sessione Spark terminata ---")