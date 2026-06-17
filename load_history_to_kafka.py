import csv
import json
import time
import os
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "city-sensors"
CSV_PATH = "spark/data/history.csv"

sensors = [
    {"id": "S-01", "zone": "Milano", "lat": 45.4642, "lon": 9.1900},
    {"id": "S-02", "zone": "Pechino", "lat": 39.9042, "lon": 116.4074},
    {"id": "S-03", "zone": "Parigi", "lat": 48.8566, "lon": 2.3522},
    {"id": "S-04", "zone": "Roma", "lat": 41.8919, "lon": 12.5113}
]

def main():
    print(f"🔄 Connessione a Kafka su {KAFKA_BROKER}...")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Connesso a Kafka!")
    except Exception as e:
        print(f"❌ Errore di connessione a Kafka: {e}")
        return

    print(f"📂 Lettura del file {CSV_PATH}...")
    if not os.path.exists(CSV_PATH):
        print(f"❌ Errore: Il file {CSV_PATH} non esiste.")
        return

    with open(CSV_PATH, mode='r') as f:
        reader = csv.DictReader(f)
        count = 0
        
        for i, row in enumerate(reader):
            s = sensors[i % 4]
            ts = datetime.fromisoformat(row["timestamp"])
            
            payload = {
                "sensor_id": s["id"],
                "zone": s["zone"],
                "lat": s["lat"], 
                "lon": s["lon"],
                "pm10": float(row["pm10"]),
                "temp": float(row["temp"]),
                "hum": int(float(row["hum"])),
                "wind_speed": float(row["wind_speed"]),
                "pressure": int(float(row["pressure"])),
                "rain": float(row["rain"]),
                "hour": int(float(row["hour"])),
                "day_of_week": int(float(row["day_of_week"])),
                "month": ts.month,
                "is_weekend": int(float(row["is_weekend"])),
                "timestamp": row["timestamp"]
            }

            producer.send(TOPIC, value=payload)
            count += 1
            time.sleep(0.01)

            if count % 200 == 0:
                print(f"📤 Inviati {count} record a Kafka...")
                producer.flush()

    producer.flush()
    print(f"✅ Finito! Tutti i {count} record storici sono stati simulati in Kafka come dati in tempo reale.")

if __name__ == "__main__":
    main()
