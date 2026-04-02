import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Configurazione del Producer
# Si collega a localhost:9092 perché Kafka ha la porta esposta sul tuo PC
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = 'city-sensors'

# Sensori posizionati in punti strategici
sensors = [
    {"id": "S-01", "zone": "Duomo", "lat": 45.4642, "lon": 9.1900},
    {"id": "S-02", "zone": "Navigli", "lat": 45.4520, "lon": 9.1760},
    {"id": "S-03", "zone": "Brera", "lat": 45.4710, "lon": 9.1870},
    {"id": "S-04", "zone": "Isola", "lat": 45.4860, "lon": 9.1860}
]

print(f"🚀 Simulatore avviato! Invio dati al topic '{TOPIC}'...")

try:
    while True:
        for s in sensors:
            # Generiamo dati realistici ma casuali
            payload = {
                "sensor_id": s["id"],
                "zone": s["zone"],
                "lat": s["lat"],
                "lon": s["lon"],
                "cars_count": random.randint(5, 120),  # Numero di auto
                "pm10": round(random.uniform(15.0, 95.0), 2), # Livello inquinamento
                "timestamp": datetime.now().isoformat()
            }
            
            # Invio a Kafka
            producer.send(TOPIC, value=payload)
            print(f"📡 Inviato da {s['zone']}: {payload['cars_count']} auto, PM10: {payload['pm10']}")
            
        time.sleep(3) # Aspettiamo 3 secondi prima del prossimo invio
except KeyboardInterrupt:
    print("\n🛑 Simulatore fermato.")
finally:
    producer.close()