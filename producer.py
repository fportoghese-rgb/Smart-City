import json
import time
import requests
import csv
import os
from datetime import datetime
from kafka import KafkaProducer

# Configurazione
BOOTSTRAP_SERVERS = [os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')] 
TOPIC = 'city-sensors'
API_KEY = "c5f9e150199f56d96cd24c9994b6c931" 
CSV_PATH = "spark/data/history.csv"

# Inizializzazione Producer Kafka
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def save_to_history(data):
    """Salva i dati nel CSV per il training futuro del modello Spark ML"""
    file_exists = os.path.isfile(CSV_PATH)
    columns = [
        'pm10', 'temp', 'hum', 'wind_speed', 'rain', 
        'pressure', 'hour', 'day_of_week', 'is_weekend', 'label'
    ]
    
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    
    with open(CSV_PATH, mode='a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        if not file_exists:
            writer.writeheader()
        
        pm10 = data.get('pm10', 0)
        rain = data.get('rain', 0)
        wind = data.get('wind_speed', 0)
        hum = data.get('hum', 50)

        # Logica per la label (Target simulato)
        target_pm10 = pm10
        if rain > 0: target_pm10 *= 0.7
        if wind > 5: target_pm10 *= 0.8
        if hum > 75: target_pm10 *= 1.1

        row = {
            'pm10': pm10,
            'temp': data.get('temp', 20),
            'hum': hum,
            'wind_speed': wind,
            'rain': rain,
            'pressure': data.get('pressure', 1013),
            'hour': data.get('hour'),
            'day_of_week': data.get('day_of_week'),
            'is_weekend': data.get('is_weekend'),
            'label': round(target_pm10, 2)
        }
        writer.writerow(row)

def get_full_data(lat, lon, zone_name, sensor_id):
    """Recupera dati Meteo (API 2.5) e Inquinamento dalle API OpenWeather"""
    
    # URL aggiornato alla 2.5 (Weather standard)
    url_weather = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
    url_pollution = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_KEY}"

    # 1. Recupero dati meteo
    res_w = requests.get(url_weather).json()
    
    # Estrazione dati per versione 2.5
    main_data = res_w.get('main', {})
    wind_data = res_w.get('wind', {})
    rain_data = res_w.get('rain', {})

    temp = main_data.get('temp', 20.0)
    hum = main_data.get('humidity', 50)
    press = main_data.get('pressure', 1013)
    wind_speed = wind_data.get('speed', 0.0)
    
    # Pioggia 1h (spesso ritorna 0 se non piove)
    rain_1h = rain_data.get('1h', 0.0)

    # 2. Recupero dati inquinamento (PM10)
    res_p = requests.get(url_pollution).json()
    pm10 = res_p['list'][0]['components']['pm10']
   
    # 3. Time Features
    now = datetime.now()
    day_of_week = now.weekday() 

    return {
        "sensor_id": sensor_id,
        "zone": zone_name,
        "lat": lat, 
        "lon": lon,
        "pm10": pm10,
        "temp": temp,
        "hum": hum,
        "wind_speed": wind_speed,
        "pressure": press,
        "rain": rain_1h,
        "hour": now.hour,
        "day_of_week": day_of_week,
        "month": now.month,
        "is_weekend": 1 if day_of_week >= 5 else 0,
        "timestamp": now.isoformat()
    }

# Configurazione sensori
sensors = [
    {"id": "S-01", "zone": "Milano", "lat": 45.4642, "lon": 9.1900},
    {"id": "S-02", "zone": "Pechino", "lat": 39.9042, "lon": 116.4074},
    {"id": "S-03", "zone": "Parigi", "lat": 48.8566, "lon": 2.3522},
    {"id": "S-04", "zone": "Roma", "lat": 41.8919, "lon": 12.5113}
]

print(f"🚀 Producer ATTIVO (API 2.5). Invio a Kafka e scrittura su {CSV_PATH}...")

try:
    while True:
        for s in sensors:
            try:
                payload = get_full_data(s['lat'], s['lon'], s['zone'], s['id'])
                producer.send(TOPIC, value=payload)
                save_to_history(payload)
                print(f"✅ {payload['zone']}: PM10={payload['pm10']}, Temp={payload['temp']}°C, Vento={payload['wind_speed']}m/s")
            except Exception as e:
                print(f"⚠️ Errore sensore {s['zone']}: {e}")
        
        print("-" * 40)
        time.sleep(3600) 
except KeyboardInterrupt:
    print("\nStopping Producer...")
finally:
    producer.close()