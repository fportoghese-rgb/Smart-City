import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer


BOOTSTRAP_SERVERS = ['localhost:9092'] 
TOPIC = 'city-sensors'
API_KEY = "c5f9e150199f56d96cd24c9994b6c931" 

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_pollution_data(lat, lon, zone_name, sensor_id):
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_KEY}" #fondamentale per andare al mio account openwhteher
    
  
    response = requests.get(url, timeout=5)
    
    response.raise_for_status() 
    
    data = response.json()
  
    pm10 = data['list'][0]['components']['pm10']
    
    
    aqi = data['list'][0]['main']['aqi']

    return {
        "sensor_id": sensor_id,
        "zone": zone_name,
        "lat": lat, 
        "lon": lon,
        "pm10": pm10,
        "aqi": aqi, # 1=Buono, 5=Pessimo
        "timestamp": datetime.now().isoformat()
    }

sensors = [
    {"id": "S-01", "zone": "Milano", "lat": 45.4642, "lon": 9.1900},
    {"id": "S-02", "zone": "Pechino", "lat": 39.9042, "lon": 116.4074}
] # questo serve per dire, prendi l'aria di queste zone del mondo e dimmi come è 

print(f"🚀 Avvio monitoraggio REALE su topic: {TOPIC}")

try:
    while True:
        for s in sensors:
            payload = get_pollution_data(s['lat'], s['lon'], s['zone'], s['id'])
            producer.send(TOPIC, value=payload)
            print(f" DATO REALE RICEVUTO: {payload['zone']} -> PM10: {payload['pm10']} (AQI: {payload['aqi']})")
        
        print("-" * 30)
        time.sleep(10) # OpenWeather aggiorna i dati ogni ora circa, quindi 10s o 60s va bene
except Exception as e:
    print(f" ERRORE: Non riesco a prendere i dati reali. Dettaglio: {e}")
