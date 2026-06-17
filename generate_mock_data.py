import csv
import random
import os

CSV_PATH = "spark/data/history.csv"

# Dati di base per le 4 città (Milano, Pechino, Parigi, Roma)
base_data = [
    {"pm10": 8.2, "temp": 25.95, "hum": 48, "wind_speed": 0.89, "rain": 0.0, "pressure": 1016},
    {"pm10": 128.95, "temp": 20.4, "hum": 41, "wind_speed": 1.5, "rain": 0.0, "pressure": 1012},
    {"pm10": 10.58, "temp": 23.04, "hum": 23, "wind_speed": 1.34, "rain": 0.0, "pressure": 1020},
    {"pm10": 5.62, "temp": 20.66, "hum": 26, "wind_speed": 4.92, "rain": 0.0, "pressure": 1018}
]

columns = [
    'pm10', 'temp', 'hum', 'wind_speed', 'rain', 
    'pressure', 'hour', 'day_of_week', 'is_weekend', 'label'
]

os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)

with open(CSV_PATH, mode='w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=columns)
    writer.writeheader()
    

    for _ in range(1000):
        for base in base_data:
            
            pm10 = max(0.1, base["pm10"] + random.uniform(-5.0, 5.0))
            if base["pm10"] > 100:
                pm10 = max(50.0, base["pm10"] + random.uniform(-30.0, 30.0))
                
            temp = base["temp"] + random.uniform(-5.0, 5.0)
            hum = max(10, min(100, base["hum"] + random.randint(-20, 20)))
            wind_speed = max(0.0, base["wind_speed"] + random.uniform(-2.0, 6.0))
            
            # Simuliamo la pioggia casualmente nel 20% dei casi
            rain = 0.0
            if random.random() < 0.2:
                rain = random.uniform(0.5, 12.0)
                
            pressure = base["pressure"] + random.randint(-15, 15)
            hour = random.randint(0, 23)
            day_of_week = random.randint(0, 6)
            is_weekend = 1 if day_of_week >= 5 else 0
            
            # Logica applicata alla label (la stessa di producer.py)
            target_pm10 = pm10
            if rain > 0: target_pm10 *= 0.7
            if wind_speed > 5: target_pm10 *= 0.8
            if hum > 75: target_pm10 *= 1.1
            
            row = {
                'pm10': round(pm10, 2),
                'temp': round(temp, 2),
                'hum': int(hum),
                'wind_speed': round(wind_speed, 2),
                'rain': round(rain, 2),
                'pressure': int(pressure),
                'hour': hour,
                'day_of_week': day_of_week,
                'is_weekend': is_weekend,
                'label': round(target_pm10, 2)
            }
            writer.writerow(row)

print(f"✅ Generati con successo 4000 record coerenti in {CSV_PATH}")
