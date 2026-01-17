#!/usr/bin/env python3
import requests
import json
import os
import time
from kafka import KafkaProducer
from datetime import datetime, timezone

# === Configuration ===
KAFKA_TOPIC = "idf_disruptions"
KAFKA_BROKER = ["worker2:9092", "worker4:9092"]
IDFM_API_URL = "https://prim.iledefrance-mobilites.fr/marketplace/disruptions_bulk/disruptions/v2"
IDFM_API_KEY = os.environ.get("PRIM_API_KEY")  

# === Initialisation Kafka ===
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
def is_active(disruption):
    """Retourne True si la perturbation est en cours maintenant"""
    now = datetime.now(timezone.utc)
    for period in disruption.get("applicationPeriods", []):
        # conversion ISO ou format IDFM : 20251230T024600
        begin = datetime.strptime(period["begin"], "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
        end = datetime.strptime(period["end"], "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
        if begin <= now <= end:
            return True
    return False

def fetch_disruptions():
    headers = {"apikey": IDFM_API_KEY}
    response = requests.get(IDFM_API_URL, headers=headers)
    if response.status_code != 200:
        print(f"[Erreur] API IDFM {response.status_code}")
        return []
    data = response.json()
    disruptions = data.get("disruptions", [])
    # Ne garder que celles qui sont actives
    active = [d for d in disruptions if is_active(d)]
    return active

def main_loop():
    print("[INFO] Producer IDFM démarré, envoi des perturbations toutes les minutes...")
    while True:
        try:
            active_disruptions = fetch_disruptions()
            print(f"[INFO] {len(active_disruptions)} perturbations actives récupérées")
            
            for disruption in active_disruptions:
                producer.send(KAFKA_TOPIC, disruption)
            producer.flush()
            print(f"[OK] Perturbations envoyées vers Kafka ({len(active_disruptions)})")
            
            time.sleep(60)  # pause 60 secondes
        except KeyboardInterrupt:
            print("[INFO] Arrêt du producer...")
            break
        except Exception as e:
            print("[Erreur]", e)
            time.sleep(30)  # attendre 30s avant de réessayer en cas d'erreur
            
if __name__ == "__main__":
    main_loop()
