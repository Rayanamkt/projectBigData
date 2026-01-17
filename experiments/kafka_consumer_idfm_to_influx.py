#!/usr/bin/env python3
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
import json
import os

# === Configuration ===
KAFKA_TOPIC = "idf_disruptions"
KAFKA_BROKER = ["worker2:9092", "worker4:9092"]
INFLUX_URL = "http://worker3:8086"
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN") 
INFLUX_ORG = os.environ.get("INFLUX_ORG")    
INFLUX_BUCKET = "BigData"

# === InfluxDB ===
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api()

# === Kafka ===
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id="bigdata-consumer"
)
print("[INFO] Consumer démarré...")

for msg in consumer:
    disruption = msg.value
    disruption_id = disruption.get("id", "")
    title = disruption.get("title", "")
    message = disruption.get("message", "")
    cause = disruption.get("cause", "")
    severity = disruption.get("severity", "")
    last_update = disruption.get("lastUpdate", "")
    impacted_sections = disruption.get("impactedSections", [])
    # Rassembler toutes les lignes et stops impactés
    lines = []
    stops = []
    for section in impacted_sections:
        line_id = section.get("lineId", "")
        from_stop = section.get("from", {}).get("name", "")
        to_stop = section.get("to", {}).get("name", "")
        if line_id and line_id not in lines:
            lines.append(line_id)
        if from_stop and from_stop not in stops:
            stops.append(from_stop)
        if to_stop and to_stop not in stops:
            stops.append(to_stop)
    # Créer un point unique pour la disruption
    point = Point("idf_disruptions") \
        .tag("disruption_id", disruption_id) \
        .field("title", title) \
        .field("message", message) \
        .field("cause", cause) \
        .field("severity", severity) \
        .field("lines", json.dumps(lines)) \
        .field("stops", json.dumps(stops)) \
        .field("last_update", last_update)
    write_api.write(bucket=INFLUX_BUCKET, record=point)
    print(f"[OK] Perturbation envoyée ({disruption_id})")
