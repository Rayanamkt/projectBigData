# Real-Time Monitoring of Public Transport Disruptions (Île-de-France)

## Project Overview
This project implements an end-to-end Big Data pipeline for real-time monitoring of public transport disruptions in Île-de-France using the official Île-de-France Mobilités (PRIM) API.  

The pipeline collects live disruptions data and produces real-time analytics dashboards for monitoring KPIs such as disruptions per minute and severity distribution.

**Data Source:**  
- Endpoint: [IDFM Disruptions API](https://prim.iledefrance-mobilites.fr/marketplace/disruptions_bulk/disruptions/v2)  
- Format: JSON  
- Authentication: API Key  

---

## Architecture
**Pipeline Flow:**  
IDFM API → Kafka (2 brokers) → Spark Structured Streaming → InfluxDB (time-series storage) → Grafana (dashboard visualization)

**Components Overview:**  

- **Kafka:** Handles real-time ingestion of JSON messages from the IDFM API.  
- **Spark Streaming:** Processes Kafka messages and computes KPIs.  
- **InfluxDB:** Stores computed metrics as a time-series database.  
- **Grafana:** Visualizes KPIs with real-time dashboards.

---

## Infrastructure

| Node     | IP           | Services                          |
|----------|--------------|----------------------------------|
| master   | 10.0.0.116   | Hadoop / YARN / Spark Master      |
| worker1  | 10.0.0.117   | Grafana                           |
| worker2  | 10.0.0.104   | Kafka Broker                      |
| worker3  | 10.0.0.105   | InfluxDB                          |
| worker4  | 10.0.0.113   | Kafka Broker                      |
| worker5  | 10.0.0.112   | Spark Node                        |

---

## Cluster Setup Instructions

### Hadoop & YARN (on master)
```bash
start-dfs.sh
start-yarn.sh
```

### Spark (on master)
```bash
cd /opt/spark/sbin
./start-master.sh --host 10.0.0.116
```

### Spark (on each worker)
```bash
cd /opt/spark/sbin
./start-worker.sh spark://10.0.0.116:7077
```

### Kafka + Zookeeper (on worker2 and worker4)
```bash
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
bin/kafka-server-start.sh -daemon config/server.properties
```

### InfluxDB (on worker3)
```bash
./influxd &
```

### Grafana (on worker1)
```bash
./bin/grafana-server
```

### Start Kafka Producer
```bash
python3 kafka_producer_idfm.py
```

### Start Spark Streaming Job
```bash
spark-submit --master spark://10.0.0.116:7077 spark_streaming_idfm_to_influx.py
```

---

## Monitoring
Grafana dashboards visualize real-time KPIs:
- Disruptions per minute (time series)
- Distribution by severity
- Evolution of severity over time

### Accessing Grafana
- Grafana is running on **worker1 (10.0.0.117)**  
- Default port: **3000**  
- Open in browser: `http://10.0.0.117:3000`  

---

## Dependencies
- Java 11.0.29
- Hadoop 3.3.6
- Spark 3.5.7
- Kafka 2.13-3.7.2
- Python 3.12.3 
- InfluxDB 2.7.1