from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, count
from pyspark.sql.types import StructType, StringType
import os

# -------------------------
# Config InfluxDB (v2)
# -------------------------
INFLUX_URL = "http://worker3:8086"
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN")
INFLUX_ORG = os.environ.get("INFLUX_ORG", "BigData")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "BigData")
if not INFLUX_TOKEN:
    raise RuntimeError("INFLUX_TOKEN env var is missing")

# -------------------------
# Schéma JSON
# -------------------------
schema = (
    StructType()
    .add("title", StringType())
    .add("last_update", StringType())
    .add("severity", StringType())
)
spark = (
    SparkSession.builder
    .appName("IDFM-Spark-Streaming-To-Influx")
    .master("spark://10.0.0.116:7077")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# -------------------------
# Lecture Kafka
# -------------------------
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "worker2:9092,worker4:9092")
    .option("subscribe", "idf_disruptions")
    .option("startingOffsets", "latest")
    .load()
)
json_df = raw_df.selectExpr("CAST(value AS STRING) AS json")
parsed = json_df.select(from_json(col("json"), schema).alias("d")).select("d.*")
df = parsed.withColumn("event_time", to_timestamp(col("last_update")))
df_valid = df.filter(col("event_time").isNotNull())

# -------------------------
# KPIs
# -------------------------
events_per_min = (
    df_valid
    .groupBy(window(col("event_time"), "1 minute"))
    .agg(count("*").alias("events_per_min"))
)
severity_per_min = (
    df_valid
    .groupBy(window(col("event_time"), "1 minute"), col("severity"))
    .agg(count("*").alias("count"))
)

# -------------------------
# Influx writer
# -------------------------
def write_to_influx(batch_df, measurement, tag_cols, field_cols):
    import requests
    rows = batch_df.collect()
    if not rows:
        return
    lines = []
    for r in rows:
        ts = r["window"]["end"]
        ts_ns = int(ts.timestamp() * 1e9)
        tags = []
        for t in tag_cols:
            v = r[t]
            if v is not None:
                tags.append(f"{t}={str(v).replace(' ', r'\ ')}")
        fields = []
        for f in field_cols:
            val = r[f]
            if val is None:
                continue
            if isinstance(val, int):
                fields.append(f"{f}={val}i")
            else:
                fields.append(f"{f}={val}")
        if not fields:
            continue
        tag_str = "," + ",".join(tags) if tags else ""
        field_str = ",".join(fields)
        lines.append(f"{measurement}{tag_str} {field_str} {ts_ns}")
    payload = "\n".join(lines)
    url = f"{INFLUX_URL}/api/v2/write?org={INFLUX_ORG}&bucket={INFLUX_BUCKET}&precision=ns"
    headers = {
        "Authorization": f"Token {INFLUX_TOKEN}",
        "Content-Type": "text/plain",
    }
    resp = requests.post(url, data=payload.encode(), headers=headers, timeout=10)
    if resp.status_code >= 300:
        print("Influx error:", resp.status_code, resp.text)

def foreach_events(batch_df, epoch_id):
    write_to_influx(batch_df, "spark_kpi", [], ["events_per_min"])

def foreach_severity(batch_df, epoch_id):
    write_to_influx(batch_df, "spark_severity", ["severity"], ["count"])

# -------------------------
# Streams
# -------------------------
q1 = (
    events_per_min.writeStream
    .outputMode("update")
    .foreachBatch(foreach_events)
    .option("checkpointLocation", "file:/tmp/spark_ckpt_events")
    .start()
)
q2 = (
    severity_per_min.writeStream
    .outputMode("update")
    .foreachBatch(foreach_severity)
    .option("checkpointLocation", "file:/tmp/spark_ckpt_severity")
    .start()
)
spark.streams.awaitAnyTermination()