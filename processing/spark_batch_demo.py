from pyspark.sql import SparkSession

# Création de la session Spark
spark = SparkSession.builder \
    .appName("IDFM-Batch-Demo") \
    .master("spark://10.0.0.116:7077") \
    .getOrCreate()

# Lecture d'un petit fichier JSON local
df = spark.read.json("file:///home/adm-mcsc/sample_disruptions.json")

# Analyse simple : compter par sévérité
result = df.groupBy("severity").count()

# Affichage
result.show(truncate=False)
spark.stop()
