from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType

# 1) Sesión Spark
spark = (
    SparkSession.builder
    .appName("SensorDataAnalysis")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 2) Lector Kafka
df_k = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sensor_data")
    .load()
)

# 3) Esquema del JSON que envía tu producer
schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("timestamp", TimestampType(), True)
])

# 4) Parseo: value -> JSON -> columnas tipadas
parsed = (
    df_k.select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
)

# 5) Agregación por ventana de 1 minuto + sensor
windowed_stats = (
    parsed.groupBy(
        window(col("timestamp"), "1 minute"),
        col("sensor_id")
    )
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    )
)

# 6) Sink en consola
query = (
    windowed_stats.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .start()
)

query.awaitTermination()
