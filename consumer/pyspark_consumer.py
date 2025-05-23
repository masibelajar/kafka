from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("GudangMonitor") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "1") \
    .getOrCreate()

# Suhu schema
suhu_schema = StructType() \
    .add("gudang_id", StringType()) \
    .add("suhu", IntegerType())

# Kelembaban schema
kelembaban_schema = StructType() \
    .add("gudang_id", StringType()) \
    .add("kelembaban", IntegerType())

# Stream suhu
suhu_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .load()

suhu_data = suhu_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), suhu_schema).alias("data")) \
    .select("data.*") \
    .withWatermark("timestamp", "10 seconds") \
    .withColumn("timestamp", expr("current_timestamp()"))

# Stream kelembaban
kelembaban_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .load()

kelembaban_data = kelembaban_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), kelembaban_schema).alias("data")) \
    .select("data.*") \
    .withWatermark("timestamp", "10 seconds") \
    .withColumn("timestamp", expr("current_timestamp()"))

# Filter suhu tinggi
suhu_alert = suhu_data.filter(col("suhu") > 80)
kelembaban_alert = kelembaban_data.filter(col("kelembaban") > 70)

# Gabungan suhu dan kelembaban dalam 10s window
combined = suhu_data.join(
    kelembaban_data,
    expr("""
        gudang_id = gudang_id AND
        suhu_data.timestamp BETWEEN kelembaban_data.timestamp - interval 10 seconds AND kelembaban_data.timestamp + interval 10 seconds
    """)
)

# Gabungan akhir dengan status
status = combined.select(
    col("suhu_data.gudang_id").alias("gudang"),
    col("suhu").alias("suhu"),
    col("kelembaban"),
    expr("""
        CASE
            WHEN suhu > 80 AND kelembaban > 70 THEN 'Bahaya tinggi! Barang berisiko rusak'
            WHEN suhu > 80 THEN 'Suhu tinggi, kelembaban normal'
            WHEN kelembaban > 70 THEN 'Kelembaban tinggi, suhu aman'
            ELSE 'Aman'
        END AS status
    """)
)

query1 = suhu_alert.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query2 = kelembaban_alert.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query3 = status.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

spark.streams.awaitAnyTermination()
