from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def run_spark_job():
    # 1. Initialize Spark Session
    # We need the Kafka package to read from Kafka
    spark = SparkSession.builder \
        .appName("FlightDelayProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    # 2. Define Schema (Must match your Producer's output)
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("AIRLINE_CODE", StringType(), True), # Updated header name
        StructField("ORIGIN", StringType(), True),
        StructField("DEST", StringType(), True),
        StructField("DEP_DELAY", DoubleType(), True),
        StructField("ARR_DELAY", DoubleType(), True),
        StructField("DISTANCE", DoubleType(), True)
    ])

    # 3. Read from Kafka
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "flight_events") \
        .option("startingOffsets", "earliest") \
        .load()

    # 4. Parse JSON
    df_parsed = df_raw.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # 5. Simple Transformation (Verification)
    # Let's filter for delays > 0 just to prove we are doing something
    df_delayed = df_parsed.filter(col("DEP_DELAY") > 0)

    # 6. Write to Console (For now, since we have no DB)
    # trigger(availableNow=True) turns this Streaming job into a Batch job!
    query = df_delayed.writeStream \
        .outputMode("append") \
        .format("console") \
        .trigger(availableNow=True) \
        .start()

    query.awaitTermination()
    spark.stop()

if __name__ == "__main__":
    run_spark_job()