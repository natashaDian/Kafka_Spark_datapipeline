from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, when, lit, concat_ws, to_timestamp, current_timestamp, window, sum as _sum, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)
from datetime import datetime

spark = SparkSession.builder \
    .appName("KafkaTransactionValidation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

schema = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True)
])

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 1. Mandatory field check
mandatory_check = (
    col("user_id").isNotNull() &
    col("amount").isNotNull() &
    col("timestamp").isNotNull()
)

# 2. Type validation
type_check = (
    col("amount").cast("int").isNotNull()
)

# 3. Range validation
range_check = col("amount").between(1, 10_000_000)

# 4. Source validation
source_check = col("source").isin("mobile", "web", "pos")

parsed_df = parsed_df.withColumn(
    "event_time",
    to_timestamp("timestamp")   
)
timestamp_check = col("event_time").isNotNull()

processing_time = current_timestamp()
late_check = col("event_time") < (processing_time - expr("INTERVAL 3 MINUTES"))

validated_df = parsed_df.withColumn(
    "error_reason",
    concat_ws(
        "; ",
        when(~mandatory_check, "MANDATORY_FIELD_MISSING"),
        when(~type_check, "INVALID_TYPE"),
        when(~range_check, "AMOUNT_OUT_OF_RANGE"),
        when(~source_check, "INVALID_SOURCE"),
        when(~timestamp_check, "INVALID_TIMESTAMP"),
        when(late_check, "LATE_EVENT_GT_3_MIN")
    )
)
validated_df = validated_df.withColumn(
    "is_valid",
    when(col("error_reason") == "", True).otherwise(False)
)
valid_df = validated_df.filter(col("is_valid") == True)
invalid_df = validated_df.filter(col("is_valid") == False)

valid_df = valid_df \
    .withWatermark("event_time", "3 minutes") \
    .dropDuplicates(["event_id"])

windowed_df = valid_df.groupBy(
    window(col("event_time"), "1 minute")
).agg(
    _sum(lit(1)).alias("txn_count")
)

running_total = {"count": 0}

def write_console(batch_df, batch_id):
    global running_total

    if batch_df.rdd.isEmpty():
        return   # <-- INI PENTING

    batch_count = batch_df.agg(
        _sum("txn_count").alias("c")
    ).collect()[0]["c"] or 0

    running_total["count"] += int(batch_count)

    out = spark.createDataFrame(
        [(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
          running_total["count"])],
        ["timestamp", "running_total"]
    )

    out.show(truncate=False)

monitor_query = windowed_df.writeStream \
    .foreachBatch(write_console) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoint_monitor") \
    .trigger(processingTime="5 seconds") \
    .start()


valid_query = valid_df.selectExpr(
    "CAST(user_id AS STRING) AS key",
    "to_json(struct(*)) AS value"
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092") \
    .option("topic", "transactions_valid") \
    .option("checkpointLocation", "/tmp/checkpoint_valid") \
    .trigger(processingTime="5 seconds") \
    .start()

invalid_query = invalid_df.selectExpr(
    "CAST(user_id AS STRING) AS key",
    "to_json(struct(*)) AS value"
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092") \
    .option("topic", "transactions_dlq") \
    .option("checkpointLocation", "/tmp/checkpoint_dlq") \
    .trigger(processingTime="5 seconds") \
    .start()

spark.streams.awaitAnyTermination()