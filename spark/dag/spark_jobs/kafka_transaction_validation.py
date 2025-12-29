from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, when, lit, concat_ws, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

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
    "event_ts",
    to_timestamp("timestamp")
)
timestamp_check = col("event_ts").isNotNull()

validated_df = parsed_df.withColumn(
    "error_reason",
    concat_ws(
        "; ",
        when(~mandatory_check, "MANDATORY_FIELD_MISSING"),
        when(~type_check, "INVALID_TYPE"),
        when(~range_check, "AMOUNT_OUT_OF_RANGE"),
        when(~source_check, "INVALID_SOURCE"),
        when(~timestamp_check, "INVALID_TIMESTAMP")
    )
)
validated_df = validated_df.withColumn(
    "is_valid",
    when(col("error_reason") == "", True).otherwise(False)
)
valid_df = validated_df.filter(col("is_valid") == True)
invalid_df = validated_df.filter(col("is_valid") == False)

valid_df = valid_df \
    .withWatermark("event_ts", "5 minutes") \
    .dropDuplicates(["event_id"])

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