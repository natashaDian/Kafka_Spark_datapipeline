import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sum as spark_sum

# Parse args
parser = argparse.ArgumentParser()
parser.add_argument("--process_date", required=True)
args = parser.parse_args()
process_date = args.process_date

# SparkSession
spark = (
   SparkSession.builder
   .appName(f"DailyGMV_{process_date}")
   .getOrCreate()
)

# Create dummy dataset (tanpa file)
data = [
   ("2025-01-01", 100),
   ("2025-01-01", 200),
   ("2025-01-02", 150),
   ("2025-01-02", 300)
]

df = spark.createDataFrame(data, ["order_date", "gmv"])

# Filter by date from Airflow
df_filtered = df.filter(df.order_date == lit(process_date))

# Compute GMV
gmv_df = df_filtered.agg(spark_sum("gmv").alias("gmv"))

gmv_value = gmv_df.collect()[0]["gmv"]

print(f"[INFO] GMV for {process_date}: {gmv_value}")

spark.stop()
print("Daily GMV job complete.")


