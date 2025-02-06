from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, min, max, avg, when, count, date_format
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("WindTurbineDataPipeline") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Define file paths
input_folder = "main/resources/input"
output_folder = "main/resources/output"
os.makedirs(output_folder, exist_ok=True)

# Load raw data and automatically infer column names
raw_df = spark.read.option("header", "true").csv(f"{input_folder}/*.csv")

# Dynamically get column names
column_names = raw_df.columns
print("Dynamically Extracted Column Names: ", column_names)

# Write Raw Data
raw_output_path = os.path.join(output_folder, "raw_data")
raw_df.write.mode("overwrite").option("header", "true").csv(raw_output_path)
print(f"Raw data written to {raw_output_path}")

# Data Cleaning (Silver Stage)
cleaned_df = raw_df.dropna()

# Dynamically cast columns to appropriate types (assuming certain columns need casting)
for col_name in column_names:
    if col_name in ['power_output', 'wind_speed', 'wind_direction']:  # Assuming these columns should be numeric
        cleaned_df = cleaned_df.withColumn(col_name, col(col_name).cast("double"))
    elif col_name == 'turbine_id':  # Assuming turbine_id should be int
        cleaned_df = cleaned_df.withColumn(col_name, col(col_name).cast("int"))


# Write Silver Data (Cleaned Data)
silver_output_path = os.path.join(output_folder, "silver_data")
cleaned_df.write.mode("overwrite").option("header", "true").csv(silver_output_path)
print(f"Silver data written to {silver_output_path}")

cleaned_df = cleaned_df.withColumn("date",  date_format(cleaned_df.timestamp, "yyyy-MM-dd"))
# Aggregate statistics for each turbine (Gold Stage)
gold_df = cleaned_df.groupBy("turbine_id","date").agg(
    min("power_output").alias("min_power"),
    max("power_output").alias("max_power"),
    avg("power_output").alias("avg_power"),
    stddev("power_output").alias("std_dev_power"),
    count("power_output").alias("data_count")
)

# Print debug info
print("\n DEBUG: Aggregated Data (First 10 Rows) ")
gold_df.show(10)

# Write Gold Data (Aggregated Data)
gold_output_path = os.path.join(output_folder, "gold_data")
gold_df.write.mode("overwrite").option("header", "true").csv(gold_output_path)
print(f"Gold data written to {gold_output_path}")

# Handle null std_dev_power (if any)
gold_df = gold_df.withColumn("std_dev_power", when(col("std_dev_power").isNull(), 1e-6).otherwise(col("std_dev_power")))

# Calculate anomaly bounds
gold_df = gold_df.withColumn("upper_bound", col("avg_power") + 2 * col("std_dev_power")) \
    .withColumn("lower_bound", col("avg_power") - 2 * col("std_dev_power"))

# Print calculated thresholds
print("\n DEBUG: Upper & Lower Bounds (First 10 Rows) ")
gold_df.select("turbine_id", "upper_bound", "lower_bound").show(10)

# Join to find anomalies
turbine_anomalies = cleaned_df.join(gold_df, "turbine_id") \
    .filter((col("power_output") > col("upper_bound")) | (col("power_output") < col("lower_bound")))

# Print statistics for power_output column
cleaned_df.describe("power_output").show()

# Print number of anomalies detected
print(f"\n DEBUG: Number of Anomalies Detected: {turbine_anomalies.count()} ")

# Store Anomaly Results
anomalies_output_path = os.path.join(output_folder, "anomalies_wind_turbine")
turbine_anomalies.write.mode("overwrite").option("header", "true").csv(anomalies_output_path)
print(f"Anomalies data written to {anomalies_output_path}")

print(f"Pipeline Execution Completed. All outputs saved in {output_folder}")

