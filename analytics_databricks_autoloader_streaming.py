# Databricks notebook source
# Streaming ingest via Databricks Autoloader (cloudFiles) -> Delta
# Copy/paste into a Databricks notebook (Python) and run or schedule as a streaming job.

# ---------------------------------------------------------------------
# Cell: Widgets / Parameters
# ---------------------------------------------------------------------
dbutils.widgets.text("cloud_path", "/mnt/raw/amazon_sales_stream/", "Cloud storage path for new files")
dbutils.widgets.text("checkpoint_path", "/mnt/checkpoints/amazon_autoloader/", "Checkpoint path for streaming query")
dbutils.widgets.text("output_delta_path", "/mnt/delta/amazon_sales_stream/", "Delta output path")
dbutils.widgets.text("format", "csv", "Input file format (csv, json, parquet)")
dbutils.widgets.text("table_name", "default.amazon_sales_stream", "Delta table name")
cloud_path = dbutils.widgets.get("cloud_path")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
output_delta_path = dbutils.widgets.get("output_delta_path")
input_format = dbutils.widgets.get("format")
table_name = dbutils.widgets.get("table_name")

# ---------------------------------------------------------------------
# Cell: Start streaming read (Autoloader)
# ---------------------------------------------------------------------
from pyspark.sql.functions import to_timestamp, to_date
input_options = {
    "cloudFiles.format": input_format,
    "cloudFiles.inferColumnTypes": "true",
    "header": "true",
    "cloudFiles.schemaLocation": checkpoint_path + "/schema"
}
# If schema drift expected, enable "cloudFiles.schemaEvolutionMode": "addNewColumns"

raw_stream = spark.readStream.format("cloudFiles").options(**input_options).load(cloud_path)

# Normalize fields (same approach as batch)
possible_date_cols = ["order_date","order_datetime","date","sale_date","timestamp"]
possible_price_cols = ["total_price","price","sale_amount","amount","revenue"]
possible_qty_cols = ["quantity","qty","units"]

cols = raw_stream.columns
def first_existing(col_list, df_cols):
    for c in col_list:
        if c in df_cols:
            return c
    return None

date_col = first_existing(possible_date_cols, cols)
price_col = first_existing(possible_price_cols, cols)
qty_col = first_existing(possible_qty_cols, cols)

s = raw_stream
if date_col:
    s = s.withColumn("order_ts", to_timestamp(s[date_col]))
    s = s.withColumn("sale_date", to_date(s["order_ts"]))
else:
    s = s.withColumn("order_ts", F.lit(None))

if price_col:
    s = s.withColumn("total_price", s[price_col].cast("double"))
else:
    s = s.withColumn("total_price", F.lit(0.0))

if qty_col:
    s = s.withColumn("quantity", s[qty_col].cast("long"))
else:
    s = s.withColumn("quantity", F.lit(1))

# ---------------------------------------------------------------------
# Cell: Stream write to Delta with checkpointing
# ---------------------------------------------------------------------
# Use "mergeSchema" if you want schema evolution
query = s.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .option("mergeSchema", "true") \
    .outputMode("append") \
    .start(output_delta_path)

print("Streaming started with id:", query.id)
# To register to a table (idempotent)
spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{output_delta_path}'")

# Query will run until stopped. Use Jobs to keep it running, or stop() to finish:
# query.stop()