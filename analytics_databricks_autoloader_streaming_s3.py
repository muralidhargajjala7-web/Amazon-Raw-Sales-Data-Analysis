# Databricks notebook source
# Autoloader (cloudFiles) streaming ingest -> Delta on S3
# Copy/paste into a Databricks Python notebook and start as a streaming job.

# ---------------------------------------------------------------------
# Cell: Widgets / Parameters
# ---------------------------------------------------------------------
dbutils.widgets.text("cloud_path", "s3://my-bucket/raw/amazon_sales_stream/", "S3 path for incoming files")
dbutils.widgets.text("checkpoint_path", "s3://my-bucket/checkpoints/amazon_autoloader/", "S3 path for checkpointing")
dbutils.widgets.text("output_delta_path", "s3://my-bucket/delta/amazon_sales_stream/", "S3 path for Delta output")
dbutils.widgets.text("input_format", "csv", "Input format (csv,json,parquet)")
dbutils.widgets.text("aws_secret_scope", "", "Databricks secret scope (leave empty to use instance profile)")
dbutils.widgets.text("aws_access_key_secret", "", "Secret name for AWS access key id")
dbutils.widgets.text("aws_secret_key_secret", "", "Secret name for AWS secret key")
cloud_path = dbutils.widgets.get("cloud_path")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
output_delta_path = dbutils.widgets.get("output_delta_path")
input_format = dbutils.widgets.get("input_format")
aws_secret_scope = dbutils.widgets.get("aws_secret_scope")
aws_access_key_secret = dbutils.widgets.get("aws_access_key_secret")
aws_secret_key_secret = dbutils.widgets.get("aws_secret_key_secret")

# ---------------------------------------------------------------------
# Cell: Optional S3 secret config (if not using instance profile)
# ---------------------------------------------------------------------
def configure_s3_from_secrets(scope, access_key_secret, secret_key_secret):
    if not scope:
        return False
    try:
        ak = dbutils.secrets.get(scope=scope, key=access_key_secret)
        sk = dbutils.secrets.get(scope=scope, key=secret_key_secret)
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.access.key", ak)
        hadoop_conf.set("fs.s3a.secret.key", sk)
        return True
    except Exception as e:
        print("S3 secret config failed:", e)
        return False

if aws_secret_scope:
    configured = configure_s3_from_secrets(aws_secret_scope, aws_access_key_secret, aws_secret_key_secret)
    if configured:
        print("S3 configured via secrets.")
    else:
        print("S3 secrets config not applied.")

# ---------------------------------------------------------------------
# Cell: Start Autoloader streaming read
# ---------------------------------------------------------------------
from pyspark.sql.functions import to_timestamp, to_date
input_options = {
    "cloudFiles.format": input_format,
    "cloudFiles.inferColumnTypes": "true",
    "header": "true",
    "cloudFiles.schemaLocation": checkpoint_path.rstrip("/") + "/schema"
}
# Enable schema evolution if you expect drift:
# input_options["cloudFiles.schemaEvolutionMode"] = "addNewColumns"

raw_stream = spark.readStream.format("cloudFiles").options(**input_options).load(cloud_path)

# ---------------------------------------------------------------------
# Cell: Normalize fields (same pattern as batch)
# ---------------------------------------------------------------------
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
query = s.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .option("mergeSchema", "true") \
    .outputMode("append") \
    .start(output_delta_path)

print("Streaming started with id:", query.id)
# Register table (idempotent)
table_name = "default.amazon_sales_stream"
spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{output_delta_path}'")

# To stop stream: query.stop()