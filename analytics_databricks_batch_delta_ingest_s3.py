# Databricks notebook source
# Batch ingestion -> Delta on S3 (works with s3:// or s3a:// paths)
# Copy/paste cells into a Databricks Python notebook.

# ---------------------------------------------------------------------
# Cell: Widgets / Parameters
# ---------------------------------------------------------------------
dbutils.widgets.text("input_path", "s3://my-bucket/raw/amazon_sales/", "Input raw S3 path (s3://bucket/path/)")
dbutils.widgets.text("output_delta_path", "s3://my-bucket/delta/amazon_sales/", "Delta output S3 path (s3://bucket/path/)")
dbutils.widgets.text("delta_table_name", "default.amazon_sales_delta", "Delta table identifier (catalog.schema.table)")
dbutils.widgets.text("partition_by", "sale_date", "Partition column (or empty)")
dbutils.widgets.text("sample_rows_for_schema", "1000", "Rows to sample when inferring schema")
dbutils.widgets.text("overwrite_mode", "false", "If true, overwrite existing table")
dbutils.widgets.text("aws_secret_scope", "", "Databricks secret scope name if using AWS keys (leave empty to use instance profile)")
dbutils.widgets.text("aws_access_key_secret", "", "Secret key name in scope for AWS access key id")
dbutils.widgets.text("aws_secret_key_secret", "", "Secret key name in scope for AWS secret access key")
dbutils.widgets.text("aws_session_token_secret", "", "Secret key name in scope for AWS session token (optional)")
input_path = dbutils.widgets.get("input_path")
output_delta_path = dbutils.widgets.get("output_delta_path")
delta_table_name = dbutils.widgets.get("delta_table_name")
partition_by = dbutils.widgets.get("partition_by")
sample_rows_for_schema = int(dbutils.widgets.get("sample_rows_for_schema"))
overwrite_mode = dbutils.widgets.get("overwrite_mode").lower() == "true"
aws_secret_scope = dbutils.widgets.get("aws_secret_scope")
aws_access_key_secret = dbutils.widgets.get("aws_access_key_secret")
aws_secret_key_secret = dbutils.widgets.get("aws_secret_key_secret")
aws_session_token_secret = dbutils.widgets.get("aws_session_token_secret")

# ---------------------------------------------------------------------
# Cell: AWS credential handling (preferred: instance profile; fallback: secrets)
# ---------------------------------------------------------------------
# Recommended: attach an IAM role / instance profile to the cluster with S3 read/write permissions.
# If you must use static keys, store them in a Databricks Secret Scope and set aws_secret_scope + secret names above.

def configure_s3_from_secrets(scope, access_key_secret, secret_key_secret, session_token_secret=None):
    if not scope or not access_key_secret or not secret_key_secret:
        return False
    try:
        ak = dbutils.secrets.get(scope=scope, key=access_key_secret)
        sk = dbutils.secrets.get(scope=scope, key=secret_key_secret)
        sc = dbutils.secrets.get(scope=scope, key=session_token_secret) if session_token_secret else None
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        # Use s3a settings
        hadoop_conf.set("fs.s3a.access.key", ak)
        hadoop_conf.set("fs.s3a.secret.key", sk)
        if sc:
            hadoop_conf.set("fs.s3a.session.token", sc)
        # Optional: set implementation and endpoint if required
        # hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        return True
    except Exception as e:
        print("Failed to configure S3 using secrets:", e)
        return False

used_secrets = False
if aws_secret_scope:
    used_secrets = configure_s3_from_secrets(aws_secret_scope, aws_access_key_secret, aws_secret_key_secret, aws_session_token_secret)
    if used_secrets:
        print("Configured S3 access via Databricks secret scope.")
    else:
        print("Secret-based S3 configuration not applied; ensure secrets exist and names are correct or attach an IAM role to the cluster.")

# ---------------------------------------------------------------------
# Cell: Infer schema (sample) and read data
# ---------------------------------------------------------------------
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType, DateType
from pyspark.sql import functions as F
import os

print("Listing input path (may fail if permissions missing):", input_path)
try:
    display(dbutils.fs.ls(input_path))
except Exception as e:
    print("Could not list path via dbutils.fs (still OK if using s3a with IAM role). Error:", e)

# Sample small subset to inspect schema (inferSchema)
sample_df = spark.read.option("header", True).option("inferSchema", True).csv(input_path).limit(sample_rows_for_schema)
print("Sample schema:")
sample_df.printSchema()

# Read entire dataset with inferred schema (or swap to a curated schema for production)
df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
print("Full read columns:", df.columns)

# ---------------------------------------------------------------------
# Cell: Canonicalize / normalize columns (robust)
# ---------------------------------------------------------------------
possible_date_cols = ["order_date","order_datetime","date","sale_date","timestamp"]
possible_price_cols = ["total_price","price","sale_amount","amount","revenue"]
possible_qty_cols = ["quantity","qty","units","order_quantity"]
possible_product_cols = ["product_id","asin","sku","product","product_title"]
possible_category_cols = ["category","product_category","category_name","department"]
possible_seller_cols = ["seller_id","seller","merchant","seller_name"]

def first_existing(col_list, df_cols):
    for c in col_list:
        if c in df_cols:
            return c
    return None

cols = df.columns
date_col = first_existing(possible_date_cols, cols)
price_col = first_existing(possible_price_cols, cols)
qty_col = first_existing(possible_qty_cols, cols)
product_col = first_existing(possible_product_cols, cols)
category_col = first_existing(possible_category_cols, cols)
seller_col = first_existing(possible_seller_cols, cols)

print("Detected columns:", {"date_col":date_col, "price_col":price_col, "qty_col":qty_col, "product_col":product_col, "category_col":category_col, "seller_col":seller_col})

# Normalize types and create canonical columns
if date_col:
    df = df.withColumn("order_ts", F.to_timestamp(F.col(date_col)))
    df = df.withColumn("sale_date", F.to_date(F.col("order_ts")))
else:
    df = df.withColumn("order_ts", F.lit(None).cast(TimestampType())).withColumn("sale_date", F.lit(None).cast(DateType()))

if price_col:
    df = df.withColumn("total_price", F.col(price_col).cast("double"))
else:
    unit_price_col = first_existing(["unit_price","price"], cols)
    if unit_price_col and qty_col:
        df = df.withColumn("total_price", (F.col(unit_price_col).cast("double") * F.col(qty_col).cast("double")))
    else:
        df = df.withColumn("total_price", F.lit(0.0))

if qty_col:
    df = df.withColumn("quantity", F.col(qty_col).cast("long"))
else:
    df = df.withColumn("quantity", F.lit(1))

if product_col:
    df = df.withColumn("product_id_canon", F.col(product_col).cast("string"))
else:
    df = df.withColumn("product_id_canon", F.lit(None).cast(StringType()))

if category_col:
    df = df.withColumn("category_canon", F.col(category_col).cast("string"))
else:
    df = df.withColumn("category_canon", F.lit("unknown"))

if seller_col:
    df = df.withColumn("seller_canon", F.col(seller_col).cast("string"))
else:
    df = df.withColumn("seller_canon", F.lit(None).cast(StringType()))

# ---------------------------------------------------------------------
# Cell: Write to Delta on S3 and register table
# ---------------------------------------------------------------------
from delta.tables import DeltaTable

write_mode = "overwrite" if overwrite_mode else "append"
partition_cols = [p for p in [partition_by] if p and p in df.columns]

print("Writing to Delta at:", output_delta_path, "partition_cols:", partition_cols, "mode:", write_mode)

if write_mode == "overwrite":
    df.write.format("delta").mode("overwrite").partitionBy(*partition_cols).option("overwriteSchema", "true").save(output_delta_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {delta_table_name} USING DELTA LOCATION '{output_delta_path}'")
else:
    df.write.format("delta").mode("append").partitionBy(*partition_cols).save(output_delta_path)
    try:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {delta_table_name} USING DELTA LOCATION '{output_delta_path}'")
    except Exception as e:
        print("Table create-check warning:", e)

# ---------------------------------------------------------------------
# Cell: Optional OPTIMIZE / ZORDER (Databricks runtime only)
# ---------------------------------------------------------------------
try:
    if partition_cols:
        spark.sql(f"OPTIMIZE {delta_table_name} ZORDER BY (product_id_canon, seller_canon)")
    else:
        spark.sql(f"OPTIMIZE {delta_table_name}")
    print("OPTIMIZE completed.")
except Exception as e:
    print("OPTIMIZE skipped or failed (requires Databricks runtime with OPTIMIZE):", e)

# ---------------------------------------------------------------------
# Cell: Aggregations & write aggregates to Delta / Parquet on S3
# ---------------------------------------------------------------------
agg_daily = df.groupBy("sale_date").agg(
    F.sum("total_price").alias("revenue"),
    F.sum("quantity").alias("units_sold"),
    F.countDistinct(F.col("order_id")).alias("orders")
).orderBy("sale_date")

display(agg_daily.limit(50))
agg_daily.write.mode("overwrite").parquet(os.path.join(output_delta_path.rstrip("/"), "aggregates", "daily"))

top_products = df.groupBy("product_id_canon").agg(F.sum("total_price").alias("revenue")).orderBy(F.desc("revenue")).limit(100)
display(top_products)
top_products.write.mode("overwrite").parquet(os.path.join(output_delta_path.rstrip("/"), "aggregates", "top_products"))

top_cats = df.groupBy("category_canon").agg(F.sum("total_price").alias("revenue")).orderBy(F.desc("revenue"))
top_cats.write.mode("overwrite").parquet(os.path.join(output_delta_path.rstrip("/"), "aggregates", "top_categories"))

# ---------------------------------------------------------------------
# Cell: Vacuum caution (do not run blindly in prod)
# ---------------------------------------------------------------------
# from delta.tables import DeltaTable
# delta = DeltaTable.forPath(spark, output_delta_path)
# delta.vacuum(retentionHours=168)  # requires retentionDurationCheck override if < 7 days