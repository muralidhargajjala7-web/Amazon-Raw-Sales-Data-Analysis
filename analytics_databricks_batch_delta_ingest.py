# Databricks notebook source
# Batch ingestion + Delta table creation for Amazon Raw Sales data
# Cell boundaries are simple comments — copy/paste each cell into a Databricks Python notebook cell.

# ---------------------------------------------------------------------
# Cell: Parameters (use dbutils.widgets in Databricks to parameterize jobs)
# ---------------------------------------------------------------------
dbutils.widgets.text("input_path", "/mnt/raw/amazon_sales/", "Input raw path (CSV or folder)")
dbutils.widgets.text("output_delta_path", "/mnt/delta/amazon_sales/", "Delta output path")
dbutils.widgets.text("delta_table_name", "default.amazon_sales_delta", "Delta table identifier (catalog.schema.table)")
dbutils.widgets.text("partition_by", "sale_date", "Partition column (or empty)")
dbutils.widgets.text("sample_rows_for_schema", "1000", "Rows to sample when inferring schema")
dbutils.widgets.text("overwrite_mode", "false", "If true, overwrite existing table")
input_path = dbutils.widgets.get("input_path")
output_delta_path = dbutils.widgets.get("output_delta_path")
delta_table_name = dbutils.widgets.get("delta_table_name")
partition_by = dbutils.widgets.get("partition_by")
sample_rows_for_schema = int(dbutils.widgets.get("sample_rows_for_schema"))
overwrite_mode = dbutils.widgets.get("overwrite_mode").lower() == "true"

# ---------------------------------------------------------------------
# Cell: Optional - mount storage (example for Azure ADLS / AWS S3). Skip if already mounted.
# ---------------------------------------------------------------------
# Example (Azure ADLS) - requires secret scope and service principal setup:
# storage_account = "mystorage"
# container = "raw"
# mount_point = "/mnt/raw"
# if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
#     dbutils.fs.mount(
#         source = f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
#         mount_point = mount_point,
#         extra_configs = {"fs.azure.account.auth.type": "OAuth", ...}
#     )

# ---------------------------------------------------------------------
# Cell: Infer schema (safe approach) or accept user-provided schema
# ---------------------------------------------------------------------
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType, DateType
from pyspark.sql import functions as F

# Helper to sample and infer
sample_df = spark.read.option("header", True).option("inferSchema", True).csv(input_path).limit(sample_rows_for_schema)
print("Sample schema:")
sample_df.printSchema()

# You can optionally replace the inferred schema with a curated one to avoid surprises:
# curated_schema = StructType([
#     StructField("order_id", StringType(), True),
#     StructField("order_date", TimestampType(), True),
#     StructField("customer_id", StringType(), True),
#     StructField("product_id", StringType(), True),
#     StructField("asin", StringType(), True),
#     StructField("quantity", LongType(), True),
#     StructField("total_price", DoubleType(), True),
#     StructField("category", StringType(), True),
# ])
# df = spark.read.schema(curated_schema).option("header", True).csv(input_path)

# For safety, read with inferred schema but coerce columns later:
df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

# ---------------------------------------------------------------------
# Cell: Clean/normalize columns (robust to slightly differing schemas)
# ---------------------------------------------------------------------
# Frequent canonical names used in the scripts:
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

# Coerce types and create canonical names
if date_col:
    df = df.withColumn("order_ts", F.to_timestamp(F.col(date_col)))
    df = df.withColumn("sale_date", F.to_date(F.col("order_ts")))
else:
    df = df.withColumn("order_ts", F.lit(None).cast(TimestampType())).withColumn("sale_date", F.lit(None).cast(DateType()))

if price_col:
    df = df.withColumn("total_price", F.col(price_col).cast("double"))
else:
    # try compute price via unit price * qty
    unit_price_col = first_existing(["unit_price","price"], cols)
    if unit_price_col and qty_col:
        df = df.withColumn("total_price", (F.col(unit_price_col).cast("double") * F.col(qty_col).cast("double")))
    else:
        df = df.withColumn("total_price", F.lit(0.0))

if qty_col:
    df = df.withColumn("quantity", F.col(qty_col).cast("long"))
else:
    df = df.withColumn("quantity", F.lit(1))

# Choose product/category/seller canonical cols if present
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

# Drop problematic nested/complex columns early if any
# df = df.select([c for c in df.columns if not c.startswith("_")])

# ---------------------------------------------------------------------
# Cell: Write to Delta with partitioning and create table
# ---------------------------------------------------------------------
from delta.tables import DeltaTable
import os

# Create output path and write mode
write_mode = "overwrite" if overwrite_mode else "append"
partition_cols = [p for p in [partition_by] if p and p in df.columns]  # only partition if exists

print("Writing to Delta at:", output_delta_path, "partition_cols:", partition_cols, "mode:", write_mode)

if write_mode == "overwrite":
    # Overwrite the directory/table safely
    df.write.format("delta").mode("overwrite").partitionBy(*partition_cols).option("overwriteSchema", "true").save(output_delta_path)
    # register table
    spark.sql(f"CREATE TABLE IF NOT EXISTS {delta_table_name} USING DELTA LOCATION '{output_delta_path}'")
else:
    # Append
    df.write.format("delta").mode("append").partitionBy(*partition_cols).save(output_delta_path)
    # Create table if does not exist
    try:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {delta_table_name} USING DELTA LOCATION '{output_delta_path}'")
    except Exception as e:
        print("Table create-check warning:", e)

# ---------------------------------------------------------------------
# Cell: Optimize & ZORDER recommendations (run if Delta cache / OPTIMIZE supported)
# ---------------------------------------------------------------------
# If using Databricks runtime with Delta Lake optimizations:
try:
    if partition_cols:
        spark.sql(f"OPTIMIZE {delta_table_name} ZORDER BY ({', '.join(['seller_canon','product_id_canon'])})")
    else:
        spark.sql(f"OPTIMIZE {delta_table_name}")
    print("OPTIMIZE completed.")
except Exception as e:
    print("OPTIMIZE skipped or failed (may require Databricks Runtime):", e)

# ---------------------------------------------------------------------
# Cell: Basic analytics & saving aggregated outputs
# ---------------------------------------------------------------------
# Create daily aggregates and save as Parquet/Delta for downstream consumption
agg_daily = df.groupBy("sale_date").agg(
    F.sum("total_price").alias("revenue"),
    F.sum("quantity").alias("units_sold"),
    F.countDistinct("order_id").alias("orders")
).orderBy("sale_date")

# Show sample and write
display(agg_daily.limit(50))
agg_daily.write.mode("overwrite").option("header", True).parquet(os.path.join(output_delta_path, "aggregates", "daily"))

# Top products
top_products = df.groupBy("product_id_canon").agg(F.sum("total_price").alias("revenue")).orderBy(F.desc("revenue")).limit(100)
display(top_products)
top_products.write.mode("overwrite").parquet(os.path.join(output_delta_path, "aggregates", "top_products"))

# Top categories
top_cats = df.groupBy("category_canon").agg(F.sum("total_price").alias("revenue")).orderBy(F.desc("revenue"))
top_cats.write.mode("overwrite").parquet(os.path.join(output_delta_path, "aggregates", "top_categories"))

# ---------------------------------------------------------------------
# Cell: Vacuum and retention (careful in prod)
# ---------------------------------------------------------------------
# Only run vacuum if you understand retention implications. Example:
# from delta.tables import DeltaTable
# delta = DeltaTable.forPath(spark, output_delta_path)
# delta.vacuum(retentionHours=168)  # 7 days; requires spark.databricks.delta.retentionDurationCheck.enabled=false if < 7 days