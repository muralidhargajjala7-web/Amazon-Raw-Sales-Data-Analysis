```python
#!/usr/bin/env python3
"""
PySpark analytics for large Amazon sales CSVs.
Run with: spark-submit analytics/spark_analysis.py --input s3://bucket/sales.csv --output /tmp/spark_reports
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, to_date, date_format, desc, row_number
from pyspark.sql.window import Window

def main(input_path, output_path, date_col, price_col, qty_col):
    spark = SparkSession.builder.appName("amazon_sales_analysis").getOrCreate()
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
    print("Loaded data with columns:", df.columns)

    # basic cleansing: cast price/qty to double/long
    if price_col in df.columns:
        df = df.withColumn("price_num", col(price_col).cast("double"))
    else:
        df = df.withColumn("price_num", col(price_col))  # will be null

    if qty_col in df.columns:
        df = df.withColumn("qty_num", col(qty_col).cast("long"))
    else:
        df = df.withColumn("qty_num", col(qty_col))

    if date_col in df.columns:
        df = df.withColumn("date", to_date(col(date_col)))
    else:
        df = df.withColumn("date", to_date(col(date_col)))  # may be null

    # daily revenue
    daily = df.groupBy("date").agg(_sum("price_num").alias("revenue"), _sum("qty_num").alias("units_sold"))
    daily.orderBy("date").write.mode("overwrite").csv(output_path + "/daily_revenue", header=True)

    # top sellers
    seller_col = None
    for c in ["seller_id","seller","merchant","seller_name"]:
        if c in df.columns:
            seller_col = c
            break
    if seller_col:
        top_sellers = df.groupBy(seller_col).agg(_sum("price_num").alias("revenue")).orderBy(desc("revenue")).limit(100)
        top_sellers.write.mode("overwrite").csv(output_path + "/top_sellers", header=True)

    # top products
    product_col = None
    for c in ["product_id","asin","sku","product_title","product"]:
        if c in df.columns:
            product_col = c
            break
    if product_col:
        top_products = df.groupBy(product_col).agg(_sum("price_num").alias("revenue")).orderBy(desc("revenue")).limit(200)
        top_products.write.mode("overwrite").csv(output_path + "/top_products", header=True)

    # rolling 7-day avg revenue (window)
    w = Window.orderBy("date").rowsBetween(-6, 0)
    if "revenue" in daily.columns:
        from pyspark.sql.functions import avg
        daily_with_rolling = daily.withColumn("rolling_7d_revenue", avg("revenue").over(w))
        daily_with_rolling.write.mode("overwrite").csv(output_path + "/daily_with_rolling", header=True)

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--datecol", default="order_date")
    parser.add_argument("--pricecol", default="total_price")
    parser.add_argument("--qtycol", default="quantity")
    args = parser.parse_args()
    main(args.input, args.output, args.datecol, args.pricecol, args.qtycol)