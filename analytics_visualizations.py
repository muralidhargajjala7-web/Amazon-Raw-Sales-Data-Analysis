```python
#!/usr/bin/env python3
"""
Visualization examples using Matplotlib/Seaborn/Plotly.
Produces PNG files in the output directory.
"""
import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def plot_time_series(daily_csv, out_dir):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(daily_csv, parse_dates=["ds", "date"], infer_datetime_format=True)
    date_col = "ds" if "ds" in df.columns else "date"
    revenue_col = "y" if "y" in df.columns else "revenue"
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    plt.figure(figsize=(12,5))
    sns.lineplot(data=df, x=date_col, y=revenue_col)
    plt.title("Revenue over time")
    plt.tight_layout()
    plt.savefig(out_dir / "revenue_timeseries.png")
    print("Saved revenue_timeseries.png")

def plot_top_categories(revenue_by_category_csv, out_dir, top_n=20):
    df = pd.read_csv(revenue_by_category_csv)
    if df.shape[1] >= 2:
        # assume first column is category, second is revenue
        cat_col = df.columns[0]
        rev_col = df.columns[1]
        df_sorted = df.sort_values(by=rev_col, ascending=False).head(top_n)
        plt.figure(figsize=(10,6))
        sns.barplot(y=cat_col, x=rev_col, data=df_sorted)
        plt.title("Top categories by revenue")
        plt.tight_layout()
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        plt.savefig(Path(out_dir) / "top_categories.png")
        print("Saved top_categories.png")
    else:
        print("Unexpected format for revenue_by_category CSV")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--daily", required=False, help="daily_aggregates.csv or prophet daily")
    parser.add_argument("--bycat", required=False, help="revenue_by_category.csv")
    parser.add_argument("--out", default="visualizations")
    args = parser.parse_args()
    if args.daily:
        plot_time_series(args.daily, args.out)
    if args.bycat:
        plot_top_categories(args.bycat, args.out)