import time, statistics
import pandas as pd
import polars as pl

SOURCE="data/orders.csv"
REPEATS=5

def pandas_run():
    df=pd.read_csv(SOURCE)
    df["gross_amount"]=df["quantity"]*df["unit_price"]
    df["net_amount"]=df["gross_amount"]*(1-df["discount_pct"])
    return df.groupby("product_category",dropna=False)["net_amount"].sum()

def polars_eager():
    df=pl.read_csv(SOURCE)
    df=df.with_columns(
      (pl.col("quantity")*pl.col("unit_price")).alias("gross_amount"),
      (pl.col("quantity")*pl.col("unit_price")*(1-pl.col("discount_pct"))).alias("net_amount"))
    return df.group_by("product_category").agg(pl.col("net_amount").sum())

def polars_lazy():
    return (pl.scan_csv(SOURCE)
      .with_columns(
       (pl.col("quantity")*pl.col("unit_price")).alias("gross_amount"),
       (pl.col("quantity")*pl.col("unit_price")*(1-pl.col("discount_pct"))).alias("net_amount"))
      .group_by("product_category").agg(pl.col("net_amount").sum()).collect())

def measure(fn):
    times=[]
    for _ in range(REPEATS):
        t=time.perf_counter(); fn(); times.append(time.perf_counter()-t)
    return statistics.median(times)

def main():
    rows=sum(1 for _ in open(SOURCE,encoding="utf-8"))-1
    results={"rows":rows,"repeats":REPEATS,
      "pandas_median_seconds":measure(pandas_run),
      "polars_eager_median_seconds":measure(polars_eager),
      "polars_lazy_median_seconds":measure(polars_lazy)}
    print(results)
    print("Caveat: tiny dataset, CSV parsing included, local machine and cache affect timings; do not generalize.")
    # Skew experiment: replicate rows with one dominant customer_id.
    df=pd.read_csv(SOURCE)
    skew=pd.concat([df, *[df.assign(customer_id="C_DOMINANT") for _ in range(20)]],ignore_index=True)
    print("normal_top_groups:")
    print(df.groupby("customer_id").size().sort_values(ascending=False).head(5))
    print("skew_top_groups:")
    print(skew.groupby("customer_id").size().sort_values(ascending=False).head(5))
    for label,frame in [("normal",df),("skewed",skew)]:
        t=time.perf_counter(); frame.groupby("customer_id")["quantity"].sum()
        print(f"groupby_{label}_rows={len(frame)} seconds={time.perf_counter()-t:.6f}")
if __name__=="__main__": main()
