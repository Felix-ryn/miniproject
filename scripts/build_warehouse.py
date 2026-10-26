from pathlib import Path
import duckdb
import pandas as pd
from validate import validate_csv

def main():
    valid_path="data/valid/orders.parquet"
    if not Path(valid_path).exists():
        validate_csv("data/orders.csv",valid_path,"data/rejected/orders.csv")
    Path("warehouse").mkdir(exist_ok=True)
    con=duckdb.connect("warehouse/warehouse.duckdb")
    con.execute("CREATE OR REPLACE TABLE stg_orders AS SELECT * FROM read_parquet(?)",[valid_path])
    con.execute("""CREATE OR REPLACE TABLE dim_customer AS
       SELECT DISTINCT customer_id FROM stg_orders""")
    con.execute("""CREATE OR REPLACE TABLE dim_product_category AS
       SELECT DISTINCT product_category FROM stg_orders""")
    con.execute("""CREATE OR REPLACE TABLE fct_orders AS
       SELECT order_id, customer_id, product_category, CAST(order_ts AS TIMESTAMPTZ) AS order_ts,
              CAST(CAST(order_ts AS TIMESTAMPTZ) AS DATE) AS order_date,
              quantity, unit_price, discount_pct, gross_amount, net_amount, payment_method, status
       FROM stg_orders""")
    con.execute("""CREATE OR REPLACE TABLE mart_daily_sales AS
       SELECT order_date, product_category, payment_method,
              SUM(net_amount) AS net_sales, COUNT(*) AS order_count
       FROM fct_orders WHERE status IN ('paid','shipped')
       GROUP BY 1,2,3""")
    print("fct_orders",con.execute("SELECT COUNT(*) FROM fct_orders").fetchone()[0])
    print("\nNet sales per day:")
    print(con.execute("SELECT order_date, SUM(net_sales) net_sales FROM mart_daily_sales GROUP BY 1 ORDER BY 1").df().to_string(index=False))
    print("\nSales by category:")
    print(con.execute("SELECT product_category, SUM(net_sales) net_sales FROM mart_daily_sales GROUP BY 1 ORDER BY 2 DESC").df().to_string(index=False))
    print("\nOrders by status:")
    print(con.execute("SELECT status, COUNT(*) order_count, SUM(net_amount) net_amount FROM fct_orders GROUP BY 1 ORDER BY 1").df().to_string(index=False))
    print("\nPayment methods:")
    print(con.execute("SELECT payment_method, COUNT(*) order_count FROM fct_orders GROUP BY 1 ORDER BY 2 DESC").df().to_string(index=False))
    print("\nRejected reasons:")
    if Path("data/rejected/orders.csv").exists():
        print(pd.read_csv("data/rejected/orders.csv")["reason"].value_counts().to_string())
    con.close()
if __name__=="__main__": main()
