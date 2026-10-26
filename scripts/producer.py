import argparse, json, time
import pandas as pd
from kafka import KafkaProducer

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--source",default="data/orders.csv")
    p.add_argument("--bootstrap",default="localhost:9092")
    p.add_argument("--duplicate-first",action="store_true")
    p.add_argument("--limit",type=int,default=20)
    a=p.parse_args()
    df=pd.read_csv(a.source,dtype=str).fillna("")
    producer=KafkaProducer(bootstrap_servers=a.bootstrap,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        key_serializer=lambda x: x.encode("utf-8"))
    rows=df.head(a.limit).to_dict(orient="records")
    for i,row in enumerate(rows):
        producer.send("orders",key=str(row.get("order_id","")),value=row)
        if a.duplicate_first and i==0:
            producer.send("orders",key=str(row.get("order_id","")),value=row)
    producer.flush(); producer.close()
    print(f"published={len(rows) + (1 if a.duplicate_first and rows else 0)} topic=orders")
if __name__=="__main__": main()
