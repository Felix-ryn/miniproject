import argparse, csv, json
from pathlib import Path
from kafka import KafkaConsumer
from validate import validate_records

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--bootstrap",default="localhost:9092")
    p.add_argument("--group",default="orders-validator-v1")
    p.add_argument("--max-messages",type=int,default=21)
    p.add_argument("--valid",default="data/stream/valid_orders.jsonl")
    p.add_argument("--rejected",default="data/stream/rejected_orders.jsonl")
    p.add_argument("--seen",default="data/stream/seen_order_ids.json")
    a=p.parse_args()
    Path(a.valid).parent.mkdir(parents=True,exist_ok=True)
    seen_path=Path(a.seen)
    seen=set(json.loads(seen_path.read_text())) if seen_path.exists() else set()
    consumer=KafkaConsumer("orders",bootstrap_servers=a.bootstrap,group_id=a.group,
        auto_offset_reset="earliest",enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")))
    count=0; valid_count=0; rejected_count=0; duplicate_count=0
    try:
        for msg in consumer:
            payload=msg.value
            valid,rejected=validate_records([payload])
            meta={"topic":msg.topic,"partition":msg.partition,"offset":msg.offset}
            if valid:
                order=valid[0]; oid=order["order_id"]
                if oid in seen:
                    duplicate_count+=1
                    record={**meta,"order_id":oid,"reason":"duplicate order_id (sink idempotency)"}
                    with open(a.rejected,"a",encoding="utf-8") as f: f.write(json.dumps(record)+"\n")
                    rejected_count+=1
                else:
                    with open(a.valid,"a",encoding="utf-8") as f: f.write(json.dumps({**meta,**order})+"\n")
                    seen.add(oid); valid_count+=1
            else:
                with open(a.rejected,"a",encoding="utf-8") as f:
                    f.write(json.dumps({**meta,**rejected[0]})+"\n")
                rejected_count+=1
            seen_path.write_text(json.dumps(sorted(seen),indent=2),encoding="utf-8")
            consumer.commit()
            count+=1
            print(f"topic={msg.topic} partition={msg.partition} offset={msg.offset}")
            if count>=a.max_messages: break
    finally:
        consumer.close()
    print(f"consumed={count} valid_written={valid_count} rejected_written={rejected_count} duplicates={duplicate_count}")
    print("Guarantee: at-least-once processing with a file-based idempotency guard, not end-to-end exactly-once.")
if __name__=="__main__": main()
