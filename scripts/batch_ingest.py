"""Paginated local-CSV adapter simulating a paginated API; includes retry, watermark and upsert."""
import argparse, json, time
from pathlib import Path
import pandas as pd

def get_page(df, page, page_size, retries=4, fail_first=False):
    for attempt in range(retries):
        try:
            if fail_first and attempt == 0:
                raise TimeoutError("Simulated temporary failure")
            start = page * page_size
            return df.iloc[start:start+page_size].copy()
        except (TimeoutError, ConnectionError):
            if attempt == retries-1:
                raise
            delay = 0.5 * (2 ** attempt)
            print(f"temporary_error page={page}; retry={attempt+1}; sleep={delay}s")
            time.sleep(delay)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--source",default="data/orders.csv")
    ap.add_argument("--out",default="data/raw")
    ap.add_argument("--page-size",type=int,default=10)
    ap.add_argument("--checkpoint",default="data/watermark.json")
    ap.add_argument("--simulate-failure",action="store_true")
    a=ap.parse_args()
    if a.page_size < 1: raise SystemExit("--page-size must be >= 1")
    source=pd.read_csv(a.source, dtype={"order_id":"string","customer_id":"string"})
    # Preserve malformed timestamps in raw layer; normalize valid timestamps only.
    source["order_ts_parsed"]=pd.to_datetime(source["order_ts"],errors="coerce",utc=True)
    cp=Path(a.checkpoint); cp.parent.mkdir(parents=True,exist_ok=True)
    watermark=None
    if cp.exists():
        watermark=json.loads(cp.read_text(encoding="utf-8")).get("watermark")
    out=Path(a.out); out.mkdir(parents=True,exist_ok=True)
    target=out/"orders.parquet"
    pages=[]; page=0; read=0; first=a.simulate_failure
    while True:
        chunk=get_page(source,page,a.page_size,fail_first=first)
        first=False
        if chunk.empty: break
        read += len(chunk)
        print(f"page={page} rows={len(chunk)}")
        pages.append(chunk); page+=1
    new=pd.concat(pages,ignore_index=True) if pages else source.iloc[0:0].copy()
    # Watermark is used as an incremental filter; order_id upsert provides a second guard.
    if watermark:
        ts=pd.to_datetime(new["order_ts"],errors="coerce",utc=True)
        new=new.loc[ts > pd.Timestamp(watermark)].copy()
    before=len(new)
    old_count = 0
    old_ids = set()

    if target.exists():
        old = pd.read_parquet(target)
        old_count = len(old)
        old_ids = set(old["order_id"].dropna().astype(str))

        merged = pd.concat([old, new], ignore_index=True)
    else:
        merged = new.copy()

    merged = merged.drop_duplicates("order_id", keep="last")
    merged.to_parquet(target, index=False)

    new_ids = set(merged["order_id"].dropna().astype(str))
    records_added = len(new_ids - old_ids)

    print(f"records_read={read}")
    print(f"candidates_after_watermark={before}")
    print(f"records_added={records_added}")
    print(f"previous_total={old_count}")
    print(f"total_unique={len(merged)}")
    if "order_id" in merged:
        merged=merged.drop_duplicates("order_id",keep="last")
    merged.to_parquet(target,index=False)
    valid_ts=pd.to_datetime(merged["order_ts"],errors="coerce",utc=True).dropna()
    if not valid_ts.empty:
        cp.write_text(json.dumps({"watermark":valid_ts.max().isoformat()},indent=2),encoding="utf-8")
    print(f"records_read={read} candidates_after_watermark={before} total_unique={len(merged)}")
    print(f"records_added_estimate={max(0,len(merged)-(len(pd.read_parquet(target)) if False else 0)) if not target.exists() else 'see before/after run'}")
    print(f"raw_parquet={target}; watermark={cp}")
    print("Note: CSV pagination simulates API pagination; replace get_page with requests.get for a real endpoint.")
if __name__=="__main__": main()
