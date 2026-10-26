import json
from datetime import datetime
from pathlib import Path
from typing import Literal
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

class Order(BaseModel):
    model_config = ConfigDict(extra="forbid")
    order_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    order_ts: datetime
    product_category: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    unit_price: float = Field(ge=0)
    discount_pct: float = Field(ge=0, le=1)
    payment_method: str = Field(min_length=1)
    status: Literal["paid", "shipped", "cancelled", "refunded"]

    @field_validator("order_id", "customer_id", "product_category", "payment_method", mode="before")
    @classmethod
    def strip_strings(cls, v):
        if isinstance(v, str):
            v = v.strip()
        return v

def validate_records(records):
    valid, rejected = [], []
    for row in records:
        payload = dict(row)
        # Pandas may represent missing cells as NaN; convert these to None for Pydantic.
        payload = {k: (None if pd.isna(v) else v) for k, v in payload.items()}
        try:
            order = Order.model_validate(payload)
            item = order.model_dump(mode="json")
            ts = pd.to_datetime(item["order_ts"], utc=True)
            item["order_ts"] = ts.isoformat()
            item["quantity"] = int(item["quantity"])
            item["unit_price"] = float(item["unit_price"])
            item["discount_pct"] = float(item["discount_pct"])
            item["gross_amount"] = item["quantity"] * item["unit_price"]
            item["net_amount"] = item["gross_amount"] * (1 - item["discount_pct"])
            valid.append(item)
        except (ValidationError, ValueError, TypeError) as e:
            errs = e.errors() if isinstance(e, ValidationError) else [{"msg": str(e)}]
            rejected.append({
                "order_id": str(payload.get("order_id", "")) if payload.get("order_id") is not None else "",
                "reason": "; ".join(f'{x.get("loc", "")}: {x.get("msg", "invalid")}' for x in errs),
                "payload": json.dumps(payload, default=str, ensure_ascii=False)
            })
    return valid, rejected

def validate_csv(source, valid_path="data/valid/orders.parquet", rejected_path="data/rejected/orders.csv"):
    df = pd.read_csv(source, dtype={"order_id":"string","customer_id":"string"}, keep_default_na=True)
    valid, rejected = validate_records(df.to_dict(orient="records"))
    Path(valid_path).parent.mkdir(parents=True, exist_ok=True)
    Path(rejected_path).parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(valid).to_parquet(valid_path, index=False)
    pd.DataFrame(rejected, columns=["order_id","reason","payload"]).to_csv(rejected_path, index=False)
    print(f"read={len(df)} valid={len(valid)} rejected={len(rejected)}")
    print(f"valid_file={valid_path}\nrejected_file={rejected_path}")
    return valid, rejected

if __name__ == "__main__":
    import argparse
    p=argparse.ArgumentParser()
    p.add_argument("--source", default="data/orders.csv")
    p.add_argument("--valid", default="data/valid/orders.parquet")
    p.add_argument("--rejected", default="data/rejected/orders.csv")
    a=p.parse_args()
    validate_csv(a.source,a.valid,a.rejected)
