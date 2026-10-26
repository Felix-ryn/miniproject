import time
from pathlib import Path
from statistics import median

import pandas as pd
import polars as pl


# Lokasi project dan input
ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data" / "valid" / "orders.parquet"
OUTPUT = ROOT / "data" / "processed" / "orders_transformed.parquet"

REPEATS = 5


def transform_pandas(df):
    """Transformasi menggunakan Pandas."""
    df = df.copy()

    df["order_id"] = df["order_id"].astype("string").str.strip()
    df["customer_id"] = df["customer_id"].astype("string").str.strip()

    df["order_ts"] = pd.to_datetime(
        df["order_ts"], errors="coerce", utc=True
    )
    df["quantity"] = pd.to_numeric(
        df["quantity"], errors="coerce"
    ).astype("Int64")
    df["unit_price"] = pd.to_numeric(
        df["unit_price"], errors="coerce"
    ).astype("float64")
    df["discount_pct"] = pd.to_numeric(
        df["discount_pct"], errors="coerce"
    ).astype("float64")

    df["gross_amount"] = df["quantity"] * df["unit_price"]
    df["net_amount"] = df["gross_amount"] * (
        1 - df["discount_pct"]
    )

    return df


def transform_polars(df):
    """Transformasi menggunakan Polars eager execution."""
    return df.with_columns(
        pl.col("order_id").cast(pl.String).str.strip_chars(),
        pl.col("customer_id").cast(pl.String).str.strip_chars(),
        pl.col("order_ts")
        .cast(pl.String)
        .str.to_datetime(time_zone="UTC", strict=False),
        pl.col("quantity").cast(pl.Int64, strict=False),
        pl.col("unit_price").cast(pl.Float64, strict=False),
        pl.col("discount_pct").cast(pl.Float64, strict=False),
    ).with_columns(
        (pl.col("quantity") * pl.col("unit_price")).alias(
            "gross_amount"
        ),
        (
            pl.col("quantity")
            * pl.col("unit_price")
            * (1 - pl.col("discount_pct"))
        ).alias("net_amount"),
    )


def benchmark(label, function):
    """Jalankan fungsi beberapa kali dan laporkan median waktu."""
    times = []

    for _ in range(REPEATS):
        start = time.perf_counter()
        result = function()
        # Memastikan hasil tersedia sebelum waktu dihitung.
        if isinstance(result, pl.LazyFrame):
            result = result.collect()
        _ = len(result)
        times.append(time.perf_counter() - start)

    print(
        f"{label}: median={median(times):.6f} detik "
        f"(pengulangan={REPEATS})"
    )
    return result


def main():
    if not SOURCE.exists():
        raise FileNotFoundError(
            f"Input tidak ditemukan: {SOURCE}\n"
            "Jalankan validasi terlebih dahulu."
        )

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    # Ukuran dataset yang sama untuk kedua library
    pandas_input = pd.read_parquet(SOURCE)
    polars_input = pl.read_parquet(SOURCE)

    print("=== INFORMASI DATASET ===")
    print(f"File input       : {SOURCE.relative_to(ROOT)}")
    print(f"Jumlah baris     : {len(pandas_input)}")
    print(f"Jumlah kolom     : {len(pandas_input.columns)}")
    print(
        f"Ukuran file      : "
        f"{SOURCE.stat().st_size / 1024:.2f} KB"
    )
    print(f"Pengulangan      : {REPEATS}")
    print()

    # Transformasi dan benchmark
    pandas_result = benchmark(
        "Pandas (baca + transformasi)",
        lambda: transform_pandas(pd.read_parquet(SOURCE)),
    )

    polars_result = benchmark(
        "Polars eager (baca + transformasi)",
        lambda: transform_polars(pl.read_parquet(SOURCE)),
    )

    # Lazy execution: query baru dieksekusi ketika collect() dipanggil.
    def lazy_query():
        return transform_polars(pl.scan_parquet(SOURCE)).collect()

    lazy_result = benchmark(
        "Polars lazy (scan_parquet + collect)",
        lazy_query,
    )

    # Simpan hasil transformasi dari Pandas sebagai output utama.
    pandas_result.to_parquet(OUTPUT, index=False)

    print()
    print("=== CONTOH HASIL TRANSFORMASI ===")
    columns = [
        "order_id",
        "quantity",
        "unit_price",
        "discount_pct",
        "gross_amount",
        "net_amount",
        "order_ts",
    ]
    print(pandas_result[columns].head(5).to_string(index=False))

    print()
    print("=== PERBANDINGAN JUMLAH BARIS ===")
    print(f"Pandas      : {len(pandas_result)}")
    print(f"Polars eager: {polars_result.height}")
    print(f"Polars lazy : {lazy_result.height}")
    print(f"Output      : {OUTPUT.relative_to(ROOT)}")

    # Eksperimen data skew: gandakan customer paling dominan.
    print()
    print("=== EKSPERIMEN DATA SKEW ===")
    customer_counts = pandas_result["customer_id"].value_counts()
    dominant_customer = customer_counts.index[0]

    skewed = pandas_result.copy()
    dominant_rows = skewed[
        skewed["customer_id"] == dominant_customer
    ]
    skewed = pd.concat(
        [skewed] + [dominant_rows] * 20,
        ignore_index=True,
    )

    print(f"Customer dominan       : {dominant_customer}")
    print(f"Baris awal              : {len(pandas_result)}")
    print(f"Baris setelah data skew : {len(skewed)}")
    print(
        "Duplikasi tambahan      : "
        f"{len(skewed) - len(pandas_result)}"
    )

    before = pandas_result.groupby("customer_id").size()
    after = skewed.groupby("customer_id").size()

    print(f"Grup customer sebelum   : {len(before)}")
    print(f"Grup customer sesudah   : {len(after)}")
    print(
        "Jumlah baris customer dominan sebelum: "
        f"{int(before[dominant_customer])}"
    )
    print(
        "Jumlah baris customer dominan sesudah: "
        f"{int(after[dominant_customer])}"
    )

    # Bandingkan waktu agregasi pada data normal dan data skew.
    for label, data in [
        ("Normal", pandas_result),
        ("Skew", skewed),
    ]:
        times = []
        for _ in range(REPEATS):
            start = time.perf_counter()
            result = data.groupby("customer_id").agg(
                order_count=("order_id", "count"),
                total_net_amount=("net_amount", "sum"),
            )
            times.append(time.perf_counter() - start)

        print(
            f"Groupby {label}: median={median(times):.6f} detik "
            f"(baris={len(data)}, pengulangan={REPEATS})"
        )

    print()
    print("Benchmark ini hanya berlaku untuk lingkungan dan dataset ini.")
    print("Data skew di atas adalah simulasi, bukan pengujian OOM.")


if __name__ == "__main__":
    main()