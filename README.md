# End-to-End Event-to-Warehouse Pipeline

Paket praktikum menggunakan dataset sintetis `data/orders.csv`. Dataset sengaja menyertakan record invalid, jadi jangan bersihkan sumber sebelum validasi.

## Persiapan (Windows PowerShell)
```powershell
cd data_processing_miniproject
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```
Jika aktivasi ditolak, jalankan `Set-ExecutionPolicy -Scope Process Bypass` di terminal PowerShell yang sama lalu aktifkan lagi. Docker Desktop harus aktif.

## A. Batch ingestion
```powershell
python scripts/batch_ingest.py --source data/orders.csv --out data/raw --page-size 10 --simulate-failure
python scripts/batch_ingest.py --source data/orders.csv --out data/raw --page-size 10
```
Bandingkan jumlah baris dan ID unik `data/raw/orders.parquet` sebelum/sesudah run kedua. File `data/watermark.json` menyimpan watermark. Sumber di sini adalah CSV yang dipaginasi untuk simulasi; ini bukan panggilan API nyata. Untuk API nyata, ganti adapter `get_page` dengan `requests.get`, timeout, pemeriksaan status HTTP, `Retry-After`, dan parameter `page/page_size` yang sesuai API.

Catatan: watermark timestamp murni tidak cukup untuk menangani late-arriving events yang timestamp-nya lama; upsert `order_id` merupakan perlindungan tambahan. Untuk latihan sederhana, sumber dianggap snapshot tetap.

## C. Validasi dan transformasi
```powershell
python scripts/validate.py --source data/orders.csv
```
Output: `data/valid/orders.parquet` dan `data/rejected/orders.csv`. Rejected file menyimpan alasan dan payload asal. Record invalid tidak dimasukkan ke warehouse.

## B. Kafka
```powershell
docker compose up -d
docker compose ps
```
Terminal 1:
```powershell
python scripts/consumer.py --max-messages 21
```
Terminal 2:
```powershell
python scripts/producer.py --limit 20 --duplicate-first
```
Producer menerbitkan 20 baris dan mengulang event pertama satu kali. Consumer group/offset bersifat persisten; untuk mengulang demo dari awal, hentikan consumer dan gunakan group baru:
```powershell
python scripts/consumer.py --group orders-demo-2 --max-messages 21
```
Consumer memakai deduplikasi berbasis `order_id` di file sink. Ini contoh edukatif: file valid/rejected dan daftar seen tidak ditulis secara atomik bersama commit offset, jadi jaminannya bukan exactly-once end-to-end. Pemrosesan bisa berulang saat crash; idempotensi sink mengurangi duplikasi bisnis pada run normal.

Setelah selesai:
```powershell
docker compose down
```

## D. Benchmark dan skew
```powershell
python scripts/benchmark.py
```
Skrip mengukur median lima run untuk Pandas, Polars eager, dan Polars lazy pada CSV yang sama; lalu membuat data skew dengan memperbanyak baris ber-customer_id dominan. Dataset contoh kecil, jadi hasil waktu hanya demonstrasi lokal, bukan kesimpulan umum. Lazy query baru dieksekusi saat `collect()` dipanggil.

## E. DuckDB
```powershell
python scripts/build_warehouse.py
```
Membuat `warehouse/warehouse.duckdb` dengan staging, `dim_customer`, `dim_product_category`, `fct_orders`, dan `mart_daily_sales`. Definisi sales di contoh hanya menghitung status `paid` dan `shipped`; cancelled/refunded tidak dianggap penjualan yang diakui.

## dbt
Jalankan validasi dan bangun database dulu. Lalu:
```powershell
cd dbt_project
dbt debug --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```
Buka URL lokal yang dicetak oleh `dbt docs serve` dan ambil screenshot untuk laporan. Jika versi dbt yang terpasang tidak menerima `arguments` pada relationships test, cek `dbt --version` lalu sesuaikan sintaks dengan versi itu.

## Konsep yang harus dijelaskan
- Partition: membagi topic menjadi log; urutan dijamin per partition.
- Consumer group: konsumen dalam group membagi partisi untuk pemrosesan paralel.
- Offset: posisi pesan dalam partition.
- At-most-once: commit sebelum proses dapat membuat pesan hilang.
- At-least-once: commit setelah sink berhasil menghindari kehilangan pada alur normal, tetapi crash dapat membuat pesan diproses ulang.
- Exactly-once: perlu jaminan atomik pada batas sistem terkait; Kafka transactions saja tidak otomatis mencakup file/DB eksternal.
- Star schema: fact terhubung langsung ke dimensi. Snowflake: dimensi dipecah/ dinormalisasi menjadi tabel terkait.
- Incremental dbt: proses hanya data baru/perubahan dengan unique key dan strategi merge/upsert yang benar.

## Pengumpulan bukti
Simpan log batch dua kali, hitungan valid/rejected, output benchmark, output skew, `docker compose ps`, producer/consumer logs, output `dbt run/test`, dan screenshot dbt docs. Jangan tulis angka benchmark atau hasil tes sebelum benar-benar menjalankannya.
