# Retail Data Platform — Olist

A PySpark + MinIO pipeline that ingests the [Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) from Kaggle and processes it through a **Bronze → Silver → Gold** medallion architecture.

---

## 1. Data Architecture

### Bronze layer (`s3a://clean/olist/bronze/*`)

Raw-to-Parquet ingestion with explicit schemas, one table per dataset:

`customers` · `geolocation` · `order_items` · `order_payments` · `order_reviews` · `orders` · `products` · `sellers` · `category_translation`

### Silver layer (`s3a://clean/olist/silver/*`)

| Table                   | Description                                     |
| ----------------------- | ----------------------------------------------- |
| `stg_orders`            | Validated, deduplicated orders (DQ gate output) |
| `stg_orders_quarantine` | Rows that failed validation                     |
| `dim_customers`         | Customer dimension                              |
| `dim_sellers`           | Seller dimension                                |
| `dim_products`          | Product dimension with English category names   |
| `dim_geolocation`       | ZIP-level lat/lng averages                      |
| `fct_order_items`       | Fact table — grain: `order_id + order_item_id`  |

### Gold layer (`s3a://mart/olist/gold/*`)

| Table                       | Description                                      |
| --------------------------- | ------------------------------------------------ |
| `mart_order_analytics`      | Wide row-level table for BI / ad-hoc exploration |
| `mart_daily_sales`          | Day-level GMV, freight, review score, delay rate |
| `mart_state_performance`    | State-level GMV and KPIs                         |
| `mart_category_performance` | Category-level GMV and KPIs                      |
| `mart_order_status_daily`   | Daily KPIs broken out by order status            |

---

## 2. Job Responsibilities

| Job                   | Path                   | Reads from                      | Writes to                                           |
| --------------------- | ---------------------- | ------------------------------- | --------------------------------------------------- |
| `bronze_ingest_olist` | `jobs/ingestion/`      | `raw/*` CSVs                    | `bronze/*`                                          |
| `etl_orders`          | `jobs/etl/`            | `bronze/orders`                 | `silver/stg_orders`, `silver/stg_orders_quarantine` |
| `silver_build_model`  | `jobs/transformation/` | `bronze/*`, `silver/stg_orders` | `silver/dim_*`, `silver/fct_order_items`            |
| `gold_build_marts`    | `jobs/transformation/` | `silver/*`                      | `gold/mart_*`                                       |

> `gold_build_marts` consolidates all five gold marts in one job to avoid re-reading and re-joining silver tables multiple times.

---

## 3. Setup

### 3.1 Configure credentials

```bash
cp .env.example .env
# Edit .env — fill in S3_ACCESS_KEY, S3_SECRET_KEY, KAGGLE_USERNAME, KAGGLE_KEY
```

`.env` is git-ignored. **Never commit credentials.**

### 3.2 Start infrastructure

```bash
docker compose up -d
```

The Spark container automatically loads `.env` via `env_file` in `docker-compose.yml`.

### 3.3 (Optional) Download raw data from Kaggle

```bash
# Linux / macOS
./scripts/run_kaggle_upload.sh

# Windows PowerShell
powershell -ExecutionPolicy Bypass -File .\scripts\run_kaggle_upload.ps1
```

### 3.4 Run full pipeline

```bash
# Linux / macOS
./scripts/run_olist_pipeline.sh

# Windows PowerShell
powershell -ExecutionPolicy Bypass -File .\scripts\run_olist_pipeline.ps1
```

Optional parameters (PowerShell):

```powershell
.\scripts\run_olist_pipeline.ps1 -Container spark -Master "local[*]" -Packages "org.apache.hadoop:hadoop-aws:3.3.4"
```

### 3.5 Run a single job

```bash
docker exec spark /opt/spark/bin/spark-submit \
  --master local[*] \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  jobs/ingestion/bronze_ingest_olist.py
```

---

## 4. Project Structure

```
.
├── core/
│   ├── config.py          # StorageConfig, OLIST_DATASETS, OLIST_SCHEMAS
│   ├── io.py              # read_raw_csv (explicit schema), write_parquet (with row logging)
│   ├── logger.py          # shared logger
│   └── spark_session.py   # SparkSession factory
├── jobs/
│   ├── ingestion/
│   │   ├── kaggle_upload.py         # Kaggle → MinIO raw upload (boto3, no Spark)
│   │   └── bronze_ingest_olist.py   # raw CSV → bronze Parquet (PySpark)
│   ├── etl/
│   │   └── etl_orders.py            # DQ gate: valid → stg_orders, invalid → quarantine
│   └── transformation/
│       ├── silver_build_model.py    # dims + fct_order_items
│       └── gold_build_marts.py      # all five gold marts (shared enriched cache)
├── scripts/
│   ├── run_olist_pipeline.ps1 / .sh
│   └── run_kaggle_upload.ps1 / .sh
├── .env.example
├── .gitignore
└── docker-compose.yml
```

---

## 5. Environment Variables

| Variable                       | Required         | Default             | Description                 |
| ------------------------------ | ---------------- | ------------------- | --------------------------- |
| `S3_ENDPOINT`                  | ✅               | —                   | MinIO / S3 endpoint URL     |
| `S3_ACCESS_KEY`                | ✅               | —                   | Access key                  |
| `S3_SECRET_KEY`                | ✅               | —                   | Secret key                  |
| `KAGGLE_USERNAME`              | ✅ (ingest only) | —                   | Kaggle account username     |
| `KAGGLE_KEY`                   | ✅ (ingest only) | —                   | Kaggle API key              |
| `RAW_BASE_PATH`                |                  | `s3a://raw`         | Root path for raw CSVs      |
| `CLEAN_BASE_PATH`              |                  | `s3a://clean/olist` | Root path for bronze/silver |
| `MART_BASE_PATH`               |                  | `s3a://mart/olist`  | Root path for gold marts    |
| `SPARK_SQL_SHUFFLE_PARTITIONS` |                  | `8`                 | Spark shuffle partitions    |