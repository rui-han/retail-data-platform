# Olist Medallion Data Platform (Personal Project)

This repository is my personal end-to-end data engineering project built on the
[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

The goal is to demonstrate practical **mid-level to senior-level** data engineering skills:
- layered lakehouse modeling (Bronze/Silver/Gold)
- data quality handling and quarantine
- dimensional modeling + business marts
- reproducible Spark jobs on local Docker + MinIO

---

## 1) Project Scope

### Source datasets
I use these 9 CSV files from Olist:
- `olist_customers_dataset.csv`
- `olist_geolocation_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_orders_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `product_category_name_translation.csv`

### Tech stack
- **Compute:** Apache Spark (PySpark)
- **Storage:** MinIO (S3-compatible)
- **Orchestration (local):** shell / PowerShell wrappers around `spark-submit`
- **Runtime:** Docker Compose

---

## 2) Data Architecture

### Bronze layer (`s3a://clean/olist/bronze/*`)
Raw-to-parquet ingestion, table-per-dataset:
- `customers`, `geolocation`, `order_items`, `order_payments`, `order_reviews`,
  `orders`, `products`, `sellers`, `category_translation`

### Silver layer (`s3a://clean/olist/silver/*`)
Conformed/staged datasets:
- Dimensions:
  - `dim_customers`
  - `dim_sellers`
  - `dim_products`
  - `dim_geolocation`
- Fact:
  - `fct_order_items` (grain: `order_id + order_item_id`)
- DQ staging:
  - `stg_orders`
  - `stg_orders_quarantine`

### Gold layer (`s3a://mart/olist/gold/*`)
Business-ready marts:
- `mart_order_analytics` (wide analytical table)
- `mart_daily_sales`
- `mart_state_performance`
- `mart_category_performance`
- `mart_order_status_daily`

---

## 3) Jobs and Responsibilities

1. `jobs/bronze_ingest_olist.py`
   - Ingest all raw CSVs into Bronze parquet datasets.

2. `jobs/etl_orders.py`
   - Standardize orders timestamps/strings.
   - Validate primary key (`order_id`) and isolate invalid rows into quarantine.

3. `jobs/silver_build_model.py`
   - Build dimensions and core fact table.
   - Join translations, aggregate payment/review attributes, derive delivery delay flag.

4. `jobs/gold_build_marts.py`
   - Build aggregated marts for daily sales, state-level performance, and category-level performance.

5. `jobs/build_order_analytics_mart.py`
   - Build a wide analytics mart for BI/exploration.

6. `jobs/build_order_kpis.py`
   - Build daily KPI mart by order status.

---

## 4) How to Run

### 4.1 Start infrastructure
```bash
docker compose up -d
```

### 4.2 Run full pipeline (recommended)

#### Windows PowerShell
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_olist_pipeline.ps1
```

Optional parameters:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_olist_pipeline.ps1 `
  -Container spark `
  -Master "local[*]" `
  -Packages "org.apache.hadoop:hadoop-aws:3.3.4"
```

#### Linux / macOS / WSL
```bash
./scripts/run_olist_pipeline.sh
```

### 4.3 Run a single job (debug mode)
```powershell
docker exec spark /opt/spark/bin/spark-submit `
  --master local[*] `
  --packages org.apache.hadoop:hadoop-aws:3.3.4 `
  jobs/etl_orders.py
```

---

## 5) Configuration


### Bucket convention used in this repo
- `raw` bucket: source CSV files only
- `clean` bucket: Bronze + Silver outputs
- `mart` bucket: Gold/business marts

All storage/config settings are centralized in `core/config.py`.

Common environment variables:
- `S3_ENDPOINT` (default: `http://minio:9000`)
- `S3_ACCESS_KEY` (default: `admin`)
- `S3_SECRET_KEY` (default: `password123`)
- `RAW_BASE_PATH` (default: `s3a://raw`)
- `CLEAN_BASE_PATH` (default: `s3a://clean/olist`)
- `MART_BASE_PATH` (default: `s3a://mart/olist`)

---

## 6) Why this project is portfolio-relevant

This project showcases capabilities expected in production-oriented analytics/data teams:
- data layer design and standardization
- reproducible batch pipelines
- data quality checks and quarantine handling
- business-oriented marts and KPIs
- environment-driven configuration for portability

---

## 7) Next Improvements

Planned enhancements:
- add automated tests (PySpark unit tests + data quality assertions)
- add incremental processing strategy (partitioning + watermark)
- add orchestration with Airflow/Dagster
- add dbt semantic layer + tests + documentation
- add BI dashboard examples on top of Gold marts
