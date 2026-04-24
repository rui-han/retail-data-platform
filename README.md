## 1. Data Architecture

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

## 2. Jobs and Responsibilities

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

## 3. How to Run

### 3.1 Start infrastructure

```bash
docker compose up -d
```

### 3.2 Run full pipeline (recommended)

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

### 3.3 Run a single job (debug mode)

```powershell
docker exec spark /opt/spark/bin/spark-submit `
  --master local[*] `
  --packages org.apache.hadoop:hadoop-aws:3.3.4 `
  jobs/etl_orders.py
```

---

## 4. Configuration

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
