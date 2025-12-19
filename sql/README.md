# SQL (Snowflake)

This folder contains the SQL scripts that implement the full Snowflake pipeline:
**RAW → INT → PRES**, daily automation (Tasks), and Snowflake ML forecasting.

Back to root: [../README.md](../README.md)

---

## Files overview (in order)

### 01 — Historical bulk load into RAW (optional)
- **[`01_raw_historical_load_from_csv.sql`](./01_raw_historical_load_from_csv.sql)**  
  **Purpose:** one-time load of a large historical CSV into `RAW.RAW_311_CSV` using `COPY INTO`.  
  **When to use:** when you already staged a big CSV file in Snowflake (internal stage) and want to load it quickly.

### 02 — Daily API ingestion into RAW (optional)
- **[`02_raw_http_ingest_snowpark.sql`](./02_raw_http_ingest_snowpark.sql)**  
  **Purpose:** creates:
  - network rule + external access integration
  - Snowpark Python stored procedure to fetch *yesterday* from API
  - a daily Snowflake Task to run that procedure  
  **When to use:** if you want “production-like” daily ingestion from the NYC API.

### 03 — Build INT (typed table + initial backfill)
- **[`03_int_build_and_backfill.sql`](./03_int_build_and_backfill.sql)**  
  **Purpose:** creates `INT.INT_311_SERVICE_REQUESTS` and fills it from `RAW.RAW_311_CSV`.  
  Includes parsing timestamps/dates, `RESPONSE_TIME_HOURS`, `IS_CLOSED`.

### 04 — Build PRES marts + Power BI view
- **[`04_pres_metrics_and_pbi_views.sql`](./04_pres_metrics_and_pbi_views.sql)**  
  **Purpose:** creates:
  - `PRES.PRES_311_DAILY_METRICS` (daily aggregates)
  - `PRES.VW_PBI_DAILY_BOROUGH_METRICS` (includes `BOROUGH_LOCATION` for maps)  
  Also cleans borough values like `01 BROOKLYN` → `BROOKLYN`.

### 05 — Forecasting (Snowflake ML)
- **[`05_forecast_daily_requests.sql`](./05_forecast_daily_requests.sql)**  
  **Purpose:** builds `PRES.PRES_311_DAILY_FORECAST_BASE`, trains a Snowflake ML forecast model,
  and writes predictions into `PRES_311_FORECAST_RESULTS`.

### 06 — Daily automation (Tasks)
- **[`06_tasks_daily_etl.sql`](./06_tasks_daily_etl.sql)**  
  **Purpose:** creates procedures + daily task that:
  - loads yesterday from RAW → INT (incremental)
  - refreshes yesterday in PRES marts + forecast base  
  **Output:** `PRES.TASK_311_DAILY_ETL` (scheduled).

### 07 — Forecast evaluation (backtesting)
- **[`07_forecast_evaluation.sql`](./07_forecast_evaluation.sql)**  
  **Purpose:** evaluates forecast performance on the last 60 days and stores:
  - day-by-day actual vs predicted
  - MAE / RMSE / MAPE metrics  
  **Tables created:**
  - `PRES_311_FORECAST_EVAL_DAILY`
  - `PRES_311_FORECAST_EVAL_METRICS`

---

## Recommended run order (first time)

### If you have a historical CSV staged (bulk load)
1. `01_raw_historical_load_from_csv.sql`
2. `03_int_build_and_backfill.sql`
3. `04_pres_metrics_and_pbi_views.sql`
4. `05_forecast_daily_requests.sql`
5. `07_forecast_evaluation.sql`
6. `06_tasks_daily_etl.sql`

### If you use daily API ingestion
1. `02_raw_http_ingest_snowpark.sql`
2. `03_int_build_and_backfill.sql`
3. `04_pres_metrics_and_pbi_views.sql`
4. `05_forecast_daily_requests.sql`
5. `06_tasks_daily_etl.sql`

---

## Main objects (what to look at)

- RAW: `RAW.RAW_311_CSV`
- INT: `INT.INT_311_SERVICE_REQUESTS`
- PRES:
  - `PRES.PRES_311_DAILY_METRICS`
  - `PRES.VW_PBI_DAILY_BOROUGH_METRICS`
  - `PRES.PRES_311_DAILY_FORECAST_BASE`
  - `PRES_311_FORECAST_RESULTS`
- Tasks:
  - `RAW.TASK_311_INGEST_HTTP` (optional)
  - `PRES.TASK_311_DAILY_ETL`
