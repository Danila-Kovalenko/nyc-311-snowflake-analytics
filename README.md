# nyc-311-snowflake-analytics

End-to-end **Data Engineering + Analytics** portfolio project based on **NYC 311 Service Requests**:
- Ingest daily data from a public API into **Snowflake RAW**
- Transform into **INT** (typed, enriched)
- Build **PRES** marts for BI + mapping
- Orchestrate daily pipelines with **Snowflake Tasks**
- Train a **Snowflake ML Forecast** model and store predictions
- Visualize in **Power BI**

Repository: https://github.com/Danila-Kovalenko/nyc-311-snowflake-analytics

---

## Repository structure

- **[`sql/`](./sql/)** — Snowflake pipeline scripts (RAW → INT → PRES, tasks, forecasting)  
  → Readme: **[`sql/README.md`](./sql/README.md)**
- **[`python/`](./python/)** — local utilities / loaders used during development  
  → Readme: **[`python/README.md`](./python/README.md)**
- **[`powerbi/`](./powerbi/)** — Power BI report/dashboard assets  
  → Readme: **[`powerbi/README.md`](./powerbi/README.md)**

---

## Data source

**NYC Open Data — 311 Service Requests**  
- Daily ingestion uses Socrata API endpoint (JSON):  
  `https://data.cityofnewyork.us/resource/erm2-nwe9.json`

The project demonstrates both approaches:
1) **Production-like daily ingestion** (API → Snowflake via Snowpark Python SP + Task)  
2) **Optional historical bulk load** from a large CSV into an internal stage and then `COPY INTO` RAW

---

## Architecture (RAW → INT → PRES)

### RAW (landing)
**Goal:** keep data close to source, minimal transformations  
- Table: `RAW.RAW_311_CSV`  
- Ingestion options:
  - API ingestion (daily): [`sql/data_to_raw.sql`](./sql/data_to_raw.sql)
  - CSV bulk load (optional): [`sql/delete.sql`](./sql/delete.sql)

### INT (typed, enriched)
**Goal:** correct types, compute derived fields
- Table: `INT.INT_311_SERVICE_REQUESTS`
- Parsing timestamps / dates, computing:
  - `RESPONSE_TIME_HOURS`
  - `IS_CLOSED`
- Script: [`sql/INT_Worksheet.sql`](./sql/INT_Worksheet.sql)

### PRES (analytics marts)
**Goal:** aggregated tables for BI / dashboards
- Table: `PRES.PRES_311_DAILY_METRICS`
- View for Power BI mapping:
  - `PRES.VW_PBI_DAILY_BOROUGH_METRICS` (includes `BOROUGH_LOCATION`)
- Script: [`sql/PRES.sql`](./sql/PRES.sql)

---

## Orchestration (daily automation)

### Daily API ingestion task
Creates external access integration + Snowpark procedure + daily task:
- Script: [`sql/data_to_raw.sql`](./sql/data_to_raw.sql)
- Main objects created:
  - `RAW.SP_INGEST_311_YESTERDAY_HTTP()`
  - `RAW.TASK_311_INGEST_HTTP`

### Daily ETL task (RAW → INT → PRES)
Loads yesterday’s rows into INT and refreshes PRES marts for yesterday:
- Script: [`sql/tasks.sql`](./sql/tasks.sql)
- Main objects created:
  - `INT.SP_LOAD_311_FROM_RAW_YESTERDAY()`
  - `PRES.SP_REFRESH_311_PRES_FOR_YESTERDAY()`
  - `PRES.TASK_311_DAILY_ETL`

---

## Forecasting (Snowflake ML)

This project includes a baseline time-series forecast of daily total 311 requests:

- Base table: `PRES.PRES_311_DAILY_FORECAST_BASE`
- Model: `SNOWFLAKE.ML.FORECAST NYC311_DAILY_REQUESTS_MODEL`
- Predictions table: `PRES_311_FORECAST_RESULTS` (created by script)

Script:
- [`sql/Daily_Forecast.sql`](./sql/Daily_Forecast.sql)

> Note: This is a **baseline** forecast using historical daily totals. Accuracy can be improved by adding exogenous features and/or segmentation (borough/agency).

---

## Power BI dashboard

The Power BI report uses the curated view:
- `PRES.VW_PBI_DAILY_BOROUGH_METRICS`

This view is created in:
- [`sql/PRES.sql`](./sql/PRES.sql)

Power BI folder:
- [`powerbi/`](./powerbi/)  
  → Readme: [`powerbi/README.md`](./powerbi/README.md)

---

## Quick start (recommended run order)

### Option A — Daily ingestion from API (production-like)
1. Run: [`sql/data_to_raw.sql`](./sql/data_to_raw.sql)  
2. Run: [`sql/INT_Worksheet.sql`](./sql/INT_Worksheet.sql) (first backfill from current RAW)  
3. Run: [`sql/PRES.sql`](./sql/PRES.sql)  
4. Run: [`sql/Daily_Forecast.sql`](./sql/Daily_Forecast.sql)  
5. Run: [`sql/tasks.sql`](./sql/tasks.sql)  

### Option B — Historical bulk load from large CSV (optional)
1. Upload the CSV into an internal stage: `PUT ... @NYC_311_STAGE` (outside this repo)  
2. Run: [`sql/delete.sql`](./sql/delete.sql) (creates RAW table + COPY INTO)  
3. Continue with steps 2–5 from Option A

---

## What this project demonstrates

- **Snowflake data modeling**: RAW → INT → PRES layering
- **Incremental loading**: “yesterday only” logic for daily pipeline
- **Orchestration**: Snowflake Tasks + stored procedures
- **BI-ready marts**: daily metrics and mapping-friendly fields
- **ML integration**: training and generating forecasts inside Snowflake
- **Clean repo organization**: separated SQL / Python / Power BI components

---

## License / notes

This is a learning/portfolio project based on open public NYC data.
No private credentials or secrets are included in the repository.
