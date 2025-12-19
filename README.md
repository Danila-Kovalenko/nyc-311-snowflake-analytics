# nyc-311-snowflake-analytics

Portfolio project: **end-to-end data pipeline + BI + forecasting** using **NYC 311 Service Requests** data.

This repo demonstrates a realistic analytics workflow:
- **RAW**: ingest data (historical CSV or daily API)
- **INT**: clean & type data, add derived fields
- **PRES**: build BI-ready marts + Power BI views
- **Automation**: daily Snowflake Tasks
- **Forecasting**: Snowflake ML time-series forecast + backtesting
- **Power BI**: map and line charts (with screenshots)

---

## Repository structure

- **[`sql/`](./sql/)** — Snowflake SQL scripts (RAW → INT → PRES, tasks, forecasting, evaluation)  
  → docs: [`sql/README.md`](./sql/README.md)

- **[`python/`](./python/)** — local helper scripts (bulk CSV loading / splitting; dev utilities)  
  → docs: [`python/README.md`](./python/README.md)

- **[`powerbi/`](./powerbi/)** — Power BI reports (`.pbix`) + screenshots (`.png`)  
  → docs: [`powerbi/README.md`](./powerbi/README.md)

---

## Data source

NYC Open Data (Socrata):
- API (JSON): `https://data.cityofnewyork.us/resource/erm2-nwe9.json`
- Historical CSV export is also available on NYC Open Data (used for the bulk load scenario)

---

## Architecture (Snowflake layers)

### RAW (landing)
Goal: store data close to the source (minimal transforms).
- Table: `RAW.RAW_311_CSV`
- Ingest options:
  - Historical **CSV bulk load** (Stage → COPY INTO): [`sql/01_raw_historical_load_from_csv.sql`](./sql/01_raw_historical_load_from_csv.sql)
  - Daily **API ingestion** (Snowpark Python SP + Task): [`sql/02_raw_http_ingest_snowpark.sql`](./sql/02_raw_http_ingest_snowpark.sql)

### INT (clean + typed)
Goal: parse datetimes, enforce types, compute derived fields.
- Table: `INT.INT_311_SERVICE_REQUESTS`
- Adds:
  - `RESPONSE_TIME_HOURS`
  - `IS_CLOSED`
- Script: [`sql/03_int_build_and_backfill.sql`](./sql/03_int_build_and_backfill.sql)

### PRES (marts for analytics + BI)
Goal: aggregate data for dashboarding.
- Table: `PRES.PRES_311_DAILY_METRICS`
- Power BI view:
  - `PRES.VW_PBI_DAILY_BOROUGH_METRICS` (includes `BOROUGH_LOCATION` for maps)
- Script: [`sql/04_pres_metrics_and_pbi_views.sql`](./sql/04_pres_metrics_and_pbi_views.sql)

---

## Forecasting (Snowflake ML)

- Build daily totals base: `PRES.PRES_311_DAILY_FORECAST_BASE`
- Train Snowflake ML Forecast model
- Write predictions to `PRES_311_FORECAST_RESULTS`
- Script: [`sql/05_forecast_daily_requests.sql`](./sql/05_forecast_daily_requests.sql)

Backtesting / evaluation:
- Compute MAE / RMSE / MAPE on a 60-day holdout window  
- Script: [`sql/07_forecast_evaluation.sql`](./sql/07_forecast_evaluation.sql)

---

## Automation (daily pipeline)

Daily ETL procedures + task:
- Script: [`sql/06_tasks_daily_etl.sql`](./sql/06_tasks_daily_etl.sql)
- Runs every day:
  1) load yesterday RAW → INT
  2) refresh yesterday PRES marts + forecast base

Daily API ingest task (optional, if you use API ingestion):
- Script: [`sql/02_raw_http_ingest_snowpark.sql`](./sql/02_raw_http_ingest_snowpark.sql)

---

## Quick start (recommended)

### Option A — Historical CSV bulk load + daily ETL
1. Run: [`sql/01_raw_historical_load_from_csv.sql`](./sql/01_raw_historical_load_from_csv.sql)
2. Run: [`sql/03_int_build_and_backfill.sql`](./sql/03_int_build_and_backfill.sql)
3. Run: [`sql/04_pres_metrics_and_pbi_views.sql`](./sql/04_pres_metrics_and_pbi_views.sql)
4. Run: [`sql/05_forecast_daily_requests.sql`](./sql/05_forecast_daily_requests.sql)
5. Run: [`sql/07_forecast_evaluation.sql`](./sql/07_forecast_evaluation.sql)
6. Run (enable daily task): [`sql/06_tasks_daily_etl.sql`](./sql/06_tasks_daily_etl.sql)

### Option B — Daily API ingestion + daily ETL
1. Run: [`sql/02_raw_http_ingest_snowpark.sql`](./sql/02_raw_http_ingest_snowpark.sql)
2. Run: [`sql/03_int_build_and_backfill.sql`](./sql/03_int_build_and_backfill.sql)
3. Run: [`sql/04_pres_metrics_and_pbi_views.sql`](./sql/04_pres_metrics_and_pbi_views.sql)
4. Run: [`sql/05_forecast_daily_requests.sql`](./sql/05_forecast_daily_requests.sql)
5. Run: [`sql/06_tasks_daily_etl.sql`](./sql/06_tasks_daily_etl.sql)

---

## Power BI

Power BI reports and screenshots are in:
- [`powerbi/`](./powerbi/) → [`powerbi/README.md`](./powerbi/README.md)

Screenshots (PNG):
- Map: [`powerbi/map.png`](./powerbi/map.png)
- Line chart: [`powerbi/line_diagram.png`](./powerbi/line_diagram.png)
- Forecast line chart: [`powerbi/forecast_line_diagram.png`](./powerbi/forecast_line_diagram.png)

---

## Security note

This repository should not contain real credentials.
- Do **not** commit Snowflake passwords.
- Prefer environment variables / local config files that are excluded via `.gitignore`.
