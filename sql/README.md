# SQL (Snowflake) — End-to-End Pipeline

This folder contains the Snowflake-side part of the project: **RAW → INT → PRES + daily tasks + forecasting**.

- Back to root: [../README.md](../README.md)
- Python scripts: [../python/](../python/)
- Power BI assets: [../powerbi/](../powerbi/)

---

## Files (what each script does)

### 1) Ingestion to RAW (NYC Open Data API → RAW)
- **`data_to_raw.sql`** — creates external access integration (egress rule), defines a **Snowpark Python stored procedure** that pulls *yesterday’s* data from NYC Open Data API and appends to `RAW.RAW_311_CSV`. Also creates a daily ingest task.  
  Link: [data_to_raw.sql](./data_to_raw.sql)

### 2) (Optional) Full historical load from large CSV (Stage → RAW)
- **`delete.sql`** — despite its name, this file also contains:
  - `DROP TABLE ...`
  - `CREATE TABLE RAW.RAW_311_CSV`
  - `COPY INTO RAW.RAW_311_CSV FROM @NYC_311_STAGE ...`
  
  Use this **only if** you load a large historical CSV into an internal stage and then COPY it into RAW.  
  Link: [delete.sql](./delete.sql)

### 3) Transform RAW → INT (typed table, response time, closure flag)
- **`INT_Worksheet.sql`** — creates `INT.INT_311_SERVICE_REQUESTS` and fills it from `RAW.RAW_311_CSV` (casts, dates/timestamps, `RESPONSE_TIME_HOURS`, `IS_CLOSED`).  
  Link: [INT_Worksheet.sql](./INT_Worksheet.sql)

### 4) Build PRES marts (daily metrics for BI)
- **`PRES.sql`** — creates `PRES.PRES_311_DAILY_METRICS`, populates it from INT, cleans borough names, and creates the Power BI-facing view `PRES.VW_PBI_DAILY_BOROUGH_METRICS` including `BOROUGH_LOCATION` for mapping.  
  Link: [PRES.sql](./PRES.sql)

### 5) Orchestration (daily ETL tasks)
- **`tasks.sql`** — creates:
  - `INT.SP_LOAD_311_FROM_RAW_YESTERDAY()` (incremental RAW→INT)
  - `PRES.SP_REFRESH_311_PRES_FOR_YESTERDAY()` (refreshes PRES metrics + forecast base for yesterday)
  - `PRES.TASK_311_DAILY_ETL` (runs both procedures daily)
  
  Link: [tasks.sql](./tasks.sql)

### 6) Forecasting (Snowflake ML FORECAST)
- **`Daily_Forecast.sql`** — creates a daily time-series base table (`PRES.PRES_311_DAILY_FORECAST_BASE`), training views, trains `SNOWFLAKE.ML.FORECAST` model, and writes predictions into `PRES_311_FORECAST_RESULTS`.  
  Link: [Daily_Forecast.sql](./Daily_Forecast.sql)

### 7) Sanity checks / status
- **`Initial_Table.sql`** — small helper queries to verify data presence (counts, load dates).  
  Link: [Initial_Table.sql](./Initial_Table.sql)

---

## Recommended run order (first-time setup)

### A) If you are doing ONLY daily incremental ingestion from API
1. Run: [data_to_raw.sql](./data_to_raw.sql)  
2. Run: [INT_Worksheet.sql](./INT_Worksheet.sql)  (first full insert from current RAW data)
3. Run: [PRES.sql](./PRES.sql)
4. Run: [Daily_Forecast.sql](./Daily_Forecast.sql)
5. Run: [tasks.sql](./tasks.sql)

### B) If you load a large historical CSV first (Stage → RAW)
1. Upload the CSV into an internal stage (`PUT ... @NYC_311_STAGE`) and define file format (done outside or in your own setup script)
2. Run: [delete.sql](./delete.sql)  (creates RAW table + COPY INTO)
3. Then run steps 2–5 from section A

---

## Tables / Views created by these scripts

### RAW
- `RAW.RAW_311_CSV`

### INT
- `INT.INT_311_SERVICE_REQUESTS`

### PRES
- `PRES.PRES_311_DAILY_METRICS`
- `PRES.VW_PBI_DAILY_BOROUGH_METRICS`
- `PRES.PRES_311_DAILY_FORECAST_BASE`
- `PRES.PRES_311_FORECAST_RESULTS` (or `PRES_311_FORECAST_RESULTS` depending on script)

---

## Notes
- The project intentionally keeps **RAW** close to source format (strings for timestamps) and performs type casting in **INT**.
- If borough values contain prefixes (e.g., `01 BROOKLYN`), the cleaning step in [PRES.sql](./PRES.sql) uses:
  `REGEXP_SUBSTR(BOROUGH, '[^ ]+$')` to keep only the last token (`BROOKLYN`).

