# Power BI (Dashboard / Report)

This folder contains Power BI artifacts (PBIX/PBIT) and any supporting notes/screenshots.

- Back to root: [../README.md](../README.md)
- Snowflake SQL pipeline: [../sql/](../sql/)
- Python scripts: [../python/](../python/)

---

## Data source (Snowflake)

Power BI connects to Snowflake and uses the curated view:

- `PRES.VW_PBI_DAILY_BOROUGH_METRICS`

This view is created in:
- [../sql/PRES.sql](../sql/PRES.sql)

It provides:
- `METRIC_DATE`
- `BOROUGH` (cleaned)
- `COMPLAINT_TYPE`
- `REQUESTS_COUNT`, `CLOSED_COUNT`, `AVG_RESPONSE_HOURS`
- `BOROUGH_LOCATION` (text location used for map visuals)

---

## Recommended visuals (what to build)

### 1) Daily requests (Actuals)
- Line chart:
  - X: `METRIC_DATE`
  - Y: sum of `REQUESTS_COUNT`
  - Legend (optional): `BOROUGH` or `COMPLAINT_TYPE`

### 2) Map (by borough)
- Map visual using:
  - Location: `BOROUGH_LOCATION`
  - Size: `REQUESTS_COUNT`
  - Color: `BOROUGH` (categorical)

### 3) Forecast (if you use Snowflake forecast results)
Forecast results are written by:
- [../sql/Daily_Forecast.sql](../sql/Daily_Forecast.sql)

Typical table name:
- `PRES.PRES_311_FORECAST_RESULTS` (or `PRES_311_FORECAST_RESULTS`)

Create a combined “Actual + Forecast” line chart by:
- Actuals: `PRES.VW_PBI_DAILY_BOROUGH_METRICS` aggregated to daily totals
- Forecast: forecast results table (date + predicted value)

---

## Add your PBIX here

Place your Power BI file in this folder, for example:
- `nyc_311_dashboard.pbix`

Then link it from the root README and from here.

