# Python

This folder contains local helper scripts used during development and bulk loading.
The “production-like” daily ingestion is implemented inside Snowflake via Snowpark
(see [`../sql/02_raw_http_ingest_snowpark.sql`](../sql/02_raw_http_ingest_snowpark.sql)).

Back to root: [../README.md](../README.md)

---

## Files

- **[`fetch_311_to_snowflake.py`](./fetch_311_to_snowflake.py)**  
  Development script used to fetch NYC 311 data and load it into Snowflake (used early in the project).

- **[`split_csv_into_40_parts.py`](./split_csv_into_40_parts.py)**  
  Utility to split a very large historical CSV into multiple chunks (e.g., 40 parts).

- **[`load_csv_chunks_to_raw.py`](./load_csv_chunks_to_raw.py)**  
  Loads multiple CSV chunks into Snowflake RAW (bulk loading helper).

---

## When to use Python vs Snowflake-only approach

- Use **Python scripts** if:
  - you need to pre-process a huge CSV locally
  - you want custom local loading logic

- Use **Snowflake (COPY INTO / Tasks / Snowpark SP)** if:
  - you want a “production-like” daily pipeline running inside Snowflake:
    - [`../sql/02_raw_http_ingest_snowpark.sql`](../sql/02_raw_http_ingest_snowpark.sql)
    - [`../sql/06_tasks_daily_etl.sql`](../sql/06_tasks_daily_etl.sql)

---

## Security note (important)
Do NOT commit real credentials to GitHub.
- Do not hardcode Snowflake passwords in code.
- Prefer environment variables or local config files excluded via `.gitignore`.
