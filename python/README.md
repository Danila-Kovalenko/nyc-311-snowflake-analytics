# Python — local utilities & loaders

This folder contains Python scripts used during development and/or for bulk loading.

- Back to root: [../README.md](../README.md)
- Snowflake SQL pipeline: [../sql/](../sql/)
- Power BI assets: [../powerbi/](../powerbi/)

---

## What lives here

### Bulk loading / staging helpers
- **`load_csv_chunks_to_raw.py`** — loads CSV chunks into Snowflake RAW (used during the “8 GB CSV” phase).  
  Link: [load_csv_chunks_to_raw.py](./load_csv_chunks_to_raw.py)

> If you have other helper scripts in this folder (e.g., splitting CSV, quick API fetch tests), keep them here too.  
> The recommended pattern is: small, single-purpose scripts with clear CLI arguments.

---

## Suggested minimal structure (recommended)

If you continue improving this folder, this is a clean layout recruiters like:

- `requirements.txt` (exact packages used)
- `load_csv_chunks_to_raw.py` (bulk loader)
- `split_csv_into_40_parts.py` (optional utility, if you still keep it)
- `README.md` (this file)

---

## Typical usage flow

1) **Historical bulk load** (optional):
- Split / upload CSV chunks
- Load into `RAW.RAW_311_CSV` (or `RAW.NEW_RAW_REQUESTS` depending on your RAW design)

2) **Daily ingestion (production-like)**:
- Daily ingestion is handled inside Snowflake via Snowpark procedure in:
  - [../sql/data_to_raw.sql](../sql/data_to_raw.sql)
- Daily RAW→INT→PRES is orchestrated by:
  - [../sql/tasks.sql](../sql/tasks.sql)