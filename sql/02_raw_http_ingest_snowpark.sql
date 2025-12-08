-- 02_raw_http_ingest_snowpark.sql
-- Snowpark Python–based ingestion of daily NYC 311 data directly from the public API
-- into RAW.RAW_311_CSV, using External Access Integration and a scheduled TASK.

------------------------------------------------------------
-- 1) Network rule + External Access Integration (ACCOUNTADMIN)
------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

-- Allow outbound HTTPS traffic to NYC Open Data host
CREATE OR REPLACE NETWORK RULE NYC311_API_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('data.cityofnewyork.us:443');

-- External access integration referencing the network rule
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION NYC311_API_INTEGRATION
    ALLOWED_NETWORK_RULES = (NYC311_API_RULE)
    ENABLED = TRUE;

------------------------------------------------------------
-- 2) Snowpark Python stored procedure: ingest "yesterday"
------------------------------------------------------------

USE DATABASE NYC_311;
USE SCHEMA RAW;

CREATE OR REPLACE PROCEDURE SP_INGEST_311_YESTERDAY_HTTP()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'run'
EXTERNAL_ACCESS_INTEGRATIONS = (NYC311_API_INTEGRATION)
AS
$$
import datetime
import requests
from snowflake.snowpark import Session

NYC_311_ENDPOINT = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PAGE_LIMIT = 50000  # rows per API call


def iso_to_us_datetime_str(iso_str):
    """
    Convert ISO datetime from API, e.g. '2025-11-22T00:00:07.000'
    to US format 'MM/DD/YYYY HH:MI:SS AM' used in RAW.RAW_311_CSV.
    If input is None or empty, return empty string.
    """
    if not iso_str:
        return ""
    try:
        clean = iso_str.replace("Z", "")
        dt = datetime.datetime.fromisoformat(clean)
        return dt.strftime("%m/%d/%Y %I:%M:%S %p")
    except Exception:
        return ""


def fetch_311_yesterday():
    """
    Fetch all 311 requests for yesterday using the NYC Open Data API.
    Pagination is handled via $limit / $offset.
    """
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)

    start_dt = datetime.datetime.combine(yesterday, datetime.time.min)
    end_dt = start_dt + datetime.timedelta(days=1)

    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%S")

    where_clause = (
        f"created_date >= '{start_iso}' AND created_date < '{end_iso}'"
    )

    print(f"Fetching 311 data for {yesterday} from API...")
    all_rows = []
    offset = 0

    while True:
        params = {
            "$where": where_clause,
            "$limit": PAGE_LIMIT,
            "$offset": offset,
            "$order": "created_date",
        }
        resp = requests.get(NYC_311_ENDPOINT, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        if not data:
            break

        all_rows.extend(data)
        print(f"Fetched {len(data)} rows, total so far: {len(all_rows)}")

        if len(data) < PAGE_LIMIT:
            break

        offset += PAGE_LIMIT

    print(f"Total rows fetched for {yesterday}: {len(all_rows)}")
    return all_rows, yesterday


def transform_to_raw_rows(records):
    """
    Transform API records into list of tuples matching RAW.RAW_311_CSV:
      UNIQUE_KEY_STR,
      CREATED_DATE_STR,
      CLOSED_DATE_STR,
      AGENCY,
      AGENCY_NAME,
      COMPLAINT_TYPE,
      DESCRIPTOR,
      LOCATION_TYPE,
      INCIDENT_ZIP,
      INCIDENT_ADDRESS,
      STREET_NAME,
      STATUS,
      BOROUGH,
      LATITUDE,
      LONGITUDE
    """
    rows = []

    for r in records:
        unique_key_str = r.get("unique_key")
        created_date_iso = r.get("created_date")
        closed_date_iso = r.get("closed_date")

        created_date_str = iso_to_us_datetime_str(created_date_iso)
        closed_date_str = iso_to_us_datetime_str(closed_date_iso)

        agency = r.get("agency")
        agency_name = r.get("agency_name")
        complaint_type = r.get("complaint_type")
        descriptor = r.get("descriptor")
        location_type = r.get("location_type")
        incident_zip = r.get("incident_zip")
        incident_address = r.get("incident_address")
        street_name = r.get("street_name")
        status = r.get("status")
        borough = r.get("borough")

        latitude = None
        longitude = None

        # latitude/longitude can be flat fields or nested under "location"
        if "latitude" in r and "longitude" in r:
            try:
                latitude = float(r.get("latitude"))
            except (TypeError, ValueError):
                latitude = None
            try:
                longitude = float(r.get("longitude"))
            except (TypeError, ValueError):
                longitude = None
        elif "location" in r and isinstance(r["location"], dict):
            loc = r["location"]
            try:
                latitude = float(loc.get("latitude"))
            except (TypeError, ValueError):
                latitude = None
            try:
                longitude = float(loc.get("longitude"))
            except (TypeError, ValueError):
                longitude = None

        rows.append(
            (
                unique_key_str,
                created_date_str,
                closed_date_str,
                agency,
                agency_name,
                complaint_type,
                descriptor,
                location_type,
                incident_zip,
                incident_address,
                street_name,
                status,
                borough,
                latitude,
                longitude,
            )
        )

    return rows


def run(session: Session) -> str:
    """
    Entry point for Snowpark Python stored procedure.
    Fetch yesterday's 311 data from the API and append it to RAW.RAW_311_CSV.
    """
    records, yesterday = fetch_311_yesterday()
    if not records:
        return f"No records fetched for {yesterday}."

    rows = transform_to_raw_rows(records)

    columns = [
        "UNIQUE_KEY_STR",
        "CREATED_DATE_STR",
        "CLOSED_DATE_STR",
        "AGENCY",
        "AGENCY_NAME",
        "COMPLAINT_TYPE",
        "DESCRIPTOR",
        "LOCATION_TYPE",
        "INCIDENT_ZIP",
        "INCIDENT_ADDRESS",
        "STREET_NAME",
        "STATUS",
        "BOROUGH",
        "LATITUDE",
        "LONGITUDE",
    ]

    # Create a Snowpark DataFrame and append it into RAW.RAW_311_CSV
    df = session.create_dataframe(rows, schema=columns)
    df.write.mode("append").save_as_table("RAW.RAW_311_CSV")

    return f"Inserted {len(rows)} rows for {yesterday} into RAW.RAW_311_CSV."
$$;

------------------------------------------------------------
-- 3) Scheduled TASK to run the HTTP ingestion daily
------------------------------------------------------------

USE DATABASE NYC_311;
USE SCHEMA RAW;

CREATE OR REPLACE TASK TASK_311_INGEST_HTTP
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 1 * * * Europe/Berlin'  -- run every day at 01:00 Berlin time
    COMMENT = 'Daily ingestion of yesterday''s 311 data from NYC API into RAW.RAW_311_CSV'
AS
CALL RAW.SP_INGEST_311_YESTERDAY_HTTP();

-- Enable the task
ALTER TASK TASK_311_INGEST_HTTP RESUME;

-- Optional: inspect task metadata
SHOW TASKS IN SCHEMA RAW;
