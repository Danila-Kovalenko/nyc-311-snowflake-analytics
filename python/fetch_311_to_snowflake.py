import json
import datetime
import requests
import snowflake.connector

# --------------- SNOWFLAKE SETTINGS ---------------
SNOWFLAKE_ACCOUNT   = "---"      # <-- СЮДА твой account, типа "xy12345.eu-central-1"
SNOWFLAKE_USER      = "---"         # <-- Твой логин в Snowflake
SNOWFLAKE_PASSWORD  = "---"   # Your Snowflake password
SNOWFLAKE_WAREHOUSE = "---"      # Your Snowflake warehouse name
SNOWFLAKE_DATABASE  = "NYC_311"
SNOWFLAKE_SCHEMA    = "RAW"
# -------------------------------------------------


def get_snowflake_connection():
    """
    Create a simple Snowflake connection using credentials defined above.
    """
    ctx = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    return ctx


def fetch_311_requests_for_date(target_date: datetime.date):
    """
    Fetch all 311 requests for a given date using the 'created_date' field.
    """
    base_url = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

    start_dt = datetime.datetime.combine(target_date, datetime.time.min)
    end_dt = start_dt + datetime.timedelta(days=1)

    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%S")

    where_clause = (
        f"created_date >= '{start_iso}' AND created_date < '{end_iso}'"
    )

    params = {
        "$where": where_clause,
        "$limit": 50000,
        "$order": "created_date",
    }

    print("Requesting 311 data from API...")
    response = requests.get(base_url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    if not isinstance(data, list):
        raise ValueError("Expected a list of JSON objects from 311 API")

    return data


def load_raw_to_snowflake(target_date: datetime.date, records):
    """
    Insert JSON records into RAW.RAW_311_REQUESTS, one row per JSON object.
    Uses INSERT ... SELECT to avoid PARSE_JSON() inside VALUES().
    """
    ctx = get_snowflake_connection()
    try:
        cs = ctx.cursor()

        file_name = f"API_{target_date.strftime('%Y%m%d')}.json"

        insert_sql = """
            INSERT INTO RAW.RAW_311_REQUESTS (
                LOAD_DATE,
                FILE_NAME,
                ROW_NUMBER_IN_FILE,
                RAW_PAYLOAD
            )
            SELECT %s, %s, %s, PARSE_JSON(%s)
        """

        row_number = 0
        for row in records:
            row_number += 1
            raw_json_str = json.dumps(row)

            cs.execute(
                insert_sql,
                (
                    target_date,   # LOAD_DATE
                    file_name,     # FILE_NAME
                    row_number,    # ROW_NUMBER_IN_FILE
                    raw_json_str,  # RAW_PAYLOAD (string for PARSE_JSON)
                ),
            )

        print(
            f"Inserted {row_number} rows into RAW.RAW_311_REQUESTS "
            f"for date {target_date}"
        )

    finally:
        ctx.close()


def main():
    """
    Main entry point: load data for yesterday into RAW layer.
    """
    today = datetime.date.today()
    target_date = today - datetime.timedelta(days=1)

    print(f"Loading 311 data for {target_date}...")

    records = fetch_311_requests_for_date(target_date)
    print(f"Fetched {len(records)} records from API")

    if records:
        load_raw_to_snowflake(target_date, records)
    else:
        print("No records for this date, nothing to load.")


if __name__ == "__main__":
    main()
