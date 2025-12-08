import os
import csv
import json
import datetime
import snowflake.connector

# --------------- SNOWFLAKE SETTINGS ---------------
SNOWFLAKE_ACCOUNT   = "HKHBZCU-XW96445"      # <-- СЮДА твой account, типа "xy12345.eu-central-1"
SNOWFLAKE_USER      = "DANHOEFFLIN"         # <-- Твой логин в Snowflake
SNOWFLAKE_PASSWORD  = "88005553535Poz"   # Your Snowflake password
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"      # Your Snowflake warehouse name
SNOWFLAKE_DATABASE  = "NYC_311"
SNOWFLAKE_SCHEMA    = "RAW"
# --------------------------------------------------


# --------------- CSV CHUNKS SETTINGS --------------
CHUNKS_DIR = r"C:\Dev\nyc-311-snowflake-analytics\parts_csv"  # Folder with part_XX.csv
PRINT_EVERY_N_ROWS = 10_000                                   # Progress log frequency
# --------------------------------------------------


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


def list_csv_files(directory: str):
    """
    Return sorted list of CSV file names in the given directory.
    """
    files = []
    for name in os.listdir(directory):
        if name.lower().endswith(".csv"):
            files.append(name)
    files.sort()
    return files


def load_csv_chunks_to_raw():
    """
    Load all CSV chunk files from CHUNKS_DIR into RAW.RAW_311_REQUESTS.
    Each CSV row becomes one JSON object stored in RAW_PAYLOAD (VARIANT).
    """
    if not os.path.isdir(CHUNKS_DIR):
        raise ValueError(f"Directory does not exist: {CHUNKS_DIR}")

    csv_files = list_csv_files(CHUNKS_DIR)
    if not csv_files:
        print(f"No CSV files found in {CHUNKS_DIR}")
        return

    print(f"Found {len(csv_files)} CSV files in {CHUNKS_DIR}")

    ctx = get_snowflake_connection()
    try:
        cs = ctx.cursor()

        insert_sql = """
            INSERT INTO RAW.RAW_311_REQUESTS (
                LOAD_DATE,
                FILE_NAME,
                ROW_NUMBER_IN_FILE,
                RAW_PAYLOAD
            )
            SELECT %s, %s, %s, PARSE_JSON(%s)
        """

        # Use today's date as technical LOAD_DATE for this bulk import
        load_date = datetime.date.today()

        total_inserted = 0

        for file_name in csv_files:
            file_path = os.path.join(CHUNKS_DIR, file_name)
            print(f"Processing file: {file_name}")

            row_number_in_file = 0

            with open(file_path, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    row_number_in_file += 1

                    # Convert row (dict) to JSON string
                    raw_json_str = json.dumps(row)

                    # Insert single row into Snowflake
                    cs.execute(
                        insert_sql,
                        (
                            load_date,           # LOAD_DATE
                            file_name,           # FILE_NAME
                            row_number_in_file,  # ROW_NUMBER_IN_FILE
                            raw_json_str,        # RAW_PAYLOAD as JSON string
                        ),
                    )

                    total_inserted += 1
                    if total_inserted % PRINT_EVERY_N_ROWS == 0:
                        print(f"Inserted {total_inserted} rows so far...")

            print(f"Finished file: {file_name} (rows: {row_number_in_file})")

        print(f"All CSV chunks loaded into RAW.RAW_311_REQUESTS. Total rows: {total_inserted}")

    finally:
        ctx.close()


def main():
    """
    Main entry point: load all CSV chunks into RAW layer.
    """
    load_csv_chunks_to_raw()


if __name__ == "__main__":
    main()
