import os
import math

# --------- SETTINGS ----------
INPUT_FILE_PATH = "C:\\Dev\\nyc-311-snowflake-analytics\\311_Service_Requests_from_2010_to_Present_20251123.csv"
OUTPUT_DIR      = "C:\\Dev\\nyc-311-snowflake-analytics\\parts_csv"
NUM_CHUNKS = 40                      # desired number of chunks
# -----------------------------


def ensure_output_dir(path: str) -> None:
    """
    Create output directory if it does not exist.
    """
    if not os.path.exists(path):
        os.makedirs(path)


def count_data_rows(file_path: str) -> int:
    """
    Count data rows in CSV file (excluding header).
    """
    with open(file_path, "r", encoding="utf-8", newline="") as f:
        for i, _ in enumerate(f):
            pass
    # i is index of last line (0-based), so total lines = i + 1
    total_lines = i + 1
    # subtract 1 for header
    data_rows = max(total_lines - 1, 0)
    return data_rows


def split_csv_into_n_chunks() -> None:
    """
    Split a large CSV file into approximately NUM_CHUNKS chunk files.
    Each chunk file will contain the header row.
    """
    ensure_output_dir(OUTPUT_DIR)

    # First pass: count data rows
    print(f"Counting data rows in '{INPUT_FILE_PATH}'...")
    data_rows = count_data_rows(INPUT_FILE_PATH)
    if data_rows == 0:
        print("No data rows found (only header or empty file). Nothing to split.")
        return

    rows_per_chunk = math.ceil(data_rows / NUM_CHUNKS)
    print(f"Total data rows: {data_rows}")
    print(f"Target chunks: {NUM_CHUNKS}")
    print(f"Rows per chunk (approx): {rows_per_chunk}")

    chunk_index = 1
    rows_in_current_chunk = 0
    current_output_file = None
    header_line = None

    with open(INPUT_FILE_PATH, "r", encoding="utf-8", newline="") as infile:
        for line_number, line in enumerate(infile):
            if line_number == 0:
                # First line is header, keep it to write into each chunk
                header_line = line
                continue

            # If this is the first row for a new chunk, open a new file
            if rows_in_current_chunk == 0:
                if current_output_file is not None:
                    current_output_file.close()

                chunk_filename = f"part_{chunk_index:02d}.csv"
                chunk_path = os.path.join(OUTPUT_DIR, chunk_filename)
                current_output_file = open(
                    chunk_path, "w", encoding="utf-8", newline=""
                )
                # Write header into new chunk
                current_output_file.write(header_line)
                print(f"Started new chunk: {chunk_filename}")
                chunk_index += 1

            # Write current data row
            current_output_file.write(line)
            rows_in_current_chunk += 1

            # If chunk reached the limit, reset counter to start a new chunk
            if rows_in_current_chunk >= rows_per_chunk:
                rows_in_current_chunk = 0

    if current_output_file is not None:
        current_output_file.close()

    print("CSV splitting completed.")
    print(f"Chunks created in folder: {OUTPUT_DIR}")


if __name__ == "__main__":
    split_csv_into_n_chunks()
