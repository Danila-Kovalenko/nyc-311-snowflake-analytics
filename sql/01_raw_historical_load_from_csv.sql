-- 01_raw_historical_load_from_csv.sql
-- One-time historical load of NYC 311 data from a large CSV file into RAW.RAW_311_CSV.
-- Assumes:
--   - Database NYC_311 already exists
--   - Internal stage @NYC_311_STAGE already contains the compressed CSV
--   - File format NYC_311_CSV_FORMAT is defined for the NYC Open Data CSV layout

USE DATABASE NYC_311;

-- Drop existing RAW and INT tables to start from a clean state (optional, for re-runs)
DROP TABLE IF EXISTS RAW.RAW_311_CSV;
DROP TABLE IF EXISTS INT.INT_311_SERVICE_REQUESTS;

-- RAW layer: denormalized copy of NYC 311 CSV with dates kept as strings
CREATE OR REPLACE TABLE RAW.RAW_311_CSV (
    UNIQUE_KEY_STR       VARCHAR,
    CREATED_DATE_STR     VARCHAR,
    CLOSED_DATE_STR      VARCHAR,
    AGENCY               VARCHAR,
    AGENCY_NAME          VARCHAR,
    COMPLAINT_TYPE       VARCHAR,
    DESCRIPTOR           VARCHAR,
    LOCATION_TYPE        VARCHAR,
    INCIDENT_ZIP         VARCHAR,
    INCIDENT_ADDRESS     VARCHAR,
    STREET_NAME          VARCHAR,
    STATUS               VARCHAR,
    BOROUGH              VARCHAR,
    LATITUDE             FLOAT,
    LONGITUDE            FLOAT
);

-- One-time bulk load from the staged CSV into RAW.RAW_311_CSV
COPY INTO RAW.RAW_311_CSV
FROM (
    SELECT
        $1   AS UNIQUE_KEY_STR,      -- "Unique Key"
        $2   AS CREATED_DATE_STR,    -- "Created Date" (as text)
        $3   AS CLOSED_DATE_STR,     -- "Closed Date" (as text)
        $4   AS AGENCY,              -- "Agency"
        $5   AS AGENCY_NAME,         -- "Agency Name"
        $6   AS COMPLAINT_TYPE,      -- "Complaint Type"
        $7   AS DESCRIPTOR,          -- "Descriptor"
        $8   AS LOCATION_TYPE,       -- "Location Type"
        $9   AS INCIDENT_ZIP,        -- "Incident Zip"
        $10  AS INCIDENT_ADDRESS,    -- "Incident Address"
        $11  AS STREET_NAME,         -- "Street Name"
        $19  AS STATUS,              -- "Status"
        $24  AS BOROUGH,             -- "Borough"
        TRY_TO_DOUBLE($51) AS LATITUDE,   -- "Latitude"
        TRY_TO_DOUBLE($52) AS LONGITUDE   -- "Longitude"
    FROM @NYC_311_STAGE
)
FILE_FORMAT = (FORMAT_NAME = NYC_311_CSV_FORMAT)
ON_ERROR = 'CONTINUE';  -- skip corrupted rows instead of failing the whole load

-- Optional sanity check
SELECT COUNT(*) AS ROWS_CNT
FROM RAW.RAW_311_CSV;

