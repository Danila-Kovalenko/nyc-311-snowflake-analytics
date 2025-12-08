-- 04_pres_metrics_and_pbi_views.sql
-- Build the PRES layer: daily aggregates for analytics and an additional view
-- optimized for Power BI mapping (clean borough names + geo-friendly strings).

USE DATABASE NYC_311;

-- Create PRES schema (idempotent)
CREATE OR REPLACE SCHEMA PRES;

------------------------------------------------------------
-- 1) Daily metrics per date / borough / complaint type
------------------------------------------------------------

CREATE OR REPLACE TABLE PRES.PRES_311_DAILY_METRICS (
    METRIC_DATE          DATE,
    BOROUGH              VARCHAR,
    COMPLAINT_TYPE       VARCHAR,
    REQUESTS_COUNT       NUMBER,
    CLOSED_COUNT         NUMBER,
    AVG_RESPONSE_HOURS   FLOAT
);

-- Rebuild metrics from INT; REGEXP_SUBSTR cleans borough labels
TRUNCATE TABLE PRES.PRES_311_DAILY_METRICS;

INSERT INTO PRES.PRES_311_DAILY_METRICS (
    METRIC_DATE,
    BOROUGH,
    COMPLAINT_TYPE,
    REQUESTS_COUNT,
    CLOSED_COUNT,
    AVG_RESPONSE_HOURS
)
SELECT
    CREATED_DATE                                        AS METRIC_DATE,
    REGEXP_SUBSTR(BOROUGH, '[^ ]+$')                   AS BOROUGH,         -- keep only the last token, e.g. "03 BRONX" -> "BRONX"
    COMPLAINT_TYPE,
    COUNT(*)                                            AS REQUESTS_COUNT,
    SUM(CASE WHEN IS_CLOSED THEN 1 ELSE 0 END)         AS CLOSED_COUNT,
    AVG(RESPONSE_TIME_HOURS)                           AS AVG_RESPONSE_HOURS
FROM INT.INT_311_SERVICE_REQUESTS
WHERE CREATED_DATE IS NOT NULL
GROUP BY
    CREATED_DATE,
    REGEXP_SUBSTR(BOROUGH, '[^ ]+$'),
    COMPLAINT_TYPE;

-- Optional: quick preview
SELECT DISTINCT BOROUGH
FROM PRES.PRES_311_DAILY_METRICS
ORDER BY BOROUGH;

------------------------------------------------------------
-- 2) View for Power BI maps: human-readable borough locations
------------------------------------------------------------

USE SCHEMA PRES;

CREATE OR REPLACE VIEW VW_PBI_DAILY_BOROUGH_METRICS AS
SELECT
    METRIC_DATE,
    BOROUGH,
    COMPLAINT_TYPE,
    REQUESTS_COUNT,
    CLOSED_COUNT,
    AVG_RESPONSE_HOURS,
    CASE 
        WHEN BOROUGH = 'BRONX'         THEN 'Bronx, New York City, USA'
        WHEN BOROUGH = 'BROOKLYN'      THEN 'Brooklyn, New York City, USA'
        WHEN BOROUGH = 'MANHATTAN'     THEN 'Manhattan, New York City, USA'
        WHEN BOROUGH = 'QUEENS'        THEN 'Queens, New York City, USA'
        WHEN BOROUGH = 'ISLAND'        THEN 'Staten Island, New York City, USA'  -- after REGEXP_SUBSTR we get just "ISLAND"
        ELSE BOROUGH
    END AS BOROUGH_LOCATION
FROM PRES.PRES_311_DAILY_METRICS;

-- Optional: preview mapping
SELECT DISTINCT BOROUGH, BOROUGH_LOCATION
FROM PRES.VW_PBI_DAILY_BOROUGH_METRICS
ORDER BY BOROUGH;
