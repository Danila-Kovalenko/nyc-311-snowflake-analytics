-- 06_tasks_daily_etl.sql
-- Define daily ETL procedures and a Snowflake TASK that:
--   1) Loads yesterday's data from RAW into INT
--   2) Refreshes PRES metrics and forecast base for yesterday

USE DATABASE NYC_311;
USE SCHEMA INT;

------------------------------------------------------------
-- 1) Procedure: RAW -> INT for yesterday
------------------------------------------------------------

CREATE OR REPLACE PROCEDURE INT.SP_LOAD_311_FROM_RAW_YESTERDAY()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_yesterday DATE := DATEADD('day', -1, CURRENT_DATE());
BEGIN
    -- Insert only rows for yesterday and skip duplicates by UNIQUE_KEY
    INSERT INTO INT.INT_311_SERVICE_REQUESTS (
        UNIQUE_KEY,
        CREATED_AT,
        CLOSED_AT,
        CREATED_DATE,
        CLOSED_DATE,
        AGENCY,
        AGENCY_NAME,
        COMPLAINT_TYPE,
        DESCRIPTOR,
        STATUS,
        BOROUGH,
        INCIDENT_ZIP,
        LATITUDE,
        LONGITUDE,
        RESPONSE_TIME_HOURS,
        IS_CLOSED
    )
    SELECT
        TRY_TO_NUMBER(r.UNIQUE_KEY_STR)                                        AS UNIQUE_KEY,
        TRY_TO_TIMESTAMP_NTZ(r.CREATED_DATE_STR, 'MM/DD/YYYY HH12:MI:SS AM')   AS CREATED_AT,
        TRY_TO_TIMESTAMP_NTZ(r.CLOSED_DATE_STR,  'MM/DD/YYYY HH12:MI:SS AM')   AS CLOSED_AT,
        TRY_TO_DATE(r.CREATED_DATE_STR, 'MM/DD/YYYY HH12:MI:SS AM')            AS CREATED_DATE,
        TRY_TO_DATE(r.CLOSED_DATE_STR,  'MM/DD/YYYY HH12:MI:SS AM')            AS CLOSED_DATE,
        r.AGENCY,
        r.AGENCY_NAME,
        r.COMPLAINT_TYPE,
        r.DESCRIPTOR,
        r.STATUS,
        r.BOROUGH,
        r.INCIDENT_ZIP,
        r.LATITUDE,
        r.LONGITUDE,
        CASE
            WHEN TRY_TO_TIMESTAMP_NTZ(r.CREATED_DATE_STR, 'MM/DD/YYYY HH12:MI:SS AM') IS NOT NULL
             AND TRY_TO_TIMESTAMP_NTZ(r.CLOSED_DATE_STR,  'MM/DD/YYYY HH12:MI:SS AM') IS NOT NULL
            THEN
                DATEDIFF(
                    'second',
                    TRY_TO_TIMESTAMP_NTZ(r.CREATED_DATE_STR, 'MM/DD/YYYY HH12:MI:SS AM'),
                    TRY_TO_TIMESTAMP_NTZ(r.CLOSED_DATE_STR,  'MM/DD/YYYY HH12:MI:SS AM')
                ) / 3600.0
            ELSE NULL
        END                                                                     AS RESPONSE_TIME_HOURS,
        CASE
            WHEN r.STATUS = 'Closed' THEN TRUE
            ELSE FALSE
        END                                                                     AS IS_CLOSED
    FROM RAW.RAW_311_CSV r
    WHERE TRY_TO_DATE(r.CREATED_DATE_STR, 'MM/DD/YYYY HH12:MI:SS AM') = v_yesterday
      AND NOT EXISTS (
          SELECT 1
          FROM INT.INT_311_SERVICE_REQUESTS t
          WHERE t.UNIQUE_KEY = TRY_TO_NUMBER(r.UNIQUE_KEY_STR)
      );

    RETURN 'Loaded data for ' || v_yesterday;
END;
$$;

------------------------------------------------------------
-- 2) Procedure: INT -> PRES (metrics + forecast base) for yesterday
------------------------------------------------------------

CREATE OR REPLACE PROCEDURE PRES.SP_REFRESH_311_PRES_FOR_YESTERDAY()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_yesterday DATE := DATEADD('day', -1, CURRENT_DATE());
BEGIN
    -- Remove old data for yesterday in metrics
    DELETE FROM PRES.PRES_311_DAILY_METRICS
    WHERE METRIC_DATE = v_yesterday;

    -- Insert fresh metrics for yesterday
    INSERT INTO PRES.PRES_311_DAILY_METRICS (
        METRIC_DATE,
        BOROUGH,
        COMPLAINT_TYPE,
        REQUESTS_COUNT,
        CLOSED_COUNT,
        AVG_RESPONSE_HOURS
    )
    SELECT
        i.CREATED_DATE                                      AS METRIC_DATE,
        REGEXP_SUBSTR(i.BOROUGH, '[^ ]+$')                  AS BOROUGH,
        i.COMPLAINT_TYPE,
        COUNT(*)                                            AS REQUESTS_COUNT,
        SUM(CASE WHEN i.IS_CLOSED THEN 1 ELSE 0 END)        AS CLOSED_COUNT,
        AVG(i.RESPONSE_TIME_HOURS)                          AS AVG_RESPONSE_HOURS
    FROM INT.INT_311_SERVICE_REQUESTS i
    WHERE i.CREATED_DATE = v_yesterday
    GROUP BY
        i.CREATED_DATE,
        REGEXP_SUBSTR(i.BOROUGH, '[^ ]+$'),
        i.COMPLAINT_TYPE;

    -- Remove old data for yesterday in forecast base
    DELETE FROM PRES.PRES_311_DAILY_FORECAST_BASE
    WHERE TARGET_DATE = v_yesterday;

    -- Insert fresh daily totals for forecast base
    INSERT INTO PRES.PRES_311_DAILY_FORECAST_BASE (
        TARGET_DATE,
        TOTAL_REQUESTS,
        DOW_ISO,
        IS_WEEKEND,
        MONTH_NUM
    )
    SELECT
        i.CREATED_DATE                         AS TARGET_DATE,
        COUNT(*)                               AS TOTAL_REQUESTS,
        DAYOFWEEKISO(i.CREATED_DATE)          AS DOW_ISO,
        CASE 
            WHEN DAYOFWEEKISO(i.CREATED_DATE) IN (6, 7) THEN TRUE 
            ELSE FALSE 
        END                                    AS IS_WEEKEND,
        MONTH(i.CREATED_DATE)                  AS MONTH_NUM
    FROM INT.INT_311_SERVICE_REQUESTS i
    WHERE i.CREATED_DATE = v_yesterday
    GROUP BY
        i.CREATED_DATE;

    RETURN 'Refreshed PRES for ' || v_yesterday;
END;
$$;

------------------------------------------------------------
-- 3) Daily TASK: run both procedures at 03:00 Berlin time
------------------------------------------------------------

CREATE OR REPLACE TASK PRES.TASK_311_DAILY_ETL
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 3 * * * Europe/Berlin'   -- every day at 03:00 Berlin time
    COMMENT = 'Daily ETL: load yesterday from RAW to INT and refresh PRES'
AS
BEGIN
    CALL INT.SP_LOAD_311_FROM_RAW_YESTERDAY();
    CALL PRES.SP_REFRESH_311_PRES_FOR_YESTERDAY();
END;

-- Enable the task
ALTER TASK PRES.TASK_311_DAILY_ETL RESUME;

-- Optional: inspect task definitions in PRES schema
SHOW TASKS IN SCHEMA PRES;
