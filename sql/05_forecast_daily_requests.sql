-- 05_forecast_daily_requests.sql
-- Build daily forecast base table, train a Snowflake ML Forecast model
-- on historical daily request counts and store 14-day ahead forecasts.

USE DATABASE NYC_311;

------------------------------------------------------------
-- 1) Forecast base table: daily total requests
------------------------------------------------------------

CREATE OR REPLACE TABLE PRES.PRES_311_DAILY_FORECAST_BASE (
    TARGET_DATE       DATE,
    TOTAL_REQUESTS    NUMBER,
    DOW_ISO           NUMBER,  -- 1 = Monday, 7 = Sunday
    IS_WEEKEND        BOOLEAN,
    MONTH_NUM         NUMBER   -- 1..12
);

-- Populate the forecast base from INT (full history)
INSERT INTO PRES.PRES_311_DAILY_FORECAST_BASE (
    TARGET_DATE,
    TOTAL_REQUESTS,
    DOW_ISO,
    IS_WEEKEND,
    MONTH_NUM
)
SELECT
    CREATED_DATE                         AS TARGET_DATE,
    COUNT(*)                             AS TOTAL_REQUESTS,
    DAYOFWEEKISO(CREATED_DATE)          AS DOW_ISO,
    CASE 
        WHEN DAYOFWEEKISO(CREATED_DATE) IN (6, 7) THEN TRUE 
        ELSE FALSE 
    END                                  AS IS_WEEKEND,
    MONTH(CREATED_DATE)                  AS MONTH_NUM
FROM INT.INT_311_SERVICE_REQUESTS
WHERE CREATED_DATE IS NOT NULL
GROUP BY
    CREATED_DATE;

-- Optional preview of base data
SELECT *
FROM PRES.PRES_311_DAILY_FORECAST_BASE
ORDER BY TARGET_DATE DESC
LIMIT 100;

------------------------------------------------------------
-- 2) View used for model training (simple: timestamp + target only)
------------------------------------------------------------

USE SCHEMA PRES;

CREATE OR REPLACE VIEW VW_311_FORECAST_TRAIN_SIMPLE AS
SELECT
    TO_TIMESTAMP_NTZ(TARGET_DATE) AS TARGET_TS,        -- timestamp for time series
    TOTAL_REQUESTS::FLOAT         AS TOTAL_REQUESTS    -- numeric target
FROM PRES_311_DAILY_FORECAST_BASE
WHERE TARGET_DATE IS NOT NULL
ORDER BY TARGET_DATE;

-- Optional preview of training set
SELECT *
FROM PRES.VW_311_FORECAST_TRAIN_SIMPLE
ORDER BY TARGET_TS DESC
LIMIT 10;

------------------------------------------------------------
-- 3) Train Snowflake ML forecast model
------------------------------------------------------------

CREATE OR REPLACE SNOWFLAKE.ML.FORECAST NYC311_DAILY_REQUESTS_MODEL (
    INPUT_DATA        => TABLE(PRES.VW_311_FORECAST_TRAIN_SIMPLE),
    TIMESTAMP_COLNAME => 'TARGET_TS',
    TARGET_COLNAME    => 'TOTAL_REQUESTS'
);

------------------------------------------------------------
-- 4) Table to store forecast results
------------------------------------------------------------

CREATE OR REPLACE TABLE PRES_311_FORECAST_RESULTS (
    FORECAST_DATE      DATE,
    PREDICTED_REQUESTS FLOAT,
    LOWER_BOUND        FLOAT,
    UPPER_BOUND        FLOAT,
    GENERATED_AT       TIMESTAMP_NTZ
);

------------------------------------------------------------
-- 5) Generate 14-day ahead forecast and store the results
------------------------------------------------------------

INSERT INTO PRES_311_FORECAST_RESULTS (
    FORECAST_DATE,
    PREDICTED_REQUESTS,
    LOWER_BOUND,
    UPPER_BOUND,
    GENERATED_AT
)
SELECT
    CAST(TS AS DATE)        AS FORECAST_DATE,
    FORECAST                AS PREDICTED_REQUESTS,
    LOWER_BOUND,
    UPPER_BOUND,
    CURRENT_TIMESTAMP()     AS GENERATED_AT
FROM TABLE(
    NYC311_DAILY_REQUESTS_MODEL!FORECAST(
        FORECASTING_PERIODS => 14
    )
);

-- Optional: inspect forecasted dates and values
SELECT *
FROM PRES_311_FORECAST_RESULTS
ORDER BY FORECAST_DATE;
