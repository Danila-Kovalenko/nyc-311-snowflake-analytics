-- 07_forecast_evaluation.sql
-- Evaluate the daily forecast model on a held-out test window.
-- The script:
--   1) Defines the last 60 days of history as a test period
--   2) Trains a separate evaluation model on data BEFORE the test period
--   3) Forecasts the test period
--   4) Compares forecast vs actuals and computes MAE / RMSE / MAPE

USE DATABASE NYC_311;
USE SCHEMA PRES;

------------------------------------------------------------
-- 1) Define evaluation window (last 60 days of historical data)
------------------------------------------------------------

-- This table stores a single row with evaluation dates and horizon length.
CREATE OR REPLACE TABLE PRES_311_FORECAST_EVAL_CONFIG AS
SELECT
    MAX(TARGET_DATE)                                           AS MAX_DATE,          -- last date with historical data
    DATEADD('day', -59, MAX(TARGET_DATE))                      AS TEST_START,        -- start of test window (60 days)
    MAX(TARGET_DATE)                                           AS TEST_END,          -- end of test window
    60                                                         AS TEST_HORIZON_DAYS  -- number of days to forecast
FROM PRES_311_DAILY_FORECAST_BASE;

-- Optional: inspect the evaluation config
SELECT *
FROM PRES_311_FORECAST_EVAL_CONFIG;


------------------------------------------------------------
-- 2) Training view for evaluation model (history BEFORE test window)
------------------------------------------------------------

-- This view uses the config table to limit training data
-- to dates strictly before TEST_START.
CREATE OR REPLACE VIEW VW_311_FORECAST_TRAIN_EVAL AS
SELECT
    TO_TIMESTAMP_NTZ(b.TARGET_DATE) AS TARGET_TS,      -- timestamp used for forecasting
    b.TOTAL_REQUESTS::FLOAT         AS TOTAL_REQUESTS  -- numeric target
FROM PRES_311_DAILY_FORECAST_BASE b
JOIN PRES_311_FORECAST_EVAL_CONFIG c
    ON b.TARGET_DATE < c.TEST_START
WHERE b.TARGET_DATE IS NOT NULL
ORDER BY b.TARGET_DATE;

-- Optional: preview the training data for evaluation model
SELECT *
FROM VW_311_FORECAST_TRAIN_EVAL
ORDER BY TARGET_TS DESC
LIMIT 10;


------------------------------------------------------------
-- 3) Train a dedicated evaluation model on "old" history
------------------------------------------------------------

-- This model is used ONLY for backtesting on the last 60 days
-- and does not affect the main production model.
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST NYC311_DAILY_REQUESTS_MODEL_EVAL (
    INPUT_DATA        => TABLE(PRES.VW_311_FORECAST_TRAIN_EVAL),
    TIMESTAMP_COLNAME => 'TARGET_TS',
    TARGET_COLNAME    => 'TOTAL_REQUESTS'
);


------------------------------------------------------------
-- 4) Generate forecast for the test window (60 days)
------------------------------------------------------------

-- This table stores day-by-day predictions for the evaluation horizon.
-- NOTE: FORECASTING_PERIODS must match TEST_HORIZON_DAYS from the config (60).
CREATE OR REPLACE TABLE PRES_311_FORECAST_EVAL_PRED AS
SELECT
    CAST(TS AS DATE)        AS DATE,                -- forecast date
    FORECAST                AS PREDICTED_REQUESTS   -- point forecast
FROM TABLE(
    NYC311_DAILY_REQUESTS_MODEL_EVAL!FORECAST(
        FORECASTING_PERIODS => 60
    )
);

-- Optional: preview predictions
SELECT *
FROM PRES_311_FORECAST_EVAL_PRED
ORDER BY DATE;


------------------------------------------------------------
-- 5) Join forecast with actuals for the test window
------------------------------------------------------------

-- This table stores the day-by-day comparison of
-- actual vs predicted values for the evaluation period.
CREATE OR REPLACE TABLE PRES_311_FORECAST_EVAL_DAILY AS
WITH cfg AS (
    SELECT TEST_START, TEST_END
    FROM PRES_311_FORECAST_EVAL_CONFIG
),
joined AS (
    SELECT
        a.TARGET_DATE                             AS DATE,
        a.TOTAL_REQUESTS                          AS ACTUAL_REQUESTS,
        p.PREDICTED_REQUESTS
    FROM PRES_311_DAILY_FORECAST_BASE a
    JOIN cfg c
      ON a.TARGET_DATE BETWEEN c.TEST_START AND c.TEST_END
    LEFT JOIN PRES_311_FORECAST_EVAL_PRED p
      ON p.DATE = a.TARGET_DATE
)
SELECT *
FROM joined
ORDER BY DATE;

-- Optional: inspect daily comparison
SELECT *
FROM PRES_311_FORECAST_EVAL_DAILY
ORDER BY DATE;


------------------------------------------------------------
-- 6) Compute evaluation metrics: MAE, RMSE, MAPE
------------------------------------------------------------

-- This table stores global evaluation metrics for the chosen test window.
CREATE OR REPLACE TABLE PRES_311_FORECAST_EVAL_METRICS AS
SELECT
    COUNT(*) AS N_DAYS,
    AVG(ABS(PREDICTED_REQUESTS - ACTUAL_REQUESTS))                                  AS MAE,
    SQRT(AVG(POWER(PREDICTED_REQUESTS - ACTUAL_REQUESTS, 2)))                       AS RMSE,
    AVG(
        CASE 
            WHEN ACTUAL_REQUESTS <> 0 THEN
                ABS(PREDICTED_REQUESTS - ACTUAL_REQUESTS) / ACTUAL_REQUESTS::FLOAT
            ELSE NULL
        END
    ) * 100                                                                         AS MAPE_PERCENT
FROM PRES_311_FORECAST_EVAL_DAILY
WHERE PREDICTED_REQUESTS IS NOT NULL;

-- Final metrics overview
SELECT *
FROM PRES_311_FORECAST_EVAL_METRICS;
