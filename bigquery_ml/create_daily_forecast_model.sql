-- This script creates or replaces a time-series forecasting model in BigQuery ML.
-- The model is trained on the daily revenue data from the 'daily_revenue' dbt model.
-- It uses the ARIMA_PLUS algorithm, which automatically handles trends and seasonality.

CREATE OR REPLACE MODEL `your-gcp-project-id.ecommerce.sales_forecast_daily`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='sale_date',
  time_series_data_col='total_revenue'
) AS
SELECT
  sale_date,
  total_revenue
FROM
  -- This should point to your dbt-generated daily revenue table.
  `your-gcp-project-id.ecommerce.daily_revenue`;