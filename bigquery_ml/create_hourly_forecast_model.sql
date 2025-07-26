-- This script creates or replaces an hourly time-series forecasting model.
-- It is trained on the hourly revenue data from the 'hourly_revenue' dbt model.
-- This model is useful for short-term operational forecasting.

CREATE OR REPLACE MODEL `your-gcp-project-id.ecommerce.sales_forecast_hourly`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='sale_hour',
  time_series_data_col='total_revenue'
) AS
SELECT
  sale_hour,
  total_revenue
FROM
  -- This should point to your dbt-generated hourly revenue table.
  `your-gcp-project-id.ecommerce.hourly_revenue`;