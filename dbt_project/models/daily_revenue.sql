{{
  config(
    materialized='incremental',
    unique_key='sale_date'
  )
}}

-- This incremental model calculates the total revenue for each day.
-- On each run, it only processes new events, making it highly efficient.

SELECT
    -- Truncates the full timestamp to just the date (e.g., 2025-07-26)
    DATE(event_timestamp) AS sale_date,

    -- Calculates the total revenue from all purchases on that day
    SUM(sale_price * quantity) AS total_revenue

FROM {{ source('ecommerce', 'live_events') }}

-- Filters for only purchase events
WHERE event_type = 'purchase'

{% if is_incremental() %}

  -- This filter ensures that on subsequent runs, we only process new data
  -- that has arrived since the last time the model was run.
  AND DATE(event_timestamp) > (SELECT max(sale_date) FROM {{ this }})

{% endif %}

-- Groups all rows by the sale_date to sum up the revenue correctly
GROUP BY 1