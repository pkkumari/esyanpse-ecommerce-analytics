{{
  config(
    materialized='incremental',
    unique_key='sale_hour'
  )
}}

-- This incremental model calculates the total revenue for each hour.
-- It's useful for analyzing intraday sales trends and peak shopping times.

SELECT
    -- Truncates the full timestamp down to the beginning of the hour (e.g., 2025-07-26 14:00:00)
    TIMESTAMP_TRUNC(event_timestamp, HOUR) AS sale_hour,

    -- Calculates the total revenue from all purchases within that hour
    SUM(sale_price * quantity) AS total_revenue

FROM {{ source('ecommerce', 'live_events') }}

WHERE event_type = 'purchase'

{% if is_incremental() %}

  -- This filter ensures we only process new events since the last run
  AND event_timestamp > (SELECT max(sale_hour) FROM {{ this }})

{% endif %}

GROUP BY 1