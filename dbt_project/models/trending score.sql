-- This model calculates a 'trending score' to identify products with a recent surge in popularity.
-- It works by comparing short-term view activity (7-day average) to a long-term baseline (28-day average).

-- Step 1: Aggregate the raw product view events into a daily count for each product.
WITH daily_views AS (

    SELECT
        DATE(event_timestamp) AS view_date,
        product_id,
        COUNT(*) AS view_count
    
    FROM {{ source('ecommerce', 'live_events') }}
    WHERE event_type = 'product_view'
    GROUP BY 1, 2

),

-- Step 2: Use SQL window functions to calculate short-term and long-term moving averages.
moving_averages AS (

    SELECT
        view_date,
        product_id,
        
        -- Calculate a 7-day moving average of views for each product.
        AVG(view_count) OVER(
            PARTITION BY product_id
            ORDER BY view_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS seven_day_avg_views,

        -- Calculate a 28-day moving average of views for each product.
        AVG(view_count) OVER(
            PARTITION BY product_id
            ORDER BY view_date
            ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
        ) AS twenty_eight_day_avg_views

    FROM daily_views

)

-- Step 3: Calculate the final score and select only the most recent data for each product.
SELECT
    p.product_name,
    m.view_date,
    
    -- The trending score is the ratio of recent activity to the historical baseline.
    -- A score > 1.0 indicates a product is "trending up".
    SAFE_DIVIDE(m.seven_day_avg_views, m.twenty_eight_day_avg_views) AS trending_score,
    
    m.seven_day_avg_views,
    m.twenty_eight_day_avg_views

FROM moving_averages AS m
JOIN {{ source('ecommerce', 'products') }} AS p
    ON m.product_id = p.product_id

-- QUALIFY is a BigQuery feature that efficiently filters the results of a window function.
-- This clause keeps only the single most recent row for each product.
QUALIFY ROW_NUMBER() OVER (PARTITION BY m.product_id ORDER BY m.view_date DESC) = 1