-- This model calculates a composite 'health score' for each product from 0-100.
-- The score is a weighted average of a product's sales performance, customer ratings,
-- and its return rate, allowing a business to quickly identify top and bottom performers.

-- Step 1: Calculate the total number of returns for each product.
WITH product_returns AS (

    SELECT
        product_id,
        COUNT(*) AS number_of_returns
    FROM {{ source('ecommerce', 'live_events') }}
    WHERE event_type = 'return_item'
    GROUP BY 1

),

-- Step 2: Combine all raw KPI metrics into a single CTE.
-- We use a reference to our existing 'product_kpis' model to avoid repeating code.
product_all_metrics AS (

    SELECT
        kpis.product_id,
        kpis.total_revenue,
        kpis.average_customer_rating,
        -- Calculate the return rate, handling products with no sales to avoid division-by-zero errors.
        SAFE_DIVIDE(COALESCE(r.number_of_returns, 0), kpis.units_sold) AS return_rate
    
    FROM {{ ref('product_kpis') }} AS kpis
    LEFT JOIN product_returns AS r
        ON kpis.product_id = r.product_id
    
    -- Only score products that have at least one sale.
    WHERE kpis.units_sold > 0

),

-- Step 3: Normalize all metrics by ranking them on a common scale from 0 to 1.
-- This allows us to combine different metrics (like revenue and ratings) fairly.
ranked_metrics AS (

    SELECT
        product_id,
        -- PERCENT_RANK() scores each product from 0 (worst) to 1 (best) relative to the others.
        PERCENT_RANK() OVER (ORDER BY total_revenue ASC) AS sales_rank_score,
        PERCENT_RANK() OVER (ORDER BY average_customer_rating ASC) AS rating_rank_score,
        -- For return rate, a lower value is better, so we invert the rank.
        (1 - PERCENT_RANK() OVER (ORDER BY return_rate ASC)) AS return_rank_score
    
    FROM product_all_metrics

)

-- Step 4: Calculate the final weighted score and join the raw metrics for context.
SELECT
    metrics.product_id,
    
    -- The final score is a weighted average of the normalized ranks.
    -- These weights can be adjusted to reflect different business priorities.
    -- Current weights: Sales (50%), Rating (30%), Low Return Rate (20%)
    ROUND(
      (
        r.sales_rank_score * 0.50 +
        r.rating_rank_score * 0.30 +
        r.return_rank_score * 0.20
      ) * 100, 2
    ) AS health_score,
    
    metrics.total_revenue,
    metrics.average_customer_rating,
    metrics.return_rate

FROM product_all_metrics AS metrics
JOIN ranked_metrics AS r
    ON metrics.product_id = r.product_id

ORDER BY health_score DESC