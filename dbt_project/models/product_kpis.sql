-- This model creates a wide summary table with key performance indicators (KPIs) for each product.
-- It rebuilds completely on each run to ensure all lifetime metrics are up-to-date.

-- First, we aggregate all sales-related metrics for each product.
WITH product_sales AS (

    SELECT
        product_id,
        SUM(sale_price * quantity) AS total_revenue,
        SUM(quantity) AS units_sold,
        AVG(sale_price) AS average_selling_price,
        -- We only count units sold when the 'on_sale' flag was true
        SUM(CASE WHEN on_sale = true THEN quantity ELSE 0 END) AS discounted_units_sold,
        COUNT(DISTINCT session_id) AS number_of_orders

    FROM {{ source('ecommerce', 'live_events') }}
    WHERE event_type = 'purchase'
    GROUP BY 1

),

-- Next, we count the total number of product page views for each product.
product_views AS (

    SELECT
        product_id,
        COUNT(*) AS number_of_views

    FROM {{ source('ecommerce', 'live_events') }}
    WHERE event_type = 'product_view'
    GROUP BY 1

)

-- Finally, we join the aggregated data back to the main product list.
SELECT
    p.product_id,
    p.product_name,
    p.category,
    -- Use COALESCE to show 0 instead of NULL for products with no sales
    COALESCE(s.total_revenue, 0) AS total_revenue,
    COALESCE(s.units_sold, 0) AS units_sold,
    s.average_selling_price,
    p.avg_rating AS average_customer_rating,
    -- Use SAFE_DIVIDE to prevent division-by-zero errors for products with no views
    SAFE_DIVIDE(s.number_of_orders, v.number_of_views) AS view_to_sale_conversion_rate,
    SAFE_DIVIDE(s.discounted_units_sold, s.units_sold) AS discount_effectiveness_ratio

-- Start with the 'products' table to ensure all products are included, even those with no sales or views.
FROM {{ source('ecommerce', 'products') }} p
LEFT JOIN product_sales s
    ON p.product_id = s.product_id
LEFT JOIN product_views v
    ON p.product_id = v.product_id