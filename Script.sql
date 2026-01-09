USE ecommerce_funnel;
SELECT 
    DATE_FORMAT(order_purchase_timestamp, '%H') AS hour_of_day,
    COUNT(order_id) AS total_orders,
    -- Calculate % of total volume for context
    ROUND(COUNT(order_id) * 100.0 / (SELECT COUNT(*) FROM fact_orders), 2) AS volume_percentage
FROM fact_orders
GROUP BY hour_of_day
ORDER BY total_orders DESC
LIMIT 5; 

SELECT 
    c.customer_state AS segment_state,
    COUNT(DISTINCT c.customer_unique_id) AS total_customers,
    SUM(p.payment_value) AS total_revenue,
    -- CLV Formula: Total Revenue / Total Unique Customers
    ROUND(SUM(p.payment_value) / COUNT(DISTINCT c.customer_unique_id), 2) AS avg_clv
FROM fact_orders o
JOIN dim_customers c ON o.customer_id = c.customer_id
JOIN fact_payments p ON o.order_id = p.order_id
GROUP BY c.customer_state
HAVING total_customers > 100 -- Filter out tiny states for statistical significance
ORDER BY avg_clv DESC
LIMIT 5;

SELECT 
    'Checkout (Total Orders)' AS funnel_stage, 
    COUNT(order_id) AS count,
    100.0 AS conversion_rate
FROM fact_orders
UNION ALL
SELECT 
    'Payment Approved' AS funnel_stage, 
    SUM(CASE WHEN order_approved_at IS NOT NULL THEN 1 ELSE 0 END) AS count,
    ROUND(SUM(CASE WHEN order_approved_at IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(order_id), 2)
FROM fact_orders
UNION ALL
SELECT 
    'Order Shipped' AS funnel_stage, 
    SUM(CASE WHEN order_delivered_carrier_date IS NOT NULL THEN 1 ELSE 0 END) AS count,
    ROUND(SUM(CASE WHEN order_delivered_carrier_date IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(order_id), 2)
FROM fact_orders;

SELECT 
    CASE 
        WHEN DATEDIFF(order_estimated_delivery_date, order_purchase_timestamp) <= 10 THEN 'Short Wait (<10 Days)'
        WHEN DATEDIFF(order_estimated_delivery_date, order_purchase_timestamp) BETWEEN 11 AND 20 THEN 'Medium Wait (10-20 Days)'
        ELSE 'Long Wait (>20 Days)'
    END AS expected_wait_bucket,
    COUNT(order_id) AS total_orders,
    SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END) AS canceled_orders,
    CONCAT(ROUND(SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END) * 100.0 / COUNT(order_id), 2), '%') AS cancellation_rate
FROM fact_orders
GROUP BY expected_wait_bucket
ORDER BY cancellation_rate DESC;
