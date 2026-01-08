USE ecommerce_funnel;
-- "Extracted insights like peak-selling periods"
SELECT 
    DATE_FORMAT(order_purchase_timestamp, '%H') AS hour_of_day,
    COUNT(*) AS total_orders
FROM fact_orderss
GROUP BY hour_of_day
ORDER BY total_orders DESC;
-- Insight: This usually reveals a spike between 10 AM - 4 PM.


-- "Customer segments with highest CLV"
SELECT 
    c.customer_state,
    COUNT(DISTINCT c.customer_unique_id) AS unique_customers,
    ROUND(SUM(p.payment_value) / COUNT(DISTINCT c.customer_unique_id), 2) AS avg_clv
FROM fact_orders o
JOIN dim_customers c ON o.customer_id = c.customer_id
JOIN fact_payments p ON o.order_id = p.order_id
GROUP BY c.customer_state
ORDER BY avg_clv DESC;
-- Insight: Shows which states (segments) spend the most money per person.

-- "Checkout-to-payment drop-off as the biggest funnel leakage"
SELECT 
    COUNT(order_id) AS total_checkouts,
    SUM(CASE WHEN order_approved_at IS NOT NULL THEN 1 ELSE 0 END) AS payments_successful,
    
    -- Calculate Drop-off Count
    COUNT(order_id) - SUM(CASE WHEN order_approved_at IS NOT NULL THEN 1 ELSE 0 END) AS drop_off_count,
    
    -- Calculate Drop-off Percentage
    CONCAT(ROUND(
        (COUNT(order_id) - SUM(CASE WHEN order_approved_at IS NOT NULL THEN 1 ELSE 0 END)) / COUNT(order_id) * 100
    , 2), '%') AS drop_off_rate
FROM fact_orders;
-- Insight: If this rate is high (e.g., >5%), it validates the resume claim.


-- "Late deliveries (or long waits) driving higher cancellations"
SELECT 
    CASE
        WHEN
            DATEDIFF(order_estimated_delivery_date,
                    order_purchase_timestamp) > 20
        THEN
            'Long Wait (>20 Days)'
        ELSE 'Short Wait (<20 Days)'
    END AS expected_wait_group,
    COUNT(order_id) AS total_orders,
    SUM(CASE
        WHEN order_status = 'canceled' THEN 1
        ELSE 0
    END) AS canceled_orders,
    CONCAT(ROUND(SUM(CASE
                        WHEN order_status = 'canceled' THEN 1
                        ELSE 0
                    END) / COUNT(order_id) * 100,
                    2),
            '%') AS cancel_rate
FROM
    fact_orders
GROUP BY expected_wait_group;

