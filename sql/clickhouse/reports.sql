CREATE MATERIALIZED VIEW lakehouse.mv_transactional_financial_summary
ENGINE = SummingMergeTree()
ORDER BY (dummy_key) 
POPULATE
AS
SELECT
    1 AS dummy_key,   

    sum(order_total_amount) AS gross_sales,

    sum(discount_amount) AS total_discount,

    sum(order_total_amount - item_total_amount) AS total_shipping,

    sum(ifNull(refund_amount, 0)) AS total_refund,

    sum(order_total_amount - discount_amount - ifNull(refund_amount, 0)) AS net_revenue_before_shipping,

    count() AS total_orders

FROM lakehouse.transactional_obt
WHERE order_status NOT IN ('cancelled')
  AND order_total_amount > 0
GROUP BY dummy_key;  