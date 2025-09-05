-- Data quality example: detect duplicate orders
SELECT order_id, COUNT(*) AS cnt
FROM stg_orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Window function: top orders per city
WITH ranked AS (
  SELECT
    o.order_id,
    c.city,
    o.amount,
    ROW_NUMBER() OVER (PARTITION BY c.city ORDER BY o.amount DESC) AS rn
  FROM stg_orders o
  JOIN dim_customers c ON o.customer_id = c.customer_id
)
SELECT * FROM ranked WHERE rn <= 3;

-- Slowly Changing Dimension Type 2 example
MERGE INTO dim_customers_scd2 AS tgt
USING stg_customers AS src
ON tgt.customer_id = src.customer_id AND tgt.is_current = 1
WHEN MATCHED AND (
    tgt.customer_name <> src.customer_name OR
    tgt.city <> src.city OR
    tgt.segment <> src.segment
) THEN UPDATE SET
    tgt.valid_to = src.change_ts,
    tgt.is_current = 0
WHEN NOT MATCHED BY TARGET THEN INSERT (
    customer_id, customer_name, city, segment, valid_from, valid_to, is_current
) VALUES (
    src.customer_id, src.customer_name, src.city, src.segment, src.change_ts, '9999-12-31', 1
);
