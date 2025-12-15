select * from flipcart.customers;

-- =============================================
-- Insight 1: Monthly Revenue Trend with Growth %
-- =============================================

WITH monthly_sales AS (
    SELECT 
        TO_CHAR(order_date, 'YYYY-MM') AS month,
        SUM(quantity * price_per_unit) AS revenue
    FROM flipcart.sales
    WHERE LOWER(order_status) = 'completed'
    GROUP BY TO_CHAR(order_date, 'YYYY-MM')
)
SELECT 
    month,
    revenue,
    ROUND(
        ((revenue - LAG(revenue) OVER (ORDER BY month)) / 
          COALESCE(LAG(revenue) OVER (ORDER BY month), 0)) * 100, 2
    ) AS growth_percent
FROM monthly_sales
ORDER BY month;


-- ==========================================================
-- Insight 2: Top 5 Product Categories by Revenue (Completed)
-- ==========================================================

CREATE OR REPLACE VIEW v_top5_categories AS
WITH cat_rev AS (
    SELECT 
        p.category, 
        SUM(s.quantity * s.price_per_unit) AS revenue
    FROM flipcart.sales s
    JOIN flipcart.products p ON s.product_id = p.product_id
    JOIN flipcart.customers c ON s.customer_id = c.customer_id
    WHERE LOWER(s.order_status) = 'completed'
    GROUP BY p.category
)
SELECT 
    category, 
    revenue,
    RANK() OVER (ORDER BY revenue DESC) AS revenue_rank
FROM cat_rev
ORDER BY revenue DESC
LIMIT 5;

select * from v_top5_categories;

-- ==========================================================
-- Insight 3: State-wise Average Order Value (AOV)
-- ==========================================================

CREATE OR REPLACE VIEW v_state_aov AS
WITH order_totals AS (
    SELECT 
        order_id,
        customer_id,
        SUM(quantity * price_per_unit) AS order_total
    FROM flipcart.sales
    GROUP BY order_id, customer_id
)
SELECT 
    c.state,
    ROUND(AVG(ot.order_total), 2) AS avg_order_value,
    COUNT(ot.order_id) AS orders_count
FROM order_totals ot
JOIN flipcart.customers c 
    ON ot.customer_id = c.customer_id
GROUP BY c.state
ORDER BY avg_order_value DESC;

select * from v_state_aov;

-- ==========================================================
-- Insight 4: Products Never Ordered
-- ==========================================================

CREATE OR REPLACE VIEW v_products_never_ordered AS
SELECT 
    p.product_id, 
    p.product_name, 
    p.category
FROM flipcart.products p
LEFT JOIN flipcart.sales s 
    ON p.product_id = s.product_id
WHERE s.product_id IS NULL
ORDER BY p.category, p.product_name;

select * from v_products_never_ordered;

-- 5) PL/pgSQL: Total revenue by category (cursor + loop)
CREATE OR REPLACE FUNCTION fn_revenue_by_category()
RETURNS void AS 
$$
DECLARE
  rec RECORD;
BEGIN
  RAISE NOTICE '--- Revenue by Category (Completed Orders) ---';
  FOR rec IN
    SELECT p.category, SUM(s.quantity * s.price_per_unit) AS revenue
    FROM flipcart.products p
    JOIN flipcart.sales s ON p.product_id = s.product_id
    WHERE lower(s.order_status) = 'completed'
    GROUP BY p.category
    ORDER BY revenue DESC
  LOOP
    RAISE NOTICE 'Category: % | Revenue: %', rec.category, rec.revenue;
  END LOOP;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Error in fn_revenue_by_category: %', SQLERRM;
END;
$$ 
LANGUAGE plpgsql;

SET client_min_messages = NOTICE;

SELECT fn_revenue_by_category();

-- 6) PL/pgSQL: Pending orders report (explicit cursor)
CREATE OR REPLACE FUNCTION fn_pending_orders_report()
RETURNS void AS $$
DECLARE
  cur_pending REFCURSOR;
  r RECORD;
BEGIN
  OPEN cur_pending FOR
    SELECT DISTINCT s.order_id, MIN(s.order_date) AS order_date, s.customer_id
    FROM flipcart.sales s
    LEFT JOIN flipcart.shippings sh ON s.order_id = sh.order_id
    WHERE sh.order_id IS NULL
    GROUP BY s.order_id, s.customer_id
    ORDER BY order_date;
  LOOP
    FETCH cur_pending INTO r;
    EXIT WHEN NOT FOUND;
    RAISE NOTICE 'Order ID: % | Date: % | Customer ID: %', r.order_id, r.order_date, r.customer_id;
  END LOOP;
  CLOSE cur_pending;
END;
$$ LANGUAGE plpgsql;

SELECT fn_pending_orders_report();


-- 7) PL/pgSQL: High-value customers procedure (parameterized) using aggregates and exception handling
CREATE OR REPLACE FUNCTION fn_high_value_customers(min_spend numeric)
RETURNS void AS 
$$
DECLARE
  r RECORD;
BEGIN
  RAISE NOTICE '--- High Value Customers (spend > %) ---', min_spend;
  FOR r IN
    SELECT c.customer_id, c.customer_name, SUM(s.quantity * s.price_per_unit) AS total_spent
    FROM flipcart.customers c
    JOIN flipcart.sales s ON c.customer_id = s.customer_id
    WHERE lower(s.order_status) = 'completed'
    GROUP BY c.customer_id, c.customer_name
    HAVING SUM(s.quantity * s.price_per_unit) > min_spend
    ORDER BY total_spent DESC
  LOOP
    RAISE NOTICE 'Customer ID: % | Name: % | Spent: %', r.customer_id, r.customer_name, r.total_spent;
  END LOOP;
EXCEPTION WHEN NO_DATA_FOUND THEN
  RAISE NOTICE 'No customers found above %', min_spend;
WHEN OTHERS THEN
  RAISE NOTICE 'Error in fn_high_value_customers: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

SELECT fn_high_value_customers(50000);


-- 8) PL/pgSQL: Average Order Value per customer for orders with quantity > 5
CREATE OR REPLACE FUNCTION fn_avg_order_value_high_qty()
RETURNS void AS $$
DECLARE
  r RECORD;
BEGIN
  RAISE NOTICE '--- Avg Order Value per Customer (orders where any line quantity > 5) ---';
  FOR r IN
    WITH high_qty_orders AS (
      SELECT DISTINCT order_id FROM flipcart.sales WHERE quantity > 5
    ), order_totals AS (
      SELECT s.order_id, SUM(s.quantity * s.price_per_unit) AS order_total, s.customer_id
      FROM flipcart.sales s
      WHERE s.order_id IN (SELECT order_id FROM high_qty_orders)
      GROUP BY s.order_id, s.customer_id
    )
    SELECT c.customer_id, c.customer_name, ROUND(AVG(ot.order_total)::numeric,2) AS avg_order_value, COUNT(ot.order_id) AS orders_count
    FROM order_totals ot
    JOIN flipcart.customers c ON c.customer_id = ot.customer_id
    GROUP BY c.customer_id, c.customer_name
    ORDER BY avg_order_value DESC
  LOOP
    RAISE NOTICE 'Customer ID: % | Name: % | Avg Order Value: % | Orders: %', r.customer_id, r.customer_name, r.avg_order_value, r.orders_count;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

select fn_avg_order_value_high_qty();

-- 9) PL/pgSQL: Top 5 customers by spending on 'Accessories' (uses explicit LIMIT handling)
CREATE OR REPLACE FUNCTION fn_top5_customers_accessories()
RETURNS void AS $$
DECLARE
  r RECORD;
BEGIN
  RAISE NOTICE '--- Top 5 Customers (Accessories) ---';
  FOR r IN
    SELECT c.customer_id, c.customer_name, SUM(s.quantity * s.price_per_unit) AS spent
    FROM flipcart.sales s
    JOIN flipcart.products p ON s.product_id = p.product_id
    JOIN flipcart.customers c ON s.customer_id = c.customer_id
    WHERE lower(p.category) = 'accessories' AND lower(s.order_status) = 'completed'
    GROUP BY c.customer_id, c.customer_name
    ORDER BY spent DESC
    LIMIT 5
  LOOP
    RAISE NOTICE 'Customer ID: % | Name: % | Spent: %', r.customer_id, r.customer_name, r.spent;
  END LOOP;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Error in fn_top5_customers_accessories: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

select fn_top5_customers_accessories();

drop function fn_orders_without_payments();

CREATE OR REPLACE FUNCTION fn_orders_without_payments()
RETURNS TABLE(out_order_id int, out_customer_id int, out_order_date date) AS $$
BEGIN
  RETURN QUERY
  SELECT o.order_id, o.customer_id, MIN(s.order_date) AS order_date
  FROM (
    SELECT DISTINCT order_id, customer_id 
    FROM flipcart.sales
  ) o
  LEFT JOIN flipcart.payments p ON o.order_id = p.order_id
  JOIN flipcart.sales s ON s.order_id = o.order_id
  WHERE p.payment_id IS NULL
  GROUP BY o.order_id, o.customer_id
  ORDER BY order_date;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fn_orders_without_payments();

