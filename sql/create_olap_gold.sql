CREATE SCHEMA IF NOT EXISTS olap_gold;

---------------- Product-level P&L ----------------
DROP TABLE IF EXISTS olap_gold.product_pnl CASCADE;
CREATE TABLE olap_gold.product_pnl AS
WITH sales_agg AS (
    SELECT
        s.brand,
        s.description,
        s.size,
        SUM(s.sales_dollars) AS total_revenue,
        SUM(s.sales_quantity) AS total_qty_sold,
        SUM(s.excise_tax) AS total_excise_tax,
        AVG(s.sales_price) AS avg_sales_price,
        MAX(s.vendor_no) AS vendor_number
    FROM olap_silver.fact_sales s
    GROUP BY s.brand, s.description, s.size
),
purchase_agg AS (
    SELECT
        p.brand,
        p.description,
        p.size,
        SUM(p.dollars) AS total_cogs,
        SUM(p.quantity) AS total_qty_purchased,
        AVG(p.purchase_price) AS avg_purchase_price,
        MAX(p.vendor_number) AS vendor_number
    FROM olap_silver.fact_purchases p
    GROUP BY p.brand, p.description, p.size
)
SELECT
    s.brand,
    s.description,
    s.size,
    COALESCE(v.vendor_name, 'UNKNOWN') AS vendor_name,
    ROUND(s.total_revenue, 2) AS total_revenue,
    ROUND(COALESCE(p.total_cogs, 0), 2) AS total_cogs, 
    ROUND(s.total_excise_tax, 4) AS total_excise_tax,
    ROUND(s.total_revenue - COALESCE(p.total_cogs, 0) - s.total_excise_tax, 2) AS gross_profit,
    CASE 
        WHEN s.total_revenue > 0 
        THEN ROUND((s.total_revenue - COALESCE(p.total_cogs, 0) - s.total_excise_tax) / s.total_revenue * 100, 2)
        ELSE 0 
    END AS gross_margin_pct,
    s.total_qty_sold,
    ROUND(s.avg_sales_price, 2) AS avg_sales_price,
    ROUND(COALESCE(p.avg_purchase_price, 0), 2) AS avg_purchase_price
FROM sales_agg s
LEFT JOIN purchase_agg p
    ON s.brand = p.brand 
    AND s.description = p.description 
    AND s.size = p.size
LEFT JOIN olap_silver.dim_vendor v
    ON COALESCE(p.vendor_number, s.vendor_number) = v.vendor_number;


---------------- Brand-level P&L ----------------
DROP TABLE IF EXISTS olap_gold.brand_pnl CASCADE;
CREATE TABLE olap_gold.brand_pnl AS
SELECT
    brand,
    MAX(vendor_name) AS vendor_name,
    ROUND(SUM(total_revenue), 2) AS total_revenue,
    ROUND(SUM(total_cogs), 2) AS total_cogs,
    ROUND(SUM(total_excise_tax), 4) AS total_excise_tax,
    ROUND(SUM(gross_profit), 2) AS gross_profit,
    CASE
        WHEN SUM(total_revenue) > 0
        THEN ROUND(SUM(gross_profit) / SUM(total_revenue) * 100, 2)
        ELSE 0
    END AS gross_margin_pct,
    SUM(total_qty_sold) AS total_qty_sold,
    COUNT(DISTINCT description) AS distinct_skus
FROM olap_gold.product_pnl
GROUP BY brand;


------------------- Reporting Views -------------------
DROP VIEW IF EXISTS olap_gold.vw_top_10_products_profit;
CREATE VIEW olap_gold.vw_top_10_products_profit AS
SELECT
    brand, description, size, vendor_name, total_revenue, total_cogs,
    gross_profit, gross_margin_pct, total_qty_sold
FROM olap_gold.product_pnl
WHERE gross_profit > 0
AND total_qty_sold >= 100 -- Arbitrary (exclude low-volume products)
ORDER BY gross_profit DESC LIMIT 10;

DROP VIEW IF EXISTS olap_gold.vw_top_10_products_margin;
CREATE VIEW olap_gold.vw_top_10_products_margin AS
SELECT
    brand, description, size, vendor_name, total_revenue, total_cogs,
    gross_profit, gross_margin_pct, total_qty_sold
FROM olap_gold.product_pnl
WHERE gross_margin_pct > 0
AND total_qty_sold >= 100 -- Arbitrary (exclude low-volume products)
ORDER BY gross_margin_pct DESC LIMIT 10;

DROP VIEW IF EXISTS olap_gold.vw_top_10_brands_profit;
CREATE VIEW olap_gold.vw_top_10_brands_profit AS
SELECT
    brand, vendor_name, total_revenue, total_cogs, gross_profit,
    gross_margin_pct, total_qty_sold, distinct_skus
FROM olap_gold.brand_pnl
WHERE gross_profit > 0
AND total_qty_sold >= 200 -- Arbitrary (exclude low-volume brands)
ORDER BY gross_profit DESC LIMIT 10;

DROP VIEW IF EXISTS olap_gold.vw_top_10_brands_margin;
CREATE VIEW olap_gold.vw_top_10_brands_margin AS
SELECT
    brand, vendor_name, total_revenue, total_cogs, gross_profit,
    gross_margin_pct, total_qty_sold, distinct_skus
FROM olap_gold.brand_pnl
WHERE gross_margin_pct > 0
AND total_qty_sold >= 200 -- Arbitrary (exclude low-volume brands)
ORDER BY gross_margin_pct DESC LIMIT 10;

DROP VIEW IF EXISTS olap_gold.vw_drop_candidates_products;
CREATE VIEW olap_gold.vw_drop_candidates_products AS
SELECT
    brand, description, size, vendor_name, total_revenue, total_cogs,
    gross_profit, gross_margin_pct, total_qty_sold
FROM olap_gold.product_pnl
WHERE gross_profit < 0
AND total_qty_sold >= 100 -- Arbitrary (exclude low-volume products to avoid noise)
ORDER BY gross_profit ASC;

DROP VIEW IF EXISTS olap_gold.vw_drop_candidates_brands;
CREATE VIEW olap_gold.vw_drop_candidates_brands AS
SELECT
    brand, vendor_name, total_revenue, total_cogs, gross_profit,
    gross_margin_pct, total_qty_sold, distinct_skus
FROM olap_gold.brand_pnl
WHERE gross_profit < 0
AND total_qty_sold >= 200 -- Arbitrary (exclude low-volume brands to avoid noise)
ORDER BY gross_profit ASC;