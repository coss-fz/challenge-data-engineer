CREATE SCHEMA IF NOT EXISTS olap_silver;


---------------- Dimension: Products ----------------
DROP TABLE IF EXISTS olap_silver.dim_product CASCADE;
CREATE TABLE olap_silver.dim_product (
    product_key     SERIAL PRIMARY KEY,
    brand           INTEGER,
    description     TEXT,
    size            TEXT,
    volume          TEXT,
    classification  INTEGER,
    vendor_number   INTEGER,
    vendor_name     TEXT
);


---------------- Dimension: Stores ----------------
DROP TABLE IF EXISTS olap_silver.dim_store CASCADE;
CREATE TABLE olap_silver.dim_store (
    store_key   SERIAL PRIMARY KEY,
    store_id    INTEGER,
    city        TEXT
);


---------------- Dimension: Vendors ----------------
DROP TABLE IF EXISTS olap_silver.dim_vendor CASCADE;
CREATE TABLE olap_silver.dim_vendor (
    vendor_key      SERIAL PRIMARY KEY,
    vendor_number   INTEGER,
    vendor_name     TEXT
);


----------------- Dimension: Date -----------------
DROP TABLE IF EXISTS olap_silver.dim_date CASCADE;
CREATE TABLE olap_silver.dim_date (
    date_key    DATE PRIMARY KEY,
    year        INTEGER,
    quarter     INTEGER,
    month       INTEGER,
    week        INTEGER,
    day_of_week INTEGER
);

INSERT INTO olap_silver.dim_date
SELECT
    d::DATE,
    EXTRACT(YEAR  FROM d)::INTEGER,
    EXTRACT(QUARTER FROM d)::INTEGER,
    EXTRACT(MONTH FROM d)::INTEGER,
    EXTRACT(WEEK  FROM d)::INTEGER,
    EXTRACT(DOW   FROM d)::INTEGER
FROM generate_series('2016-01-01'::DATE, '2016-12-31'::DATE, '1 day'::INTERVAL) d
ON CONFLICT DO NOTHING;


---------------------------------------------- Populate dim_product ----------------------------------------------
INSERT INTO olap_silver.dim_product (brand, description, size, volume, classification, vendor_number, vendor_name)
SELECT DISTINCT
    brand,
    description,
    size,
    volume::TEXT,
    classification,
    vendor_number,
    TRIM(vendor_name)
FROM olap_bronze.purchase_prices
ON CONFLICT DO NOTHING;


--------------- Populate dim_store ---------------
INSERT INTO olap_silver.dim_store (store_id, city)
SELECT DISTINCT store, TRIM(city)
FROM olap_bronze.beg_inventory
ON CONFLICT DO NOTHING;


--------------------- Populate dim_vendor ---------------------
INSERT INTO olap_silver.dim_vendor (vendor_number, vendor_name)
SELECT DISTINCT vendor_number, TRIM(vendor_name)
FROM olap_bronze.purchases
ON CONFLICT DO NOTHING;


-------------------- Fact: Sales --------------------
DROP TABLE IF EXISTS olap_silver.fact_sales CASCADE;
CREATE TABLE olap_silver.fact_sales (
    sale_id         BIGSERIAL PRIMARY KEY,
    inventory_id    TEXT,
    store_id        INTEGER,
    brand           INTEGER,
    description     TEXT,
    size            TEXT,
    sales_date      DATE,
    sales_quantity  INTEGER,
    sales_dollars   NUMERIC(12,2),
    sales_price     NUMERIC(12,2),
    excise_tax      NUMERIC(12,4),
    volume          INTEGER,
    classification  INTEGER,
    vendor_no       INTEGER
);

INSERT INTO olap_silver.fact_sales (
    inventory_id, store_id, brand, description, size,
    sales_date, sales_quantity, sales_dollars, sales_price,
    excise_tax, volume, classification, vendor_no
)
SELECT
    inventory_id,
    store,
    brand,
    TRIM(description),
    size,
    sales_date,
    sales_quantity,
    sales_dollars,
    sales_price,
    excise_tax,
    volume,
    classification,
    vendor_no
FROM olap_bronze.sales
WHERE sales_dollars IS NOT NULL AND sales_quantity > 0;


-------------------- Fact: Purchases --------------------
DROP TABLE IF EXISTS olap_silver.fact_purchases CASCADE;
CREATE TABLE olap_silver.fact_purchases (
    purchase_id     BIGSERIAL PRIMARY KEY,
    inventory_id    TEXT,
    store_id        INTEGER,
    brand           INTEGER,
    description     TEXT,
    size            TEXT,
    vendor_number   INTEGER,
    po_number       INTEGER,
    receiving_date  DATE,
    purchase_price  NUMERIC(12,4),
    quantity        INTEGER,
    dollars         NUMERIC(12,2),
    classification  INTEGER
);

INSERT INTO olap_silver.fact_purchases (
    inventory_id, store_id, brand, description, size,
    vendor_number, po_number, receiving_date,
    purchase_price, quantity, dollars, classification
)
SELECT
    inventory_id,
    store,
    brand,
    TRIM(description),
    size,
    vendor_number,
    po_number,
    receiving_date,
    purchase_price,
    quantity,
    dollars,
    classification
FROM olap_bronze.purchases
WHERE dollars IS NOT NULL AND quantity > 0;


-------------------- Fact: Inventory --------------------
DROP TABLE IF EXISTS olap_silver.fact_inventory CASCADE;
CREATE TABLE olap_silver.fact_inventory (
    inventory_id    TEXT,
    store_id        INTEGER,
    city            TEXT,
    brand           INTEGER,
    description     TEXT,
    size            TEXT,
    on_hand_beg     INTEGER,
    on_hand_end     INTEGER,
    price           NUMERIC(12,2),
    start_date      DATE,
    end_date        DATE
);

INSERT INTO olap_silver.fact_inventory
SELECT
    b.inventory_id,
    b.store,
    TRIM(b.city),
    b.brand,
    TRIM(b.description),
    b.size,
    b.on_hand AS on_hand_beg,
    COALESCE(e.on_hand, 0) AS on_hand_end,
    b.price,
    b.start_date,
    e.end_date
FROM olap_bronze.beg_inventory b
LEFT JOIN olap_bronze.end_inventory e
    ON b.inventory_id = e.inventory_id;