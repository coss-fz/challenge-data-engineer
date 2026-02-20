CREATE SCHEMA IF NOT EXISTS olap_bronze;


-------------------- Sales --------------------
DROP TABLE IF EXISTS olap_bronze.sales CASCADE;
CREATE TABLE olap_bronze.sales (
    inventory_id        TEXT,
    store               INTEGER,
    brand               INTEGER,
    description         TEXT,
    size                TEXT,
    sales_quantity      INTEGER,
    sales_dollars       NUMERIC(12,2),
    sales_price         NUMERIC(12,2),
    sales_date          DATE,
    volume              INTEGER,
    classification      INTEGER,
    excise_tax          NUMERIC(12,4),
    vendor_no           INTEGER,
    vendor_name         TEXT
);


----------------- Beginning Inventory -----------------
DROP TABLE IF EXISTS olap_bronze.beg_inventory CASCADE;
CREATE TABLE olap_bronze.beg_inventory (
    inventory_id    TEXT,
    store           INTEGER,
    city            TEXT,
    brand           INTEGER,
    description     TEXT,
    size            TEXT,
    on_hand         INTEGER,
    price           NUMERIC(12,2),
    start_date      DATE
);


------------------- Ending Inventory -------------------
DROP TABLE IF EXISTS olap_bronze.end_inventory CASCADE;
CREATE TABLE olap_bronze.end_inventory (
    inventory_id    TEXT,
    store           INTEGER,
    city            TEXT,
    brand           INTEGER,
    description     TEXT,
    size            TEXT,
    on_hand         INTEGER,
    price           NUMERIC(12,2),
    end_date        DATE
);


-------------------- Purchases --------------------
DROP TABLE IF EXISTS olap_bronze.purchases CASCADE;
CREATE TABLE olap_bronze.purchases (
    inventory_id        TEXT,
    store               INTEGER,
    brand               INTEGER,
    description         TEXT,
    size                TEXT,
    vendor_number       INTEGER,
    vendor_name         TEXT,
    po_number           INTEGER,
    po_date             DATE,
    receiving_date      DATE,
    invoice_date        DATE,
    pay_date            DATE,
    purchase_price      NUMERIC(12,4),
    quantity            INTEGER,
    dollars             NUMERIC(12,2),
    classification      INTEGER
);


-------------------- Invoice Purchases --------------------
DROP TABLE IF EXISTS olap_bronze.invoice_purchases CASCADE;
CREATE TABLE olap_bronze.invoice_purchases (
    vendor_number   INTEGER,
    vendor_name     TEXT,
    invoice_date    DATE,
    po_number       INTEGER,
    po_date         DATE,
    pay_date        DATE,
    quantity        INTEGER,
    dollars         NUMERIC(12,2),
    freight         NUMERIC(12,2),
    approval        TEXT
);


------------------ 2017 Purchase Prices ------------------
DROP TABLE IF EXISTS olap_bronze.purchase_prices CASCADE;
CREATE TABLE olap_bronze.purchase_prices (
    brand               INTEGER,
    description         TEXT,
    price               NUMERIC(12,2),
    size                TEXT,
    volume              TEXT,
    classification      INTEGER,
    purchase_price      NUMERIC(12,4),
    vendor_number       INTEGER,
    vendor_name         TEXT
);