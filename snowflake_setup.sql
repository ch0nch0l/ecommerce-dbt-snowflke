-- ──────────────────────────────────────────────────────────────────────────────
-- SNOWFLAKE SETUP — Run this ONCE in a SQL Worksheet as ACCOUNTADMIN
-- Sets up the databases, warehouse, role, and user for this dbt project.
-- After running, copy profiles.yml.example to ~/.dbt/profiles.yml
-- and fill in your account identifier and password.
-- ──────────────────────────────────────────────────────────────────────────────

USE ROLE ACCOUNTADMIN;

-- ── 1. Warehouse ──────────────────────────────────────────────────────────────
-- XS warehouse — more than enough for this project, auto-suspends after 60s
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WITH WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- ── 2. Databases ─────────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS ECOMMERCE_RAW;    -- raw source data lands here
CREATE DATABASE IF NOT EXISTS ECOMMERCE_DEV;    -- dbt dev builds here
CREATE DATABASE IF NOT EXISTS ECOMMERCE_PROD;   -- dbt prod builds here

-- ── 3. Role ───────────────────────────────────────────────────────────────────
CREATE ROLE IF NOT EXISTS DBT_ROLE;

GRANT USAGE  ON WAREHOUSE COMPUTE_WH   TO ROLE DBT_ROLE;
GRANT ALL    ON DATABASE  ECOMMERCE_RAW  TO ROLE DBT_ROLE;
GRANT ALL    ON DATABASE  ECOMMERCE_DEV  TO ROLE DBT_ROLE;
GRANT ALL    ON DATABASE  ECOMMERCE_PROD TO ROLE DBT_ROLE;

-- ── 4. User ───────────────────────────────────────────────────────────────────
-- Replace <your_password> with a real password
CREATE USER IF NOT EXISTS DBT_USER
  PASSWORD = '<your_password>'
  DEFAULT_ROLE = DBT_ROLE
  DEFAULT_WAREHOUSE = COMPUTE_WH;

GRANT ROLE DBT_ROLE TO USER DBT_USER;

-- ── 5. Raw schemas ────────────────────────────────────────────────────────────
USE DATABASE ECOMMERCE_RAW;
CREATE SCHEMA IF NOT EXISTS OLIST;   -- Brazilian e-commerce source data

-- ── 6. Load source tables ─────────────────────────────────────────────────────
-- The Olist dataset CSVs are in seeds/ — run `dbt seed` to load them.
-- After `dbt seed` runs, the tables below will exist automatically:
--   ECOMMERCE_RAW.OLIST.orders
--   ECOMMERCE_RAW.OLIST.order_items
--   ECOMMERCE_RAW.OLIST.customers
--   ECOMMERCE_RAW.OLIST.products
--   ECOMMERCE_RAW.OLIST.sellers
--   ECOMMERCE_RAW.OLIST.order_reviews
--   ECOMMERCE_RAW.OLIST.order_payments
--   ECOMMERCE_RAW.OLIST.product_category_name_translation

-- ── 7. Verify ─────────────────────────────────────────────────────────────────
SHOW DATABASES;
SHOW WAREHOUSES;
SHOW ROLES;
