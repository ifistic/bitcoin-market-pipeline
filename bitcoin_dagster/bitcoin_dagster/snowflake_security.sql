-- ============================================================
-- Bitcoin Market Pipeline - Snowflake RBAC & Security Setup
-- Run this once in Snowflake as ACCOUNTADMIN
-- ============================================================

-- 1. Roles
CREATE ROLE IF NOT EXISTS pipeline_writer;   -- Dagster ingestion
CREATE ROLE IF NOT EXISTS dbt_transformer;   -- dbt models
CREATE ROLE IF NOT EXISTS bi_reader;         -- Superset / BI tools
CREATE ROLE IF NOT EXISTS data_engineer;     -- Full access for team

-- 2. Warehouse grants
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE pipeline_writer;
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE dbt_transformer;
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE bi_reader;
GRANT ALL   ON WAREHOUSE compute_wh TO ROLE data_engineer;

-- 3. Database grants
GRANT USAGE ON DATABASE CRYPTO_DB TO ROLE pipeline_writer;
GRANT USAGE ON DATABASE CRYPTO_DB TO ROLE dbt_transformer;
GRANT USAGE ON DATABASE CRYPTO_DB TO ROLE bi_reader;
GRANT ALL   ON DATABASE CRYPTO_DB TO ROLE data_engineer;

-- 4. Schema grants
-- pipeline_writer: write to RAW only
GRANT USAGE, CREATE TABLE ON SCHEMA CRYPTO_DB.RAW TO ROLE pipeline_writer;

-- dbt_transformer: read RAW, write bronze/silver/gold
GRANT USAGE ON SCHEMA CRYPTO_DB.RAW              TO ROLE dbt_transformer;
GRANT SELECT ON ALL TABLES IN SCHEMA CRYPTO_DB.RAW TO ROLE dbt_transformer;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA CRYPTO_DB.ANALYTICS_BRONZE TO ROLE dbt_transformer;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA CRYPTO_DB.ANALYTICS_SILVER TO ROLE dbt_transformer;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA CRYPTO_DB.ANALYTICS_GOLD   TO ROLE dbt_transformer;

-- bi_reader: read gold only
GRANT USAGE  ON SCHEMA CRYPTO_DB.ANALYTICS_GOLD TO ROLE bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA CRYPTO_DB.ANALYTICS_GOLD TO ROLE bi_reader;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CRYPTO_DB.ANALYTICS_GOLD TO ROLE bi_reader;
GRANT SELECT ON ALL VIEWS  IN SCHEMA CRYPTO_DB.ANALYTICS_GOLD TO ROLE bi_reader;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA CRYPTO_DB.ANALYTICS_GOLD TO ROLE bi_reader;

-- 5. Service users
CREATE USER IF NOT EXISTS dagster_svc
    PASSWORD = 'CHANGE_ME_strong_password_1!'
    DEFAULT_ROLE = pipeline_writer
    DEFAULT_WAREHOUSE = compute_wh
    COMMENT = 'Dagster Cloud ingestion service account';
GRANT ROLE pipeline_writer TO USER dagster_svc;

CREATE USER IF NOT EXISTS dbt_svc
    PASSWORD = 'obontong'
    DEFAULT_ROLE = dbt_transformer
    DEFAULT_WAREHOUSE = compute_wh
    COMMENT = 'dbt transformation service account';
GRANT ROLE dbt_transformer TO USER dbt_svc;

CREATE USER IF NOT EXISTS superset_svc
    PASSWORD = 'obontong'
    DEFAULT_ROLE = bi_reader
    DEFAULT_WAREHOUSE = compute_wh
    COMMENT = 'Apache Superset read-only service account';
GRANT ROLE bi_reader TO USER superset_svc;

-- 6. Row-level audit columns policy (tag sensitive tables)
CREATE TAG IF NOT EXISTS CRYPTO_DB.RAW.data_classification
    ALLOWED_VALUES 'public', 'internal', 'confidential';

ALTER TABLE CRYPTO_DB.RAW.CRYPTO_MARKET_RAW
    SET TAG CRYPTO_DB.RAW.data_classification = 'internal';


