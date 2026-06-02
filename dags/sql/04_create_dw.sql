CREATE SCHEMA IF NOT EXISTS dw;

DROP TABLE IF EXISTS dw.fact_marketing;
DROP TABLE IF EXISTS dw.dim_customer;
DROP TABLE IF EXISTS dw.dim_campaign;

CREATE TABLE dw.dim_customer (
    customer_id BIGINT PRIMARY KEY,
    age INTEGER,
    job VARCHAR(50),
    marital VARCHAR(30),
    education VARCHAR(50),
    default_flag VARCHAR(20),
    housing VARCHAR(20),
    loan VARCHAR(20)
);

CREATE TABLE dw.dim_campaign (
    campaign_id BIGINT PRIMARY KEY,
    contact VARCHAR(30),
    month VARCHAR(20),
    day_of_week VARCHAR(20),
    duration INTEGER,
    campaign INTEGER,
    pdays INTEGER,
    previous INTEGER,
    poutcome VARCHAR(50)
);

CREATE TABLE dw.fact_marketing (
    fact_id BIGINT PRIMARY KEY,
    customer_id BIGINT REFERENCES dw.dim_customer(customer_id),
    campaign_id BIGINT REFERENCES dw.dim_campaign(campaign_id),
    emp_var_rate NUMERIC,
    cons_price_idx NUMERIC,
    cons_conf_idx NUMERIC,
    euribor3m NUMERIC,
    nr_employed NUMERIC,
    subscribed INTEGER
);
