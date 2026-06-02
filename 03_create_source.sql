DROP TABLE IF EXISTS source_customer;
CREATE TABLE source_customer AS
SELECT id AS customer_id, age, job, marital, education, default_flag, housing, loan
FROM raw_bank;

DROP TABLE IF EXISTS source_campaign;
CREATE TABLE source_campaign AS
SELECT id AS campaign_id, contact, month, day_of_week, duration, campaign, pdays, previous, poutcome
FROM raw_bank;

DROP TABLE IF EXISTS source_marketing_fact;
CREATE TABLE source_marketing_fact AS
SELECT
    id AS fact_id,
    id AS customer_id,
    id AS campaign_id,
    emp_var_rate, cons_price_idx, cons_conf_idx, euribor3m, nr_employed,
    CASE WHEN y = 'yes' THEN 1 ELSE 0 END AS subscribed
FROM raw_bank;
