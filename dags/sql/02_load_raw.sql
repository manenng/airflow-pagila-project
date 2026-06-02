COPY raw_bank(age, job, marital, education, default_flag, housing, loan, contact, month, day_of_week, duration, campaign, pdays, previous, poutcome, emp_var_rate, cons_price_idx, cons_conf_idx, euribor3m, nr_employed, y)
FROM '/tmp/bank-additional-full.csv' DELIMITER ';' CSV HEADER;
