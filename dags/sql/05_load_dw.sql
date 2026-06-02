INSERT INTO dw.dim_customer
SELECT * FROM public.source_customer;

INSERT INTO dw.dim_campaign
SELECT * FROM public.source_campaign;

INSERT INTO dw.fact_marketing
SELECT * FROM public.source_marketing_fact;
