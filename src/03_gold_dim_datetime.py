# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE uc_sa_br_dev.gold_finance_investments_market_analysis.dim_datetime
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   datetime_value AS datetime_id,
# MAGIC   TO_DATE(datetime_value) AS date_id,
# MAGIC   datetime_value AS datetime,
# MAGIC
# MAGIC   DATE_FORMAT(datetime_value, 'dd/MM HH:00') AS datetime_label,
# MAGIC
# MAGIC   YEAR(datetime_value) AS year,
# MAGIC   QUARTER(datetime_value) AS quarter,
# MAGIC   MONTH(datetime_value) AS month,
# MAGIC   DAY(datetime_value) AS day,
# MAGIC   HOUR(datetime_value) AS hour,
# MAGIC   DATE_FORMAT(datetime_value, 'yyyy-MM') AS year_month,
# MAGIC
# MAGIC   DAYOFWEEK(datetime_value) AS day_of_week,
# MAGIC
# MAGIC   CASE DAYOFWEEK(datetime_value)
# MAGIC     WHEN 1 THEN 'Domingo'
# MAGIC     WHEN 2 THEN 'Segunda'
# MAGIC     WHEN 3 THEN 'Terça'
# MAGIC     WHEN 4 THEN 'Quarta'
# MAGIC     WHEN 5 THEN 'Quinta'
# MAGIC     WHEN 6 THEN 'Sexta'
# MAGIC     WHEN 7 THEN 'Sábado'
# MAGIC   END AS day_of_week_name,
# MAGIC
# MAGIC   CASE 
# MAGIC     WHEN DAYOFWEEK(datetime_value) IN (1, 7) THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END AS is_weekend,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN HOUR(datetime_value) BETWEEN 0 AND 5 THEN 'Madrugada'
# MAGIC     WHEN HOUR(datetime_value) BETWEEN 6 AND 11 THEN 'Manhã'
# MAGIC     WHEN HOUR(datetime_value) BETWEEN 12 AND 17 THEN 'Tarde'
# MAGIC     ELSE 'Noite'
# MAGIC   END AS hour_period
# MAGIC
# MAGIC FROM (
# MAGIC   SELECT EXPLODE(
# MAGIC     SEQUENCE(
# MAGIC       TIMESTAMP('2024-01-01 00:00:00'),
# MAGIC       TIMESTAMP('2027-12-31 23:00:00'),
# MAGIC       INTERVAL 1 HOUR
# MAGIC     )
# MAGIC   ) AS datetime_value
# MAGIC );
