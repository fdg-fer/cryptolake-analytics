# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Documentação - Dimensões Temporais
# MAGIC %md
# MAGIC # Gold Layer: Temporal Dimensions
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Criar dimensões temporais para o star schema com range baseado nos dados reais da Silver.
# MAGIC
# MAGIC ## Estratégia
# MAGIC
# MAGIC ### dim_date (Diária)
# MAGIC * **Fonte:** MIN/MAX de `rate_date` da Silver
# MAGIC * **Granularidade:** Diária (sem hora)
# MAGIC * **Uso:** Fact tables diárias
# MAGIC * **Atributos:** date, day_of_week, month, quarter, year, is_weekend, etc.
# MAGIC
# MAGIC ### dim_datetime (Horária)
# MAGIC * **Fonte:** MIN/MAX de `open_time` e `close_time` da Silver
# MAGIC * **Granularidade:** Horária (timestamp completo)
# MAGIC * **Uso:** Fact tables horárias
# MAGIC * **Atributos:** datetime, hour, date, day_of_week, hour_period, etc.
# MAGIC
# MAGIC ## Benefícios
# MAGIC
# MAGIC * **Performance:** Dimensões menores = joins mais rápidos
# MAGIC * **Manutenção:** Range cresce automaticamente com novos dados
# MAGIC * **Semântica:** Separação clara entre date e datetime
# MAGIC * **Economia:** Sem overhead de registros órfãos
# MAGIC
# MAGIC ## Arquitetura Zordon
# MAGIC
# MAGIC * **Layer:** gold
# MAGIC * **Domain:** finance
# MAGIC * **Subdomain:** investments
# MAGIC * **Data Product:** market_analysis

# COMMAND ----------

# DBTITLE 1,Create dim_date (Daily)
# MAGIC %sql
# MAGIC -- Dimension: Date (Daily Granularity)
# MAGIC -- Range: Based on actual data from Silver layer
# MAGIC
# MAGIC CREATE OR REPLACE TABLE uc_sa_br_dev.gold_finance_investments_market_analysis.dim_date
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH silver_range AS (
# MAGIC   SELECT
# MAGIC     MIN(rate_date) AS min_date,
# MAGIC     MAX(rate_date) AS max_date
# MAGIC   FROM uc_sa_br_dev.silver_market_ohlcv.hourly
# MAGIC )
# MAGIC SELECT
# MAGIC   date_value AS date_id,
# MAGIC   date_value AS date,
# MAGIC   
# MAGIC   DATE_FORMAT(date_value, 'dd/MM/yyyy') AS date_label,
# MAGIC   
# MAGIC   YEAR(date_value) AS year,
# MAGIC   QUARTER(date_value) AS quarter,
# MAGIC   MONTH(date_value) AS month,
# MAGIC   DAY(date_value) AS day,
# MAGIC   DATE_FORMAT(date_value, 'yyyy-MM') AS year_month,
# MAGIC   
# MAGIC   DAYOFWEEK(date_value) AS day_of_week,
# MAGIC   
# MAGIC   CASE DAYOFWEEK(date_value)
# MAGIC     WHEN 1 THEN 'Sunday'
# MAGIC     WHEN 2 THEN 'Monday'
# MAGIC     WHEN 3 THEN 'Tuesday'
# MAGIC     WHEN 4 THEN 'Wednesday'
# MAGIC     WHEN 5 THEN 'Thursday'
# MAGIC     WHEN 6 THEN 'Friday'
# MAGIC     WHEN 7 THEN 'Saturday'
# MAGIC   END AS day_of_week_name,
# MAGIC   
# MAGIC   CASE 
# MAGIC     WHEN DAYOFWEEK(date_value) IN (1, 7) THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END AS is_weekend,
# MAGIC   
# MAGIC   CURRENT_TIMESTAMP() AS created_at
# MAGIC   
# MAGIC FROM (
# MAGIC   SELECT EXPLODE(
# MAGIC     SEQUENCE(
# MAGIC       (SELECT min_date FROM silver_range),
# MAGIC       (SELECT max_date FROM silver_range),
# MAGIC       INTERVAL 1 DAY
# MAGIC     )
# MAGIC   ) AS date_value
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Create dim_datetime (Hourly)
# MAGIC %sql
# MAGIC -- Dimension: Datetime (Hourly Granularity)
# MAGIC -- Range: Based on actual data from Silver layer
# MAGIC
# MAGIC CREATE OR REPLACE TABLE uc_sa_br_dev.gold_finance_investments_market_analysis.dim_datetime
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH silver_range AS (
# MAGIC   SELECT
# MAGIC     DATE_TRUNC('HOUR', MIN(open_time)) AS min_datetime,
# MAGIC     DATE_TRUNC('HOUR', MAX(close_time)) AS max_datetime
# MAGIC   FROM uc_sa_br_dev.silver_market_ohlcv.hourly
# MAGIC )
# MAGIC SELECT
# MAGIC   datetime_value AS datetime_id,
# MAGIC   TO_DATE(datetime_value) AS date_id,
# MAGIC   datetime_value AS datetime,
# MAGIC
# MAGIC   DATE_FORMAT(datetime_value, 'dd/MM/yyyy HH:00') AS datetime_label,
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
# MAGIC     WHEN 1 THEN 'Sunday'
# MAGIC     WHEN 2 THEN 'Monday'
# MAGIC     WHEN 3 THEN 'Tuesday'
# MAGIC     WHEN 4 THEN 'Wednesday'
# MAGIC     WHEN 5 THEN 'Thursday'
# MAGIC     WHEN 6 THEN 'Friday'
# MAGIC     WHEN 7 THEN 'Saturday'
# MAGIC   END AS day_of_week_name,
# MAGIC
# MAGIC   CASE 
# MAGIC     WHEN DAYOFWEEK(datetime_value) IN (1, 7) THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END AS is_weekend,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN HOUR(datetime_value) BETWEEN 0 AND 5 THEN 'Dawn'
# MAGIC     WHEN HOUR(datetime_value) BETWEEN 6 AND 11 THEN 'Morning'
# MAGIC     WHEN HOUR(datetime_value) BETWEEN 12 AND 17 THEN 'Afternoon'
# MAGIC     ELSE 'Night'
# MAGIC   END AS hour_period,
# MAGIC   
# MAGIC   CURRENT_TIMESTAMP() AS created_at
# MAGIC
# MAGIC FROM (
# MAGIC   SELECT EXPLODE(
# MAGIC     SEQUENCE(
# MAGIC       (SELECT min_datetime FROM silver_range),
# MAGIC       (SELECT max_datetime FROM silver_range),
# MAGIC       INTERVAL 1 HOUR
# MAGIC     )
# MAGIC   ) AS datetime_value
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Validate Dimensions
# MAGIC %sql
# MAGIC -- Validate both temporal dimensions
# MAGIC
# MAGIC SELECT 
# MAGIC   'dim_date' AS dimension,
# MAGIC   COUNT(*) AS total_records,
# MAGIC   MIN(date_id) AS min_value,
# MAGIC   MAX(date_id) AS max_value,
# MAGIC   DATEDIFF(MAX(date_id), MIN(date_id)) + 1 AS expected_records
# MAGIC FROM uc_sa_br_dev.gold_finance_investments_market_analysis.dim_date
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'dim_datetime' AS dimension,
# MAGIC   COUNT(*) AS total_records,
# MAGIC   CAST(MIN(datetime_id) AS STRING) AS min_value,
# MAGIC   CAST(MAX(datetime_id) AS STRING) AS max_value,
# MAGIC   (DATEDIFF(MAX(datetime_id), MIN(datetime_id)) + 1) * 24 AS expected_records
# MAGIC FROM uc_sa_br_dev.gold_finance_investments_market_analysis.dim_datetime
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Silver reference' AS dimension,
# MAGIC   COUNT(DISTINCT rate_date) AS total_records,
# MAGIC   CAST(MIN(rate_date) AS STRING) AS min_value,
# MAGIC   CAST(MAX(rate_date) AS STRING) AS max_value,
# MAGIC   COUNT(DISTINCT rate_date) AS expected_records
# MAGIC FROM uc_sa_br_dev.silver_market_ohlcv.hourly;
