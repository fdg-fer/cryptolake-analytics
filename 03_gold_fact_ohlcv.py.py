# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Documentação - fact_ohlcv
# MAGIC %md
# MAGIC # Gold Layer: Fact Table Market Hourly
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Criar tabela de fatos (fact table) do star schema com métricas OHLCV horárias e foreign keys para dimensões.
# MAGIC
# MAGIC ## Estratégia
# MAGIC
# MAGIC * **Fonte:** `uc_sa_br_dev.silver_market_ohlcv.hourly` + dimensões Gold
# MAGIC * **Destino:** `uc_sa_br_dev.gold_finance_investments_market_analysis.fact_market_hourly`
# MAGIC * **Granularidade:** 1 registro por (exchange, symbol, timestamp)
# MAGIC * **Tipo:** Fact table (sem particionamento físico)
# MAGIC
# MAGIC ## Schema
# MAGIC
# MAGIC ### Foreign Keys
# MAGIC * symbol_id → dim_symbol
# MAGIC * exchange_id → dim_exchange
# MAGIC * datetime_id → dim_datetime
# MAGIC
# MAGIC ### Métricas OHLCV
# MAGIC * open_price, high_price, low_price, close_price (DECIMAL)
# MAGIC * volume (DECIMAL)
# MAGIC * trade_count (BIGINT)
# MAGIC * variation_1h_pct (DECIMAL) - métrica calculada
# MAGIC
# MAGIC ### Metadados
# MAGIC * close_time (TIMESTAMP) - timestamp de fechamento do candle
# MAGIC * ingested_at (TIMESTAMP) - timestamp de ingestão no lakehouse
# MAGIC
# MAGIC ## Arquitetura Zordon
# MAGIC
# MAGIC * **Layer:** gold
# MAGIC * **Domain:** finance
# MAGIC * **Subdomain:** investments
# MAGIC * **Data Product:** market_analysis

# COMMAND ----------

# DBTITLE 1,Setup: Imports and Zordon Client
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

ZORDON_SRC_PATH = "/Workspace/Repos/CryptoLake/zordon-data-utils/src"
if ZORDON_SRC_PATH not in sys.path:
    sys.path.append(ZORDON_SRC_PATH)

import zordon
from pyspark.sql import functions as F

TABLE_NAME = "fact_market_hourly"

proj = zordon.Project(
    spark=spark,
    country="br",
    region="sa",
    environment="dev",
)

gold_star = proj.client(
    layer="gold",
    domain="finance",
    subdomain="investments",
    data_product="market_analysis",
)

target_fqn = gold_star.governance.fqn(TABLE_NAME)
logging.info(f"Target table: {target_fqn}")

# COMMAND ----------

# DBTITLE 1,Read Silver and Create Fact Table
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW fact_market_hourly_final AS
    SELECT
        -- Foreign Keys
        ds.symbol_id,
        de.exchange_id,
        dd.datetime_id,
        
        -- Metrics (OHLCV)
        s.open AS open_price,
        s.high AS high_price,
        s.low AS low_price,
        s.close AS close_price,
        s.volume,
        s.trade_count,
        
        -- Calculated Metric
        ((s.close - s.open) / s.open * 100) AS variation_1h_pct,
        
        -- Metadata
        s.close_time,
        s.ingested_at
        
    FROM uc_sa_br_dev.silver_market_ohlcv.hourly s
    
    INNER JOIN uc_sa_br_dev.gold_finance_investments_market_analysis.dim_exchange de
        ON s.exchange = de.exchange_name
    
    INNER JOIN uc_sa_br_dev.gold_finance_investments_market_analysis.dim_symbol ds
        ON s.symbol = ds.symbol_name
    
    INNER JOIN uc_sa_br_dev.gold_finance_investments_market_analysis.dim_datetime dd
        ON s.open_time = dd.datetime_id
""")

logging.info("Fact table fact_market_hourly created")

# COMMAND ----------

# DBTITLE 1,Validate Data Quality
quality_checks = spark.sql("""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(CASE WHEN open_price IS NULL THEN 1 END) AS null_open,
        COUNT(CASE WHEN high_price IS NULL THEN 1 END) AS null_high,
        COUNT(CASE WHEN low_price IS NULL THEN 1 END) AS null_low,
        COUNT(CASE WHEN close_price IS NULL THEN 1 END) AS null_close,
        COUNT(CASE WHEN volume IS NULL OR volume <= 0 THEN 1 END) AS invalid_volume,
        COUNT(CASE WHEN high_price < low_price THEN 1 END) AS invalid_high_low,
        COUNT(CASE WHEN high_price < open_price THEN 1 END) AS invalid_high_open,
        COUNT(CASE WHEN high_price < close_price THEN 1 END) AS invalid_high_close,
        COUNT(CASE WHEN low_price > open_price THEN 1 END) AS invalid_low_open,
        COUNT(CASE WHEN low_price > close_price THEN 1 END) AS invalid_low_close
    FROM fact_market_hourly_final
""")

display(quality_checks)
logging.info("Data quality validation completed")

# COMMAND ----------

# DBTITLE 1,Write Gold Table
df_fact = spark.table("fact_market_hourly_final")

row_count = df_fact.count()
logging.info(f"Writing {row_count} rows")

written_fqn = gold_star.write_table(
    df=df_fact,
    table_name=TABLE_NAME,
    mode="overwrite",
)

logging.info(f"Gold fact table written: {written_fqn}")

# COMMAND ----------

# DBTITLE 1,Final Validation
summary = spark.sql(f"""
    SELECT
        COUNT(*) AS total_records,
        COUNT(DISTINCT exchange_id) AS distinct_exchanges,
        COUNT(DISTINCT symbol_id) AS distinct_symbols,
        COUNT(DISTINCT datetime_id) AS distinct_datetimes,
        MIN(close_time) AS min_datetime,
        MAX(close_time) AS max_datetime,
        ROUND(AVG(variation_1h_pct), 2) AS avg_variation_pct
    FROM {target_fqn}
""")

display(summary)
logging.info("fact_market_hourly validation completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uc_sa_br_dev.gold_finance_investments_market_analysis.fact_market_hourly
