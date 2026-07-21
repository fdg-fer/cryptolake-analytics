# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Documentação - fact_ohlcv_daily
# MAGIC %md
# MAGIC # Gold Layer: Fact Table Market Daily
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Agregar dados horários em métricas OHLCV diárias com variação percentual diária.
# MAGIC
# MAGIC ## Estratégia
# MAGIC
# MAGIC * **Fonte:** `uc_sa_br_dev.gold_finance_investments_market_analysis.fact_market_hourly`
# MAGIC * **Destino:** `uc_sa_br_dev.gold_finance_investments_market_analysis.fact_market_daily`
# MAGIC * **Granularidade:** 1 registro por (exchange, symbol, date)
# MAGIC * **Agregação:** OHLCV correto por dia
# MAGIC
# MAGIC ## Schema
# MAGIC
# MAGIC ### Foreign Keys
# MAGIC * symbol_id → dim_symbol
# MAGIC * exchange_id → dim_exchange
# MAGIC * date_id (DATE) - data do candle diário
# MAGIC
# MAGIC ### Métricas OHLCV Diárias
# MAGIC * open_price (DECIMAL) - abertura do primeiro candle do dia
# MAGIC * high_price (DECIMAL) - maior high do dia
# MAGIC * low_price (DECIMAL) - menor low do dia
# MAGIC * close_price (DECIMAL) - fechamento do último candle do dia
# MAGIC * volume (DECIMAL) - soma do volume do dia
# MAGIC * trade_count (BIGINT) - soma de trades do dia
# MAGIC * variation_daily_pct (DECIMAL) - variação percentual do dia (close vs open)
# MAGIC * variation_prev_day_pct (DECIMAL) - variação vs dia anterior (close hoje vs close ontem)
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
from pyspark.sql.window import Window

TABLE_NAME = "fact_market_daily"

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

# DBTITLE 1,Aggregate Hourly to Daily
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW fact_market_daily_base AS
    WITH ordered_data AS (
        SELECT
            symbol_id,
            exchange_id,
            DATE(datetime_id) AS date_id,
            datetime_id,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            trade_count,
            close_time,
            ingested_at,
            ROW_NUMBER() OVER (
                PARTITION BY symbol_id, exchange_id, DATE(datetime_id)
                ORDER BY datetime_id ASC
            ) AS rn_first,
            ROW_NUMBER() OVER (
                PARTITION BY symbol_id, exchange_id, DATE(datetime_id)
                ORDER BY datetime_id DESC
            ) AS rn_last
        FROM uc_sa_br_dev.gold_finance_investments_market_analysis.fact_market_hourly
    ),
    first_candle AS (
        SELECT symbol_id, exchange_id, date_id, open_price AS day_open
        FROM ordered_data
        WHERE rn_first = 1
    ),
    last_candle AS (
        SELECT symbol_id, exchange_id, date_id, close_price AS day_close, close_time
        FROM ordered_data
        WHERE rn_last = 1
    )
    SELECT
        h.symbol_id,
        h.exchange_id,
        DATE(h.datetime_id) AS date_id,
        fc.day_open AS open_price,
        MAX(h.high_price) AS high_price,
        MIN(h.low_price) AS low_price,
        lc.day_close AS close_price,
        SUM(h.volume) AS volume,
        SUM(h.trade_count) AS trade_count,
        lc.close_time,
        MAX(h.ingested_at) AS ingested_at
    FROM uc_sa_br_dev.gold_finance_investments_market_analysis.fact_market_hourly h
    JOIN first_candle fc ON h.symbol_id = fc.symbol_id 
        AND h.exchange_id = fc.exchange_id 
        AND DATE(h.datetime_id) = fc.date_id
    JOIN last_candle lc ON h.symbol_id = lc.symbol_id 
        AND h.exchange_id = lc.exchange_id 
        AND DATE(h.datetime_id) = lc.date_id
    GROUP BY h.symbol_id, h.exchange_id, DATE(h.datetime_id), fc.day_open, lc.day_close, lc.close_time
""")

logging.info("Daily base aggregation created")

# COMMAND ----------

# DBTITLE 1,Calculate Daily Variations
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW fact_market_daily_final AS
    SELECT DISTINCT
        symbol_id,
        exchange_id,
        date_id,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        trade_count,
        
        -- Variation within the day (close vs open)
        ((close_price - open_price) / open_price * 100) AS variation_daily_pct,
        
        -- Variation vs previous day (close today vs close yesterday)
        ((close_price - LAG(close_price, 1) OVER (
            PARTITION BY exchange_id, symbol_id 
            ORDER BY date_id
        )) / LAG(close_price, 1) OVER (
            PARTITION BY exchange_id, symbol_id 
            ORDER BY date_id
        ) * 100) AS variation_prev_day_pct,
        
        close_time,
        ingested_at
        
    FROM fact_market_daily_base
""")

logging.info("Daily variations calculated")

# COMMAND ----------

# DBTITLE 1,Write Gold Table
df_daily = spark.table("fact_market_daily_final")

row_count = df_daily.count()
logging.info(f"Writing {row_count} rows")

written_fqn = gold_star.write_table(
    df=df_daily,
    table_name=TABLE_NAME,
    mode="overwrite",
)

logging.info(f"Gold daily fact table written: {written_fqn}")

# COMMAND ----------

# DBTITLE 1,Final Validation
summary = spark.sql(f"""
    SELECT
        COUNT(*) AS total_records,
        COUNT(DISTINCT exchange_id) AS distinct_exchanges,
        COUNT(DISTINCT symbol_id) AS distinct_symbols,
        COUNT(DISTINCT date_id) AS distinct_dates,
        MIN(date_id) AS min_date,
        MAX(date_id) AS max_date,
        ROUND(AVG(variation_daily_pct), 2) AS avg_variation_daily,
        ROUND(AVG(variation_prev_day_pct), 2) AS avg_variation_prev_day
    FROM {target_fqn}
""")

display(summary)
logging.info("fact_market_daily validation completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uc_sa_br_dev.gold_finance_investments_market_analysis.fact_market_daily
