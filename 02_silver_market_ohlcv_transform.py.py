# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Documentação Silver Layer
# MAGIC %md
# MAGIC # Camada Silver: Transformação de Dados OHLCV
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Transformar dados brutos da Bronze (Poloniex + Binance) em dados limpos, tipados e validados, unificados por contexto (OHLCV), prontos para analytics cross-exchange.
# MAGIC
# MAGIC ## Princípios da Camada Silver
# MAGIC
# MAGIC **Transformações Permitidas:**
# MAGIC * Type casting (string → DECIMAL, long → TIMESTAMP)
# MAGIC * Conversões de unidade (millis → timestamp)
# MAGIC * Campos derivados simples (identificador da exchange)
# MAGIC * Validações de qualidade de dados
# MAGIC * Limpeza e normalização de dados
# MAGIC
# MAGIC **Estratégia de Particionamento:**
# MAGIC * Manter `rate_date` para consistência com Bronze
# MAGIC * Habilitar partition pruning para queries eficientes
# MAGIC * Dynamic partition overwrite para reprocessamento
# MAGIC
# MAGIC ## Transformações
# MAGIC
# MAGIC ### 1. Normalização de Symbol
# MAGIC ```sql
# MAGIC -- Poloniex: já vem com underscore (BTC_USDT)
# MAGIC symbol AS symbol
# MAGIC
# MAGIC -- Binance: adicionar underscore (BTCUSDT → BTC_USDT)
# MAGIC REGEXP_REPLACE(symbol, '([A-Z]+)(USDT|USDC|BTC|ETH)', '$1_$2') AS symbol
# MAGIC ```
# MAGIC *Justificativa:* Poloniex usa `BTC_USDT` (com underscore), Binance usa `BTCUSDT` (sem underscore). Silver padroniza com underscore para garantir que o mesmo par de trading tenha identificador único independente da exchange.
# MAGIC
# MAGIC *Padrão Silver:* `{BASE}_{QUOTE}` (ex: `BTC_USDT`, `ETH_USDT`, `SOL_USDT`)
# MAGIC
# MAGIC ### 2. Identificador da Exchange
# MAGIC ```sql
# MAGIC 'poloniex' AS exchange  -- para dados bronze_poloniex_ohlcv
# MAGIC 'binance' AS exchange   -- para dados bronze_binance_ohlcv
# MAGIC ```
# MAGIC *Justificativa:* Bronze tem schemas separados por fonte (domain=exchange). Silver unifica em um único schema por contexto (domain=market), adicionando coluna `exchange` para rastreabilidade e analytics cross-exchange na Gold.
# MAGIC
# MAGIC ### 3. Type Casting de OHLCV
# MAGIC ```sql
# MAGIC CAST(open AS DECIMAL(18,8)) AS open
# MAGIC CAST(high AS DECIMAL(18,8)) AS high
# MAGIC CAST(low AS DECIMAL(18,8)) AS low
# MAGIC CAST(close AS DECIMAL(18,8)) AS close
# MAGIC CAST(volume AS DECIMAL(18,8)) AS volume
# MAGIC ```
# MAGIC *Justificativa:* Bronze armazena como string (tipo nativo da API). Silver converte para DECIMAL para cálculos numéricos precisos.
# MAGIC
# MAGIC ### 4. Conversão de Timestamps
# MAGIC ```sql
# MAGIC FROM_UNIXTIME(start_time_ms / 1000) AS open_time
# MAGIC FROM_UNIXTIME(close_time_ms / 1000) AS close_time
# MAGIC ```
# MAGIC *Justificativa:* Bronze armazena timestamps em millis (formato da API). Silver converte para TIMESTAMP para queries temporais.
# MAGIC
# MAGIC ### 5. Validações de Qualidade
# MAGIC * Verificar nulos em campos críticos
# MAGIC * Validar relações OHLC: `high >= low`, `high >= open`, `high >= close`
# MAGIC * Validar volume positivo: `volume > 0`
# MAGIC * Validar timestamps válidos: `close_time > open_time`
# MAGIC
# MAGIC ## Schema Silver
# MAGIC
# MAGIC | Campo | Tipo | Origem | Transformação |
# MAGIC | --- | --- | --- | --- |
# MAGIC | symbol | STRING | Bronze | Nenhuma |
# MAGIC | exchange | STRING | **Adicionado** | `'poloniex'` |
# MAGIC | open | DECIMAL(18,8) | Bronze | CAST de string |
# MAGIC | high | DECIMAL(18,8) | Bronze | CAST de string |
# MAGIC | low | DECIMAL(18,8) | Bronze | CAST de string |
# MAGIC | close | DECIMAL(18,8) | Bronze | CAST de string |
# MAGIC | volume | DECIMAL(18,8) | Bronze | CAST de string |
# MAGIC | open_time | TIMESTAMP | Bronze | FROM_UNIXTIME(start_time_ms/1000) |
# MAGIC | close_time | TIMESTAMP | Bronze | FROM_UNIXTIME(close_time_ms/1000) |
# MAGIC | interval | STRING | Bronze | Nenhuma |
# MAGIC | trade_count | LONG | Bronze | Nenhuma |
# MAGIC | rate_date | DATE | Bronze | Preservado para particionamento |
# MAGIC | ingested_at | TIMESTAMP | Bronze | Preservado para auditoria |
# MAGIC
# MAGIC ## Estratégia de Escrita
# MAGIC
# MAGIC * **Modo:** `overwrite` com `dynamic_partition_overwrite=True`
# MAGIC * **Particionamento:** `rate_date`
# MAGIC * **Cadência:** D-1 incremental (mesma da Bronze)
# MAGIC * **Destino:** `uc_sa_br_dev.silver_market_ohlcv.hourly`
# MAGIC
# MAGIC ## Fontes de Dados
# MAGIC
# MAGIC * **Poloniex:** `uc_sa_br_dev.bronze_poloniex_ohlcv.hourly`
# MAGIC * **Binance:** `uc_sa_br_dev.bronze_binance_ohlcv.hourly`
# MAGIC
# MAGIC **Estratégia de Unificação:** UNION ALL de ambas as fontes com identificador `exchange` para distinguir origem.

# COMMAND ----------

# DBTITLE 1,Setup: Imports and Configuration
import logging
import sys
from datetime import datetime, timedelta, timezone

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

ZORDON_SRC_PATH = "/Workspace/Repos/CryptoLake/zordon-data-utils/src"
if ZORDON_SRC_PATH not in sys.path:
    sys.path.append(ZORDON_SRC_PATH)

import zordon
from pyspark.sql import functions as F

TABLE_NAME = "hourly"

proj = zordon.Project(
    spark=spark,
    country="br",
    region="sa",
    environment="dev",
)

silver_market = proj.client(
    layer="silver",
    domain="market",
    subdomain="ohlcv",
)

target_fqn = silver_market.governance.fqn(TABLE_NAME)
logging.info(f"Target FQN: {target_fqn}")


# COMMAND ----------

# DBTITLE 1,Read Bronze Data
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW bronze_poloniex AS 
    SELECT * FROM uc_sa_br_dev.bronze_poloniex_ohlcv.hourly
""")

spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW bronze_binance AS 
    SELECT * FROM uc_sa_br_dev.bronze_binance_ohlcv.hourly
""")

logging.info("Bronze data loaded from Poloniex and Binance")

# COMMAND ----------

# DBTITLE 1,Transform: Bronze to Silver
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW silver_transformed AS
    
    -- Poloniex data
    SELECT
        'poloniex' AS exchange,
        symbol AS symbol,
        interval,
        trade_count,
        CAST(open AS DECIMAL(18,8)) AS open,
        CAST(high AS DECIMAL(18,8)) AS high,
        CAST(low AS DECIMAL(18,8)) AS low,
        CAST(close AS DECIMAL(18,8)) AS close,
        CAST(volume AS DECIMAL(18,8)) AS volume,
        CAST(FROM_UNIXTIME(close_time_ms / 1000) AS TIMESTAMP) AS close_time,
        CAST(FROM_UNIXTIME(start_time_ms / 1000) AS TIMESTAMP) AS open_time,
        rate_date,
        ingested_at
    FROM bronze_poloniex
    WHERE CAST(volume AS DECIMAL(18,8)) > 0
    
    UNION ALL
    
    -- Binance data
    SELECT
        'binance' AS exchange,
        REGEXP_REPLACE(symbol, '([A-Z]+)(USDT|USDC|BTC|ETH)', '$1_$2') AS symbol,
        interval,
        trade_count,
        CAST(open AS DECIMAL(18,8)) AS open,
        CAST(high AS DECIMAL(18,8)) AS high,
        CAST(low AS DECIMAL(18,8)) AS low,
        CAST(close AS DECIMAL(18,8)) AS close,
        CAST(volume AS DECIMAL(18,8)) AS volume,
        CAST(FROM_UNIXTIME(close_time_ms / 1000) AS TIMESTAMP) AS close_time,
        CAST(FROM_UNIXTIME(open_time_ms / 1000) AS TIMESTAMP) AS open_time,
        rate_date,
        ingested_at
    FROM bronze_binance
    WHERE CAST(volume AS DECIMAL(18,8)) > 0
""")

logging.info("Silver transformations applied for both exchanges")

# COMMAND ----------

# DBTITLE 1,Data Quality Validations
null_check = spark.sql("""
    SELECT 
        COUNT(*) AS total_rows,
        SUM(CASE WHEN symbol IS NULL THEN 1 ELSE 0 END) AS null_symbol,
        SUM(CASE WHEN open IS NULL THEN 1 ELSE 0 END) AS null_open,
        SUM(CASE WHEN high IS NULL THEN 1 ELSE 0 END) AS null_high,
        SUM(CASE WHEN low IS NULL THEN 1 ELSE 0 END) AS null_low,
        SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END) AS null_close,
        SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) AS null_volume
    FROM silver_transformed
""").collect()[0]

invalid_ohlc = spark.sql("""
    SELECT COUNT(*) FROM silver_transformed
    WHERE high < low OR high < open OR high < close OR low > open OR low > close
""").collect()[0][0]

invalid_volume = spark.sql("""
    SELECT COUNT(*) FROM silver_transformed WHERE volume <= 0
""").collect()[0][0]

invalid_timestamp = spark.sql("""
    SELECT COUNT(*) FROM silver_transformed
    WHERE open_time IS NULL OR close_time IS NULL OR close_time <= open_time
""").collect()[0][0]

logging.info(f"Validation - Total rows: {null_check.total_rows}")
logging.info(f"Validation - Invalid OHLC: {invalid_ohlc}")
logging.info(f"Validation - Invalid volume: {invalid_volume}")
logging.info(f"Validation - Invalid timestamps: {invalid_timestamp}")

assert invalid_ohlc == 0, "OHLC validation failed"
assert invalid_volume == 0, "Volume validation failed"
assert invalid_timestamp == 0, "Timestamp validation failed"

# COMMAND ----------

# DBTITLE 1,Write Silver Table
df_silver = spark.sql("SELECT * FROM silver_transformed")

partitions = df_silver.select("rate_date").distinct().collect()
partition_dates = sorted([row.rate_date for row in partitions])
logging.info(f"Writing {len(partition_dates)} partition(s): {partition_dates}")

# COMMAND ----------

# DBTITLE 1,Cell 18
written_fqn = silver_market.write_table(
    df=df_silver,
    table_name=TABLE_NAME,
    mode="overwrite",
    partition_cols=["rate_date"],
    dynamic_partition_overwrite=True,
)

logging.info(f"Silver table written: {written_fqn}")

# COMMAND ----------

# DBTITLE 1,Exercício 6: Validação Final - IMPLEMENTAÇÃO
display(spark.sql("""
    SELECT 
        symbol,
        exchange,
        COUNT(*) AS rows,
        MIN(rate_date) AS min_date,
        MAX(rate_date) AS max_date
    FROM uc_sa_br_dev.silver_market_ohlcv.hourly
    GROUP BY symbol, exchange
    ORDER BY symbol
"""))

logging.info("Silver layer processing completed")
