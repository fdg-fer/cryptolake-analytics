# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Gold Layer: Analytics & Business Metrics
# MAGIC %md
# MAGIC # Camada Gold: Métricas de Trading Horário
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC A camada Gold transforma dados limpos da Silver adicionando **variação horária percentual** para análise de performance. Esta camada foca em:
# MAGIC
# MAGIC * **Renomeação OHLCV**: Padronização de nomes (open → open_price)
# MAGIC * **Variação Horária**: Percentual de valorização/desvalorização em 1 hora
# MAGIC * **Granularidade Horária**: Mantém 1 registro por símbolo por hora (sem agregação)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Fluxo de Dados
# MAGIC
# MAGIC ```
# MAGIC Silver (silver_market_ohlcv.hourly)
# MAGIC   ↓ Renomear campos OHLCV (open → open_price, high → high_price, etc.)
# MAGIC   ↓ Calcular variação horária: (close - open) / open * 100
# MAGIC Gold (gold_finance_investments_ohlcv_metrics.hourly)
# MAGIC ```
# MAGIC
# MAGIC **Importante**: Não há agregação - cada vela horária da Silver vira 1 registro Gold com métricas adicionais.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Transformações da Camada Gold
# MAGIC
# MAGIC ### 1. Renomeação de Campos OHLCV
# MAGIC * `open` → `open_price`
# MAGIC * `high` → `high_price`
# MAGIC * `low` → `low_price`
# MAGIC * `close` → `close_price`
# MAGIC * `volume` → (mantém nome)
# MAGIC
# MAGIC ### 2. Métrica Calculada
# MAGIC
# MAGIC | Métrica | Fórmula | Descrição |
# MAGIC | --- | --- | --- |
# MAGIC | **variation_1h_pct** | `(close - open) / open * 100` | Variação % na vela horária |
# MAGIC
# MAGIC ### 3. Qualidade dos Dados
# MAGIC * Verificar que todas as velas têm OHLCV completos (sem nulos)
# MAGIC * Garantir relações OHLC: `high >= low`, `high >= open`, `high >= close`
# MAGIC * Validar `volume > 0`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Schema Esperado: gold_finance_investments_ohlcv_metrics.hourly
# MAGIC
# MAGIC | Coluna | Tipo | Descrição |
# MAGIC | --- | --- | --- |
# MAGIC | symbol | STRING | Par de negociação (ex: BTC_USDT) |
# MAGIC | exchange | STRING | Corretora de origem (poloniex) |
# MAGIC | rate_date | DATE | Data (chave de partição) |
# MAGIC | open_time | TIMESTAMP | Timestamp abertura da vela |
# MAGIC | close_time | TIMESTAMP | Timestamp fechamento da vela |
# MAGIC | **open_price** | DECIMAL(18,8) | Preço de abertura |
# MAGIC | **high_price** | DECIMAL(18,8) | Preço máximo |
# MAGIC | **low_price** | DECIMAL(18,8) | Preço mínimo |
# MAGIC | **close_price** | DECIMAL(18,8) | Preço de fechamento |
# MAGIC | volume | DECIMAL(18,8) | Volume negociado |
# MAGIC | **variation_1h_pct** | DECIMAL(18,8) | Variação % na vela |
# MAGIC | ingested_at | TIMESTAMP | Timestamp processamento |
# MAGIC
# MAGIC **Particionamento**: `rate_date` para consultas eficientes
# MAGIC
# MAGIC **Campos em negrito**: Novos campos calculados na Gold (não existem na Silver)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Fluxo de Processamento
# MAGIC
# MAGIC 1. Configurar cliente Zordon para governança da camada Gold
# MAGIC 2. Ler dados da Silver e criar view temporária
# MAGIC 3. Calcular métrica de variação horária (sem agregação)
# MAGIC 4. Validações de qualidade de dados (nulos, relações OHLC)
# MAGIC 5. Escrever tabela Gold com dynamic partition overwrite
# MAGIC 6. Validação final e sumário de métricas

# COMMAND ----------

# DBTITLE 1,Exercise 1: Setup Zordon - IMPLEMENTATION
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

gold_poloniex = proj.client(
    layer="gold",
    domain="finance",
    subdomain="investments",
    data_product="ohlcv_metrics",
)

target_fqn = gold_poloniex.governance.fqn(TABLE_NAME)
logging.info(f"Target FQN: {target_fqn}")



# COMMAND ----------

# DBTITLE 1,Exercise 2: Read Silver - IMPLEMENTATION
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW silver_hourly AS
    SELECT * FROM uc_sa_br_dev.silver_market_ohlcv.hourly
""")

logging.info("Silver data loaded into temporary view")

# COMMAND ----------

# DBTITLE 1,Exercise 3: Aggregations - IMPLEMENTATION
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW gold_hourly_metrics AS
    SELECT 
        symbol,
        exchange,
        rate_date,
        open_time,
        close_time,
        interval,
        trade_count,
        open AS open_price,
        high AS high_price,
        low AS low_price,
        close AS close_price,
        volume,
        ((close - open) / open) * 100 AS variation_1h_pct,
        CURRENT_TIMESTAMP() AS ingested_at
    FROM silver_hourly
    ORDER BY symbol, open_time
""")

logging.info("Gold metrics calculated")

# COMMAND ----------

# DBTITLE 1,Data Quality Validations
record_count = spark.sql("""
    SELECT 
        'Silver' as layer, COUNT(*) as record_count
    FROM silver_hourly
    UNION ALL
    SELECT 
        'Gold' as layer, COUNT(*) as record_count
    FROM gold_hourly_metrics
""").collect()

invalid_ohlc = spark.sql("""
    SELECT COUNT(*) FROM gold_hourly_metrics
    WHERE high_price < low_price OR high_price < open_price 
       OR high_price < close_price OR low_price > open_price 
       OR low_price > close_price OR open_price < 0 OR close_price < 0
""").collect()[0][0]

invalid_volume = spark.sql("""
    SELECT COUNT(*) FROM gold_hourly_metrics WHERE volume <= 0 OR volume IS NULL
""").collect()[0][0]

null_check = spark.sql("""
    SELECT 
        COUNT(*) as total_rows,
        SUM(CASE WHEN symbol IS NULL THEN 1 ELSE 0 END) as null_symbol,
        SUM(CASE WHEN open_price IS NULL THEN 1 ELSE 0 END) as null_open,
        SUM(CASE WHEN close_price IS NULL THEN 1 ELSE 0 END) as null_close,
        SUM(CASE WHEN variation_1h_pct IS NULL THEN 1 ELSE 0 END) as null_variation
    FROM gold_hourly_metrics
""").collect()[0]

silver_count = record_count[0][1]
gold_count = record_count[1][1]

logging.info(f"Validation - Silver: {silver_count} | Gold: {gold_count}")
logging.info(f"Validation - Invalid OHLC: {invalid_ohlc}")
logging.info(f"Validation - Invalid volume: {invalid_volume}")
logging.info(f"Validation - Null checks: symbol={null_check.null_symbol}, variation={null_check.null_variation}")

assert silver_count == gold_count, "Row count mismatch between Silver and Gold"
assert invalid_ohlc == 0, "OHLC validation failed"
assert invalid_volume == 0, "Volume validation failed"

# COMMAND ----------

# DBTITLE 1,Exercise 5: Write Gold - IMPLEMENTATION
df_gold = spark.table("gold_hourly_metrics")

partitions = df_gold.select("rate_date").distinct().collect()
partition_dates = sorted([row.rate_date for row in partitions])
logging.info(f"Writing {len(partition_dates)} partition(s): {partition_dates}")

written_fqn = gold_poloniex.write_table(
    df=df_gold,
    table_name=TABLE_NAME,
    mode="overwrite",
    partition_cols=["rate_date"],
    dynamic_partition_overwrite=True,
)

logging.info(f"Gold table written: {written_fqn}")

# COMMAND ----------

# DBTITLE 1,Exercise 6: Final Validation - IMPLEMENTATION
display(spark.sql("""
    SELECT 
        symbol,
        COUNT(*) as hourly_candles,
        MIN(rate_date) as first_date,
        MAX(rate_date) as last_date,
        ROUND(AVG(variation_1h_pct), 2) as avg_variation_pct
    FROM uc_sa_br_dev.gold_finance_investments_ohlcv_metrics.hourly
    GROUP BY symbol
    ORDER BY symbol
"""))

logging.info("Gold layer processing completed")
