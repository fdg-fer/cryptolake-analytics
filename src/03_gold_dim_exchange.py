# Databricks notebook source
# DBTITLE 1,Documentação - dim_exchange
# MAGIC %md
# MAGIC # Gold Layer: Dimensão Exchange
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Criar dimensão de exchanges (corretoras) para o star schema de análise de mercado.
# MAGIC
# MAGIC ## Estratégia
# MAGIC
# MAGIC * **Fonte:** `uc_sa_br_dev.silver_market_ohlcv.hourly`
# MAGIC * **Destino:** `uc_sa_br_dev.gold_finance_investments_market_analysis.dim_exchange`
# MAGIC * **Granularidade:** 1 registro por exchange
# MAGIC * **Tipo:** SCD Type 1 (dimensão estática)
# MAGIC
# MAGIC ## Schema
# MAGIC
# MAGIC | Coluna | Tipo | Descrição |
# MAGIC |--------|------|------------|
# MAGIC | exchange_id | INT | Surrogate key (ROW_NUMBER) |
# MAGIC | exchange_name | STRING | Nome da exchange (poloniex, binance) |
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

TABLE_NAME = "dim_exchange"

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

# DBTITLE 1,Read Silver and Create Dimension
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW dim_exchange_raw AS
    SELECT DISTINCT
        exchange AS exchange_name
    FROM uc_sa_br_dev.silver_market_ohlcv.hourly
    ORDER BY exchange_name
""")

spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW dim_exchange_final AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY exchange_name) AS exchange_id,
        exchange_name
    FROM dim_exchange_raw
""")

logging.info("Dimension dim_exchange created")

# COMMAND ----------

# DBTITLE 1,Write Gold Table
df_dim = spark.table("dim_exchange_final")

total_rows = df_dim.count()
logging.info(f"Writing {total_rows} exchange(s)")

written_fqn = gold_star.write_table(
    df=df_dim,
    table_name=TABLE_NAME,
    mode="overwrite",
)

logging.info(f"Gold dimension written: {written_fqn}")

# COMMAND ----------

# DBTITLE 1,Validation
validation = spark.sql(f"""
    SELECT
        COUNT(*) AS total_exchanges,
        COLLECT_LIST(exchange_name) AS exchanges
    FROM {target_fqn}
""")

display(validation)
logging.info("dim_exchange validation completed")

# COMMAND ----------


