# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Documentação - dim_symbol
# MAGIC %md
# MAGIC # Gold Layer: Dimensão Symbol
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Criar dimensão de pares de trading (symbols) para o star schema de análise de mercado.
# MAGIC
# MAGIC ## Estratégia
# MAGIC
# MAGIC * **Fonte:** `uc_sa_br_dev.silver_market_ohlcv.hourly`
# MAGIC * **Destino:** `uc_sa_br_dev.gold_finance_investments_market_analysis.dim_symbol`
# MAGIC * **Granularidade:** 1 registro por par de trading
# MAGIC * **Tipo:** SCD Type 1 (dimensão estática)
# MAGIC
# MAGIC ## Schema
# MAGIC
# MAGIC | Coluna | Tipo | Descrição |
# MAGIC |--------|------|------------|
# MAGIC | symbol_id | INT | Surrogate key (ROW_NUMBER) |
# MAGIC | symbol_name | STRING | Par completo normalizado (BTC_USDT) |
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

TABLE_NAME = "dim_symbol"

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
    CREATE OR REPLACE TEMPORARY VIEW dim_symbol_raw AS
    SELECT DISTINCT
        symbol AS symbol_name
    FROM uc_sa_br_dev.silver_market_ohlcv.hourly
    ORDER BY symbol_name
""")

spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW dim_symbol_final AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY symbol_name) AS symbol_id,
        symbol_name
    FROM dim_symbol_raw
""")

logging.info("Dimension dim_symbol created")

# COMMAND ----------

# DBTITLE 1,Write Gold Table
df_dim = spark.table("dim_symbol_final")

total_rows = df_dim.count()
logging.info(f"Writing {total_rows} symbol(s)")

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
        symbol_id,
        symbol_name
    FROM {target_fqn}
    ORDER BY symbol_name
""")

display(validation)
logging.info("dim_symbol validation completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `uc_sa_br_dev`; select * from `gold_finance_investments_market_analysis`.`dim_symbol` limit 100;
