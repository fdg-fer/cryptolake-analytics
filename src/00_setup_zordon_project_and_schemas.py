# Databricks notebook source
"""
00 - Project structure setup

Objetivo:
1. Criar o Project do zordon.
2. Validar os clients Bronze, Silver e Gold.
3. Confirmar que o catalog existe.
4. Criar os schemas esperados da Fase 1.
5. Imprimir os FQNs principais do projeto.

Este arquivo nao ingere dados.
Ele prepara/valida a estrutura antes da Bronze.

"""
import sys

ZORDON_SRC_PATH = "/Workspace/Repos/CryptoLake/zordon-data-utils/src"

if ZORDON_SRC_PATH not in sys.path:
    sys.path.append(ZORDON_SRC_PATH)

import zordon

print(zordon.__version__)

COUNTRY = "br"
REGION = "sa"
ENVIRONMENT = "dev"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS uc_sa_br_dev;

# COMMAND ----------

def ensure_schema(client):
    """Cria o schema do client, se ele ainda nao existir."""
    catalog = client.governance.catalog_name()
    schema = client.governance.schema_name()

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    print(f"Schema pronto: {catalog}.{schema}")

# COMMAND ----------

def print_table_plan(label, client, table_names):
    """Imprime os FQNs planejados para um client."""
    print(f"\n{label}")
    print("-" * len(label))

    for table_name in table_names:
        zordon.validate_name(table_name, label="table_name")
        print(client.governance.fqn(table_name))


# COMMAND ----------

# 1. Criar Project.
proj = zordon.Project(
    spark=spark,
    country=COUNTRY,
    region=REGION,
    environment=ENVIRONMENT,
)

# 2. Criar clients governados pelo zordon.
bronze_poloniex = proj.client(
    layer="bronze",
    domain="poloniex",
    subdomain="ohlcv",
)

# COMMAND ----------

# 3. Confirmar catalog esperado.
catalog_name = bronze_poloniex.governance.catalog_name()
print("Catalog esperado:", catalog_name)

display(spark.sql("SHOW CATALOGS"))

# COMMAND ----------

ensure_schema(bronze_poloniex)

# COMMAND ----------

# 5. Imprimir plano de tabelas da Fase 1.
print_table_plan(
    label="Bronze",
    client=bronze_poloniex,
    table_names=["hourly"]
)

# COMMAND ----------

# 6. Conferir schemas criados.
display(spark.sql(f"SHOW SCHEMAS IN {catalog_name}"))

print("Setup estrutural finalizado.")
