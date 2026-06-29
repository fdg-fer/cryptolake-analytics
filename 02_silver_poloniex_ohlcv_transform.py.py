# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Documentação Silver Layer
# MAGIC %md
# MAGIC # 02 - Silver Layer: Poloniex OHLCV Transform
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Transformar dados brutos da Bronze em dados limpos, tipados e validados para analytics.
# MAGIC
# MAGIC ## Princípios Silver Layer
# MAGIC
# MAGIC **Transformações Permitidas:**
# MAGIC * Type casting (string → DECIMAL, long → TIMESTAMP)
# MAGIC * Conversões de unidade (millis → timestamp)
# MAGIC * Adição de campos derivados simples (exchange identifier)
# MAGIC * Validações de qualidade de dados
# MAGIC * Limpeza e normalização
# MAGIC
# MAGIC **Particionamento:**
# MAGIC * Manter `rate_date` para consistência com Bronze
# MAGIC * Partition pruning para queries eficientes
# MAGIC * Dynamic partition overwrite para reprocessamento
# MAGIC
# MAGIC ## Transformações a Implementar
# MAGIC
# MAGIC ### 1. Adicionar Identificador de Exchange
# MAGIC ```sql
# MAGIC 'poloniex' AS exchange
# MAGIC ```
# MAGIC *Justificativa:* Bronze não armazena (identificação implícita no schema). Silver adiciona explicitamente para analytics multi-exchange na Gold.
# MAGIC
# MAGIC ### 2. Type Casting de OHLCV
# MAGIC ```sql
# MAGIC CAST(open AS DECIMAL(18,8)) AS open
# MAGIC CAST(high AS DECIMAL(18,8)) AS high
# MAGIC CAST(low AS DECIMAL(18,8)) AS low
# MAGIC CAST(close AS DECIMAL(18,8)) AS close
# MAGIC CAST(volume AS DECIMAL(18,8)) AS volume
# MAGIC ```
# MAGIC *Justificativa:* Bronze armazena como string (tipo original da API). Silver converte para DECIMAL para cálculos numéricos precisos.
# MAGIC
# MAGIC ### 3. Conversão de Timestamps
# MAGIC ```sql
# MAGIC FROM_UNIXTIME(start_time_ms / 1000) AS open_time
# MAGIC FROM_UNIXTIME(close_time_ms / 1000) AS close_time
# MAGIC ```
# MAGIC *Justificativa:* Bronze armazena timestamps em millis (formato API). Silver converte para TIMESTAMP para queries temporais.
# MAGIC
# MAGIC ### 4. Validações de Qualidade
# MAGIC * Verificar nulls em campos críticos
# MAGIC * Validar relações OHLC: `high >= low`, `high >= open`, `high >= close`, `low <= open`, `low <= close`
# MAGIC * Validar volume positivo: `volume > 0`
# MAGIC * Validar timestamps válidos
# MAGIC
# MAGIC ## Schema Esperado da Silver
# MAGIC
# MAGIC | Campo | Tipo | Origem | Transformação |
# MAGIC | --- | --- | --- | --- |
# MAGIC | symbol | STRING | Bronze (API) | Nenhuma |
# MAGIC | exchange | STRING | **Adicionado** | `'poloniex'` |
# MAGIC | open | DECIMAL(18,8) | Bronze (API) | CAST de string |
# MAGIC | high | DECIMAL(18,8) | Bronze (API) | CAST de string |
# MAGIC | low | DECIMAL(18,8) | Bronze (API) | CAST de string |
# MAGIC | close | DECIMAL(18,8) | Bronze (API) | CAST de string |
# MAGIC | volume | DECIMAL(18,8) | Bronze (API) | CAST de string |
# MAGIC | open_time | TIMESTAMP | Bronze (API) | FROM_UNIXTIME(start_time_ms/1000) |
# MAGIC | close_time | TIMESTAMP | Bronze (API) | FROM_UNIXTIME(close_time_ms/1000) |
# MAGIC | interval | STRING | Bronze (API) | Nenhuma |
# MAGIC | trade_count | LONG | Bronze (API) | Nenhuma |
# MAGIC | rate_date | DATE | Bronze | Preservado para particionamento |
# MAGIC | ingested_at | TIMESTAMP | Bronze | Preservado para auditoria |
# MAGIC
# MAGIC ## Estratégia de Escrita
# MAGIC
# MAGIC * **Modo:** `overwrite` com `dynamic_partition_overwrite=True`
# MAGIC * **Particionamento:** `rate_date`
# MAGIC * **Cadência:** D-1 incremental (mesma da Bronze)
# MAGIC * **Destino:** `uc_sa_br_dev.silver_poloniex_ohlcv.hourly`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exercícios Práticos
# MAGIC
# MAGIC As células abaixo contêm instruções para você implementar a transformação Silver passo a passo.

# COMMAND ----------

# DBTITLE 1,Exercício 1: Setup Zordon - INSTRUÇÃO
# MAGIC %md
# MAGIC ## Exercício 1: Setup Zordon Client
# MAGIC
# MAGIC ### Objetivo
# MAGIC Criar o cliente Silver usando zordon para governança de naming e localização.
# MAGIC
# MAGIC ### Instruções
# MAGIC
# MAGIC 1. Importar o módulo zordon
# MAGIC 2. Adicionar zordon ao sys.path (mesmo caminho da Bronze)
# MAGIC 3. Criar `zordon.Project()` com os parâmetros:
# MAGIC    * country: `"br"`
# MAGIC    * region: `"sa"`
# MAGIC    * environment: `"dev"`
# MAGIC 4. Criar client Silver usando `proj.client()`:
# MAGIC    * layer: `"silver"`
# MAGIC    * domain: `"poloniex"`
# MAGIC    * subdomain: `"ohlcv"`
# MAGIC 5. Definir constante `TABLE_NAME = "hourly"`
# MAGIC 6. Usar logging para mostrar o FQN da tabela destino
# MAGIC
# MAGIC ### Dicas
# MAGIC * Estrutura similar à Bronze, mas layer="silver"
# MAGIC * O schema será `silver_poloniex_ohlcv` automaticamente
# MAGIC * Tabela final: `uc_sa_br_dev.silver_poloniex_ohlcv.hourly`
# MAGIC
# MAGIC ### Implemente na célula abaixo:

# COMMAND ----------

# DBTITLE 1,Exercício 1: Setup Zordon - IMPLEMENTAÇÃO
# TODO: Implemente o setup do Zordon client aqui

# Imports necessários 
import logging
import sys
from datetime import datetime, timedelta, timezone

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Setup Zordon
ZORDON_SRC_PATH = "/Workspace/Repos/CryptoLake/zordon-data-utils/src"
if ZORDON_SRC_PATH not in sys.path:
    sys.path.append(ZORDON_SRC_PATH)

import zordon
from pyspark.sql import functions as F

# Constantes
TABLE_NAME = "hourly"

# 1. Criar Project.
proj = zordon.Project(
    spark=spark,
    country="br",
    region="sa",
    environment="dev",
)

# 2. Criar client Silver
silver_poloniex = proj.client(
    layer="silver",
    domain="market",
    subdomain="ohlcv",
)

# Logging do FQN
target_fqn = silver_poloniex.governance.fqn(TABLE_NAME)
print(f"Target FQN: {target_fqn}")
logging.info(f"Target FQN: {target_fqn}")


# COMMAND ----------

# DBTITLE 1,Exercício 2: Ler Bronze - INSTRUÇÃO
# MAGIC %md
# MAGIC ## Exercício 2: Ler Dados da Bronze
# MAGIC
# MAGIC ### Objetivo
# MAGIC Carregar dados da Bronze usando SQL para facilitar transformações declarativas.
# MAGIC
# MAGIC ### Instruções
# MAGIC
# MAGIC 1. Use `spark.sql()` para ler da tabela Bronze
# MAGIC 2. Tabela: `uc_sa_br_dev.bronze_poloniex_ohlcv.hourly`
# MAGIC 3. Opcional: adicionar filtro `WHERE rate_date = '2026-06-26'` para testar com subset
# MAGIC 4. Criar uma view temporária para facilitar próximas queries: `CREATE OR REPLACE TEMP VIEW bronze_raw AS ...`
# MAGIC 5. Fazer `display()` para visualizar amostra dos dados
# MAGIC
# MAGIC ### Por que SQL ao invés de DataFrame API?
# MAGIC * Transformações de tipo mais intuitivas (CAST, FROM_UNIXTIME)
# MAGIC * Lógica declarativa clara e legível
# MAGIC * Fácil de testar queries isoladamente
# MAGIC * Compatível com ferramentas SQL tradicionais
# MAGIC
# MAGIC ### Dicas
# MAGIC * Você pode usar `SELECT *` para começar
# MAGIC * A view temporária será usada nas próximas transformações
# MAGIC * Use LIMIT 10 para visualização inicial
# MAGIC
# MAGIC ### Implemente na célula abaixo:

# COMMAND ----------

# DBTITLE 1,Exercício 2: Ler Bronze - IMPLEMENTAÇÃO

# TODO: Implemente a leitura da Bronze aqui

# 1.Criando view temporaria bronze_raw
spark.sql("""
  CREATE OR REPLACE TEMPORARY VIEW bronze_raw AS 
  SELECT * FROM uc_sa_br_dev.bronze_poloniex_ohlcv.hourly
""")

display(spark.sql("SELECT * FROM bronze_raw LIMIT 10"))

# COMMAND ----------

# 2.Validar dados (em outra célula)
spark.sql("""
  SELECT * FROM bronze_raw WHERE rate_date =
 '2026-06-26' LIMIT 10
""")

# COMMAND ----------

# DBTITLE 1,Exercício 3: Transformações Silver - INSTRUÇÃO
# MAGIC %md
# MAGIC ## Exercício 3: Aplicar Transformações Silver
# MAGIC
# MAGIC ### Objetivo
# MAGIC Converter tipos de dados e adicionar campos necessários para analytics.
# MAGIC
# MAGIC ### Instruções
# MAGIC
# MAGIC Crie uma query SQL que:
# MAGIC
# MAGIC 1. **Adicione campo exchange:**
# MAGIC    ```sql
# MAGIC    'poloniex' AS exchange
# MAGIC    ```
# MAGIC
# MAGIC 2. **Converta OHLCV para DECIMAL(18,8):**
# MAGIC    ```sql
# MAGIC    CAST(open AS DECIMAL(18,8)) AS open,
# MAGIC    CAST(high AS DECIMAL(18,8)) AS high,
# MAGIC    CAST(low AS DECIMAL(18,8)) AS low,
# MAGIC    CAST(close AS DECIMAL(18,8)) AS close,
# MAGIC    CAST(volume AS DECIMAL(18,8)) AS volume
# MAGIC    ```
# MAGIC
# MAGIC 3. **Converta timestamps para TIMESTAMP:**
# MAGIC    ```sql
# MAGIC    FROM_UNIXTIME(start_time_ms / 1000) AS open_time,
# MAGIC    FROM_UNIXTIME(close_time_ms / 1000) AS close_time
# MAGIC    ```
# MAGIC
# MAGIC 4. **Preserve campos necessários:**
# MAGIC    * symbol (sem mudança)
# MAGIC    * interval (sem mudança)
# MAGIC    * trade_count (sem mudança)
# MAGIC    * rate_date (particionamento)
# MAGIC    * ingested_at (auditoria)
# MAGIC
# MAGIC 5. **Crie uma view temporária:**
# MAGIC    ```sql
# MAGIC    CREATE OR REPLACE TEMP VIEW silver_transformed AS
# MAGIC    SELECT ...
# MAGIC    ```
# MAGIC
# MAGIC 6. **Valide a transformação:**
# MAGIC    * `SELECT * FROM silver_transformed LIMIT 10`
# MAGIC    * Verificar tipos com `DESCRIBE silver_transformed`
# MAGIC
# MAGIC ### Dicas
# MAGIC * Use a view `bronze_raw` criada no exercício anterior
# MAGIC * Ordem dos campos não importa, mas organize logicamente
# MAGIC * Timestamps em millis precisam ser divididos por 1000
# MAGIC
# MAGIC ### Implemente na célula abaixo:

# COMMAND ----------

# DBTITLE 1,Exercício 3: Transformações Silver - IMPLEMENTAÇÃO
# TODO: Implemente as transformações Silver aqui

spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW silver_transformed AS
        SELECT

        -- Campo adicionado
        "poloniex" AS exchange,
        
        -- Campos mantidos
        symbol,
        interval,
        trade_count,

        -- OHLCV transformados
        CAST(open AS DECIMAL(18,8)) AS open,
        CAST(high AS DECIMAL(18,8)) AS high,
        CAST(low AS DECIMAL(18,8)) AS low,
        CAST(close AS DECIMAL(18,8)) AS close,
        CAST(volume AS DECIMAL(18,8)) AS volume,

        -- Timestamps transformados (com CAST para TIMESTAMP)
        CAST(FROM_UNIXTIME(close_time_ms / 1000) AS TIMESTAMP) close_time,
        CAST(FROM_UNIXTIME(start_time_ms / 1000) AS TIMESTAMP) open_time,

        -- Particionamento e auditoria preservados
        rate_date,
        ingested_at

        FROM bronze_raw
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_transformed LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe silver_transformed

# COMMAND ----------

# DBTITLE 1,Exercício 4: Validações de Qualidade - INSTRUÇÃO
# MAGIC %md
# MAGIC ## Exercício 4: Validações de Qualidade de Dados
# MAGIC
# MAGIC ### Objetivo
# MAGIC Garantir integridade dos dados antes de escrever na Silver.
# MAGIC
# MAGIC ### Instruções
# MAGIC
# MAGIC Crie queries SQL para validar:
# MAGIC
# MAGIC #### 1. Verificar Nulls em Campos Críticos
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   SUM(CASE WHEN symbol IS NULL THEN 1 ELSE 0 END) AS null_symbol,
# MAGIC   SUM(CASE WHEN open IS NULL THEN 1 ELSE 0 END) AS null_open,
# MAGIC   SUM(CASE WHEN high IS NULL THEN 1 ELSE 0 END) AS null_high,
# MAGIC   SUM(CASE WHEN low IS NULL THEN 1 ELSE 0 END) AS null_low,
# MAGIC   SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END) AS null_close,
# MAGIC   SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) AS null_volume
# MAGIC FROM silver_transformed
# MAGIC ```
# MAGIC
# MAGIC #### 2. Validar Relações OHLC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   COUNT(*) AS invalid_ohlc_count
# MAGIC FROM silver_transformed
# MAGIC WHERE 
# MAGIC   high < low OR
# MAGIC   high < open OR
# MAGIC   high < close OR
# MAGIC   low > open OR
# MAGIC   low > close
# MAGIC ```
# MAGIC
# MAGIC #### 3. Validar Volume Positivo
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   COUNT(*) AS invalid_volume_count
# MAGIC FROM silver_transformed
# MAGIC WHERE volume <= 0
# MAGIC ```
# MAGIC
# MAGIC #### 4. Validar Timestamps
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   COUNT(*) AS invalid_timestamp_count
# MAGIC FROM silver_transformed
# MAGIC WHERE 
# MAGIC   open_time IS NULL OR
# MAGIC   close_time IS NULL OR
# MAGIC   close_time <= open_time
# MAGIC ```
# MAGIC
# MAGIC ### Resultado Esperado
# MAGIC * Todas as contagens de erros devem ser 0
# MAGIC * Se encontrar erros, investigate os registros problemáticos
# MAGIC * Decida se deve filtrar ou corrigir
# MAGIC
# MAGIC ### Implemente na célula abaixo:

# COMMAND ----------

# DBTITLE 1,Exercício 4: Validações de Qualidade - IMPLEMENTAÇÃO
# MAGIC %sql
# MAGIC -- TODO: Implemente as validações de qualidade aqui
# MAGIC
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   SUM(CASE WHEN symbol IS NULL THEN 1 ELSE 0 END) AS null_symbol,
# MAGIC   SUM(CASE WHEN open IS NULL THEN 1 ELSE 0 END) AS null_open,
# MAGIC   SUM(CASE WHEN high IS NULL THEN 1 ELSE 0 END) AS null_high,
# MAGIC   SUM(CASE WHEN low IS NULL THEN 1 ELSE 0 END) AS null_low,
# MAGIC   SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END) AS null_close,
# MAGIC   SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) AS null_volume
# MAGIC FROM silver_transformed

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   COUNT(*) AS invalid_ohlc_count
# MAGIC FROM silver_transformed
# MAGIC WHERE 
# MAGIC   high < low OR
# MAGIC   high < open OR
# MAGIC   high < close OR
# MAGIC   low > open OR
# MAGIC   low > close

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   COUNT(*) AS invalid_volume_count
# MAGIC FROM silver_transformed
# MAGIC WHERE volume <= 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   COUNT(*) AS invalid_timestamp_count
# MAGIC FROM silver_transformed
# MAGIC WHERE 
# MAGIC   open_time IS NULL OR
# MAGIC   close_time IS NULL OR
# MAGIC   close_time <= open_time

# COMMAND ----------

# DBTITLE 1,Exercício 5: Escrever Silver com Zordon - INSTRUÇÃO
# MAGIC %md
# MAGIC ## Exercício 5: Escrever Tabela Silver
# MAGIC
# MAGIC ### Objetivo
# MAGIC Persistir dados transformados na Silver usando Zordon com estratégia D-1 incremental.
# MAGIC
# MAGIC ### Instruções
# MAGIC
# MAGIC 1. **Converter view para DataFrame:**
# MAGIC    ```python
# MAGIC    df_silver = spark.sql("SELECT * FROM silver_transformed")
# MAGIC    ```
# MAGIC
# MAGIC 2. **Verificar partições no DataFrame:**
# MAGIC    ```python
# MAGIC    partitions = df_silver.select("rate_date").distinct().collect()
# MAGIC    logging.info(f"Writing {len(partitions)} partition(s): {[row.rate_date for row in partitions]}")
# MAGIC    ```
# MAGIC
# MAGIC 3. **Escrever com Zordon:**
# MAGIC    ```python
# MAGIC    written_fqn = silver_client.write_table(
# MAGIC        df=df_silver,
# MAGIC        table_name=TABLE_NAME,
# MAGIC        mode="overwrite",
# MAGIC        partition_cols=["rate_date"],
# MAGIC        dynamic_partition_overwrite=True,
# MAGIC    )
# MAGIC    ```
# MAGIC
# MAGIC 4. **Confirmar escrita:**
# MAGIC    ```python
# MAGIC    logging.info(f"Silver table written: {written_fqn}")
# MAGIC    ```
# MAGIC
# MAGIC ### Por que Dynamic Partition Overwrite?
# MAGIC * Idempotência: pode rodar múltiplas vezes sem duplicar
# MAGIC * Backfill: reprocessar dias específicos sem afetar histórico
# MAGIC * D-1 Incremental: adiciona apenas nova partição diariamente
# MAGIC
# MAGIC ### Dicas
# MAGIC * Certifique-se que `silver_client` foi criado no Exercício 1
# MAGIC * Use logging ao invés de print
# MAGIC * A variável `TABLE_NAME` deve ser "hourly"
# MAGIC
# MAGIC ### Implemente na célula abaixo:

# COMMAND ----------

# DBTITLE 1,Exercício 5: Escrever Silver - IMPLEMENTAÇÃO
# TODO: Implemente a escrita da Silver aqui

df_silver = spark.sql("""SELECT * FROM silver_transformed""")

partitions = df_silver.select("rate_date").distinct().collect()

logging.info(f"Writing {len(partitions)} partition(s): {[row.rate_date for row in partitions]}")
print(f"Writing {len(partitions)} partition(s): {[row.rate_date for row in partitions]}")

# COMMAND ----------

# DBTITLE 1,Cell 18
written_fqn = silver_poloniex.write_table(
    df=df_silver,
    table_name=TABLE_NAME,
    mode="overwrite",
    partition_cols=["rate_date"],
    dynamic_partition_overwrite=True,
)
logging.info(f"Silver table written: {written_fqn}")
print(f"Silver table written: {written_fqn}")

# COMMAND ----------

# DBTITLE 1,Exercício 6: Validação Final - INSTRUÇÃO
# MAGIC %md
# MAGIC ## Exercício 6: Validação Final
# MAGIC
# MAGIC ### Objetivo
# MAGIC Verificar que a Silver foi criada corretamente e comparar com Bronze.
# MAGIC
# MAGIC ### Instruções
# MAGIC
# MAGIC #### 1. Verificar Tabela Criada
# MAGIC ```sql
# MAGIC SHOW TABLES IN uc_sa_br_dev.silver_poloniex_ohlcv
# MAGIC ```
# MAGIC
# MAGIC #### 2. Verificar Schema
# MAGIC ```sql
# MAGIC DESCRIBE uc_sa_br_dev.silver_poloniex_ohlcv.hourly
# MAGIC ```
# MAGIC
# MAGIC #### 3. Contagem por Símbolo e Data
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   symbol,
# MAGIC   exchange,
# MAGIC   COUNT(*) AS rows,
# MAGIC   MIN(rate_date) AS min_date,
# MAGIC   MAX(rate_date) AS max_date,
# MAGIC   MIN(open_time) AS first_candle,
# MAGIC   MAX(close_time) AS last_candle
# MAGIC FROM uc_sa_br_dev.silver_poloniex_ohlcv.hourly
# MAGIC GROUP BY symbol, exchange
# MAGIC ORDER BY symbol
# MAGIC ```
# MAGIC
# MAGIC #### 4. Comparar Bronze vs Silver
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   'Bronze' AS layer,
# MAGIC   COUNT(*) AS total_rows
# MAGIC FROM uc_sa_br_dev.bronze_poloniex_ohlcv.hourly
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Silver' AS layer,
# MAGIC   COUNT(*) AS total_rows
# MAGIC FROM uc_sa_br_dev.silver_poloniex_ohlcv.hourly
# MAGIC ```
# MAGIC
# MAGIC #### 5. Amostra de Dados Transformados
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   symbol,
# MAGIC   exchange,
# MAGIC   open,
# MAGIC   high,
# MAGIC   low,
# MAGIC   close,
# MAGIC   volume,
# MAGIC   open_time,
# MAGIC   close_time,
# MAGIC   rate_date
# MAGIC FROM uc_sa_br_dev.silver_poloniex_ohlcv.hourly
# MAGIC ORDER BY symbol, open_time
# MAGIC LIMIT 10
# MAGIC ```
# MAGIC
# MAGIC ### Resultado Esperado
# MAGIC * **Contagens:** Bronze = Silver (nenhum registro perdido)
# MAGIC * **Tipos:** DECIMAL(18,8) para OHLCV, TIMESTAMP para open_time/close_time
# MAGIC * **Exchange:** Todos os registros com 'poloniex'
# MAGIC * **Particionamento:** Organizado por rate_date
# MAGIC
# MAGIC ### Implemente na célula abaixo:

# COMMAND ----------

# DBTITLE 1,Exercício 6: Validação Final - IMPLEMENTAÇÃO
# MAGIC %sql
# MAGIC -- TODO: Implemente as validações finais aqui
# MAGIC
# MAGIC DESCRIBE uc_sa_br_dev.silver_market_ohlcv.hourly

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN uc_sa_br_dev.silver_market_ohlcv

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     symbol,
# MAGIC     exchange,
# MAGIC     COUNT(*) AS rows,
# MAGIC     MIN(rate_date) AS min_date,
# MAGIC     MAX(rate_date) AS max_date,
# MAGIC     MIN(open_time) AS first_candle,
# MAGIC     MAX(close_time) AS last_candle
# MAGIC FROM uc_sa_br_dev.silver_market_ohlcv.hourly
# MAGIC GROUP BY symbol, exchange
# MAGIC ORDER BY symbol

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     'Bronze' AS layer,
# MAGIC     COUNT(*) AS total_rows
# MAGIC FROM uc_sa_br_dev.bronze_poloniex_ohlcv.hourly
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Silver' AS layer,
# MAGIC     COUNT(*) AS total_rows
# MAGIC FROM uc_sa_br_dev.silver_market_ohlcv.hourly

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     symbol,
# MAGIC     exchange,
# MAGIC     open,
# MAGIC     high,
# MAGIC     low,
# MAGIC     close,
# MAGIC     volume,
# MAGIC     open_time,
# MAGIC     close_time,
# MAGIC     rate_date
# MAGIC FROM uc_sa_br_dev.silver_market_ohlcv.hourly
# MAGIC ORDER BY symbol, open_time
# MAGIC LIMIT 10
