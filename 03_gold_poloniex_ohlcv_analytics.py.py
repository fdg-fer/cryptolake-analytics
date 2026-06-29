# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Gold Layer: Analytics & Business Metrics
# MAGIC %md
# MAGIC # Camada Gold Fase 1: Métricas de Trading Horário
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
# MAGIC ## Exercícios do Workshop
# MAGIC
# MAGIC Você vai implementar:
# MAGIC
# MAGIC 1. **Configurar Zordon** cliente para camada Gold (hourly)
# MAGIC 2. **Ler dados da Silver** e criar view temporária
# MAGIC 3. **Calcular variação horária** usando SQL (sem GROUP BY)
# MAGIC 4. **Validar qualidade** (nulos, relações OHLC)
# MAGIC 5. **Escrever na tabela Gold** com Zordon
# MAGIC 6. **Validação final** entre camadas (Silver = Gold em contagem)

# COMMAND ----------

# DBTITLE 1,Exercise 1: Setup Zordon Client for Gold Layer
# MAGIC %md
# MAGIC ## Exercício 1: Configurar Cliente Zordon para Camada Gold
# MAGIC
# MAGIC ### Objetivos
# MAGIC
# MAGIC 1. Importar bibliotecas necessárias (logging, sys, datetime, zordon, pyspark.sql.functions)
# MAGIC 2. Configurar logging estruturado (level=INFO)
# MAGIC 3. Adicionar Zordon ao sys.path
# MAGIC 4. Criar `zordon.Project` com governança UC (country=br, region=sa, environment=dev)
# MAGIC 5. Criar cliente Gold: `domain="finance"`, `subdomain="investments"`, `data_product="ohlcv_metrics"`, `layer="gold"`
# MAGIC 6. Definir `TABLE_NAME = "hourly"`
# MAGIC 7. Logar o FQN destino (Fully Qualified Name)
# MAGIC
# MAGIC ### Saída Esperada
# MAGIC
# MAGIC ```
# MAGIC Target FQN: uc_sa_br_dev.gold_finance_investments_ohlcv_metrics.hourly
# MAGIC ```
# MAGIC
# MAGIC ### Notas de Implementação
# MAGIC
# MAGIC * Usar o mesmo caminho Zordon da Bronze/Silver: `/Workspace/Repos/CryptoLake/zordon-data-utils/src`
# MAGIC * Seguir padrões de nomenclatura CryptoLake: `gold_poloniex` ou `gold_client` para a variável cliente Zordon
# MAGIC * Usar `print()` (feedback visual) e `logging.info()` (logs de jobs)

# COMMAND ----------

# DBTITLE 1,Exercise 1: Setup Zordon - IMPLEMENTATION
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

# 2. Criar client Gold
gold_poloniex = proj.client(
    layer="gold",
    domain="finance",
    subdomain="investments",
    data_product="ohlcv_metrics",
)

# Logging do FQN
target_fqn = gold_poloniex.governance.fqn(TABLE_NAME)
print(f"Target FQN: {target_fqn}")
logging.info(f"Target FQN: {target_fqn}")



# COMMAND ----------

# DBTITLE 1,Exercise 2: Read Silver Data
# MAGIC %md
# MAGIC ## Exercício 2: Ler Dados da Silver
# MAGIC
# MAGIC ### Objetivos
# MAGIC
# MAGIC 1. Criar view temporária `silver_hourly` da tabela Silver
# MAGIC 2. Verificar disponibilidade dos dados
# MAGIC 3. Inspecionar schema e registros de amostra
# MAGIC
# MAGIC ### Tabela Origem Silver
# MAGIC
# MAGIC ```
# MAGIC uc_sa_br_dev.silver_market_ohlcv.hourly
# MAGIC ```
# MAGIC
# MAGIC ### Schema Esperado (Silver)
# MAGIC
# MAGIC * symbol, exchange, interval, trade_count
# MAGIC * open, high, low, close, volume (todos DECIMAL(18,8))
# MAGIC * open_time, close_time (TIMESTAMP)
# MAGIC * rate_date (DATE), ingested_at (TIMESTAMP)
# MAGIC
# MAGIC ### Padrão SQL
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW silver_hourly AS
# MAGIC SELECT * FROM uc_sa_br_dev.silver_market_ohlcv.hourly;
# MAGIC ```
# MAGIC
# MAGIC ### Query de Validação
# MAGIC
# MAGIC ```sql
# MAGIC SELECT symbol, rate_date, COUNT(*) as hourly_candles
# MAGIC FROM silver_hourly
# MAGIC GROUP BY symbol, rate_date
# MAGIC ORDER BY symbol, rate_date;
# MAGIC ```
# MAGIC
# MAGIC Espera-se: 24 velas horárias por símbolo por data

# COMMAND ----------

# DBTITLE 1,Exercise 2: Read Silver - IMPLEMENTATION
# TODO: Implementar leitura de dados da Silver

# Criar view temporária da tabela Silver

# Validação: Checar contagem de velas por símbolo por data
# 1.Criando view temporaria bronze_raw
spark.sql("""
  CREATE OR REPLACE TEMPORARY VIEW silver_hourly AS
  SELECT * FROM uc_sa_br_dev.silver_market_ohlcv.hourly
""")

display(spark.sql("SELECT * FROM silver_hourly LIMIT 10"))

# COMMAND ----------

# DBTITLE 1,Exercise 3: Daily Aggregations & Business Metrics
# MAGIC %md
# MAGIC ## Exercício 3: Calcular Variação Horária
# MAGIC
# MAGIC ### Objetivos
# MAGIC
# MAGIC 1. Renomear campos OHLCV da Silver (open → open_price, etc.)
# MAGIC 2. Calcular variação horária percentual (SEM GROUP BY)
# MAGIC 3. Criar view temporária `gold_hourly_metrics` com OHLCV + variação
# MAGIC
# MAGIC ### Lógica de Cálculo
# MAGIC
# MAGIC #### Renomeação de Campos
# MAGIC * `open` → `open_price`
# MAGIC * `high` → `high_price`
# MAGIC * `low` → `low_price`
# MAGIC * `close` → `close_price`
# MAGIC
# MAGIC #### Métrica Calculada (1 linha = 1 vela)
# MAGIC
# MAGIC | Métrica | Fórmula SQL |
# MAGIC | --- | --- |
# MAGIC | **variation_1h_pct** | `((close - open) / open) * 100` |
# MAGIC
# MAGIC #### Metadados
# MAGIC * Preservar: `symbol`, `exchange`, `rate_date`, `open_time`, `close_time`, `interval`, `trade_count`
# MAGIC * Adicionar: `CURRENT_TIMESTAMP() AS ingested_at`
# MAGIC
# MAGIC ### Template SQL
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gold_hourly_metrics AS
# MAGIC SELECT 
# MAGIC     -- Identificadores e timestamps
# MAGIC     symbol,
# MAGIC     exchange,
# MAGIC     rate_date,
# MAGIC     open_time,
# MAGIC     close_time,
# MAGIC     interval,
# MAGIC     trade_count,
# MAGIC     
# MAGIC     -- OHLCV renomeados
# MAGIC     open AS open_price,
# MAGIC     high AS high_price,
# MAGIC     low AS low_price,
# MAGIC     close AS close_price,
# MAGIC     volume,
# MAGIC     
# MAGIC     -- Variação horária
# MAGIC     ((close - open) / open) * 100 AS variation_1h_pct,
# MAGIC     
# MAGIC     -- Metadata
# MAGIC     CURRENT_TIMESTAMP() AS ingested_at
# MAGIC     
# MAGIC FROM silver_hourly
# MAGIC ORDER BY symbol, open_time;
# MAGIC ```
# MAGIC
# MAGIC **Nota**: NÃO use `GROUP BY` - cada vela Silver vira 1 vela Gold com variação calculada.

# COMMAND ----------

# DBTITLE 1,Exercise 3: Aggregations - IMPLEMENTATION
# Criar view gold_hourly_metrics
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW gold_hourly_metrics AS
    SELECT 
        -- Identificadores e timestamps
        symbol,
        exchange,
        rate_date,
        open_time,
        close_time,
        interval,
        trade_count,
        
        -- OHLCV renomeados
        open AS open_price,
        high AS high_price,
        low AS low_price,
        close AS close_price,
        volume,
        
        -- Variação horária
        ((close - open) / open) * 100 AS variation_1h_pct,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS ingested_at
        
    FROM silver_hourly
    ORDER BY symbol, open_time
""")

print("✅ View gold_hourly_metrics criada com sucesso")
logging.info("View gold_hourly_metrics criada")

# Validar: Exibir registros de amostra
display(spark.sql("""
    SELECT 
        symbol,
        open_time,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        variation_1h_pct
    FROM gold_hourly_metrics
    LIMIT 10
"""))

# COMMAND ----------

# DBTITLE 1,Exercise 4: Quality Validations
# MAGIC %md
# MAGIC ## Exercício 4: Validações de Qualidade
# MAGIC
# MAGIC ### Objetivos
# MAGIC
# MAGIC Validar os dados Gold antes de persistir:
# MAGIC
# MAGIC 1. **Contagem de Registros**: Verificar Gold = Silver (sem agregação)
# MAGIC 2. **Relações OHLC**: Verificar `high_price >= low_price` e lógica OHLC
# MAGIC 3. **Validação de Volume**: Garantir `volume > 0`
# MAGIC 4. **Checagem de Nulos**: Verificar sem nulos em campos críticos
# MAGIC 5. **Variação Calculada**: Verificar que variation_1h_pct foi calculada
# MAGIC
# MAGIC ### Queries de Validação
# MAGIC
# MAGIC #### 1. Contagem: Silver vs Gold (devem ser iguais)
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     'Silver' as layer,
# MAGIC     COUNT(*) as record_count
# MAGIC FROM silver_hourly
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Gold' as layer,
# MAGIC     COUNT(*) as record_count
# MAGIC FROM gold_hourly_metrics;
# MAGIC ```
# MAGIC Espera-se: Mesma contagem (sem agregação)
# MAGIC
# MAGIC #### 2. Relações OHLC Inválidas
# MAGIC
# MAGIC ```sql
# MAGIC SELECT COUNT(*) as invalid_ohlc_count
# MAGIC FROM gold_hourly_metrics
# MAGIC WHERE high_price < low_price
# MAGIC    OR high_price < open_price
# MAGIC    OR high_price < close_price
# MAGIC    OR low_price > open_price
# MAGIC    OR low_price > close_price
# MAGIC    OR open_price < 0
# MAGIC    OR close_price < 0;
# MAGIC ```
# MAGIC Espera-se: 0 registros inválidos
# MAGIC
# MAGIC #### 3. Volume Inválido
# MAGIC
# MAGIC ```sql
# MAGIC SELECT COUNT(*) as invalid_volume_count
# MAGIC FROM gold_hourly_metrics
# MAGIC WHERE volume <= 0 OR volume IS NULL;
# MAGIC ```
# MAGIC Espera-se: 0 registros inválidos
# MAGIC
# MAGIC #### 4. Checagem de Nulos em Campos Críticos
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_rows,
# MAGIC     SUM(CASE WHEN symbol IS NULL THEN 1 ELSE 0 END) as null_symbol,
# MAGIC     SUM(CASE WHEN open_price IS NULL THEN 1 ELSE 0 END) as null_open,
# MAGIC     SUM(CASE WHEN close_price IS NULL THEN 1 ELSE 0 END) as null_close,
# MAGIC     SUM(CASE WHEN variation_1h_pct IS NULL THEN 1 ELSE 0 END) as null_variation
# MAGIC FROM gold_hourly_metrics;
# MAGIC ```
# MAGIC Espera-se: Todas as contagens de nulos = 0
# MAGIC
# MAGIC #### 5. Spot Check Variação (amostra)
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     symbol,
# MAGIC     open_time,
# MAGIC     open_price,
# MAGIC     close_price,
# MAGIC     variation_1h_pct,
# MAGIC     ((close_price - open_price) / open_price) * 100 as manual_calc
# MAGIC FROM gold_hourly_metrics
# MAGIC LIMIT 5;
# MAGIC ```
# MAGIC Espera-se: variation_1h_pct = manual_calc (validar fórmula)

# COMMAND ----------

# DBTITLE 1,Exercise 4: Validations - IMPLEMENTATION
# TODO: Implementar validações de qualidade

# 1. Checar contagem Silver vs Gold (devem ser iguais)

# 2. Validar relações OHLC

# 3. Validar volumes

# 4. Checar nulos em campos críticos

# 5. Spot check: validar cálculo de variation_1h_pct

# COMMAND ----------

# DBTITLE 1,Exercise 5: Write Gold Table with Zordon
# MAGIC %md
# MAGIC ## Exercício 5: Escrever Tabela Gold com Zordon
# MAGIC
# MAGIC ### Objetivos
# MAGIC
# MAGIC 1. Converter view temporária `gold_hourly_metrics` para DataFrame
# MAGIC 2. Verificar partições a serem escritas
# MAGIC 3. Usar `write_table()` do Zordon com dynamic partition overwrite
# MAGIC 4. Logar o FQN escrito
# MAGIC
# MAGIC ### Padrão de Implementação
# MAGIC
# MAGIC ```python
# MAGIC # Convert view to DataFrame
# MAGIC df_gold = spark.table("gold_hourly_metrics")
# MAGIC
# MAGIC # Verify partitions
# MAGIC partitions = df_gold.select("rate_date").distinct().collect()
# MAGIC partition_dates = sorted([row.rate_date for row in partitions])
# MAGIC print(f"Writing {len(partition_dates)} partition(s): {partition_dates}")
# MAGIC logging.info(f"Writing {len(partition_dates)} partition(s): {partition_dates}")
# MAGIC
# MAGIC # Write to Gold with Zordon
# MAGIC written_fqn = gold_client.write_table(
# MAGIC     df=df_gold,
# MAGIC     table_name=TABLE_NAME,
# MAGIC     mode="overwrite",
# MAGIC     partition_cols=["rate_date"],
# MAGIC     dynamic_partition_overwrite=True,
# MAGIC )
# MAGIC
# MAGIC print(f"Gold table written: {written_fqn}")
# MAGIC logging.info(f"Gold table written: {written_fqn}")
# MAGIC ```
# MAGIC
# MAGIC ### Comportamento Esperado
# MAGIC
# MAGIC * **dynamic_partition_overwrite=True**: Sobrescreve apenas partições presentes no DataFrame
# MAGIC * **Idempotente**: Re-executar com a mesma data sobrescreve apenas a partição daquela data
# MAGIC * **Preservação de Partições**: Partições de outras datas permanecem intactas
# MAGIC
# MAGIC ### Tabela Destino
# MAGIC
# MAGIC ```
# MAGIC uc_sa_br_dev.gold_finance_investments_ohlcv_metrics.hourly
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Exercise 5: Write Gold - IMPLEMENTATION
# TODO: Implementar escrita da tabela Gold com Zordon

# 1. Converter view para DataFrame

# 2. Verificar partições

# 3. Escrever com Zordon (dynamic_partition_overwrite=True)

# COMMAND ----------

# DBTITLE 1,Exercise 6: Final Validation
# MAGIC %md
# MAGIC ## Exercício 6: Validação Final
# MAGIC
# MAGIC ### Objetivos
# MAGIC
# MAGIC Validar a tabela Gold persistida:
# MAGIC
# MAGIC 1. **Existência da Tabela**: Verificar que a tabela foi criada no Unity Catalog
# MAGIC 2. **Verificação de Schema**: Checar todas as colunas e tipos
# MAGIC 3. **Contagens de Dados**: Comparar contagens de registros Silver vs Gold (devem ser iguais)
# MAGIC 4. **Inspeção de Amostra**: Revisar OHLCV + variação
# MAGIC 5. **Consistência Entre Camadas**: Verificar que variation_1h_pct foi calculada corretamente
# MAGIC
# MAGIC ### Queries de Validação
# MAGIC
# MAGIC #### 1. Schema da Tabela
# MAGIC ```sql
# MAGIC DESCRIBE uc_sa_br_dev.gold_finance_investments_ohlcv_metrics.hourly;
# MAGIC ```
# MAGIC
# MAGIC #### 2. Contagens de Registros por Símbolo
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     symbol,
# MAGIC     COUNT(*) as hourly_candles,
# MAGIC     MIN(rate_date) as first_date,
# MAGIC     MAX(rate_date) as last_date
# MAGIC FROM uc_sa_br_dev.gold_finance_investments_ohlcv_metrics.hourly
# MAGIC GROUP BY symbol
# MAGIC ORDER BY symbol;
# MAGIC ```
# MAGIC
# MAGIC #### 3. Validação de Contagem Silver vs Gold
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     'Silver (hourly)' as layer,
# MAGIC     COUNT(*) as record_count
# MAGIC FROM uc_sa_br_dev.silver_market_ohlcv.hourly
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Gold (hourly)' as layer,
# MAGIC     COUNT(*) as record_count
# MAGIC FROM uc_sa_br_dev.gold_finance_investments_ohlcv_metrics.hourly;
# MAGIC ```
# MAGIC
# MAGIC Espera-se: `Contagem Gold = Contagem Silver` (sem agregação, apenas adição de métricas)
# MAGIC
# MAGIC #### 4. Amostra de Registros Gold
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     symbol,
# MAGIC     rate_date,
# MAGIC     open_time,
# MAGIC     open_price,
# MAGIC     high_price,
# MAGIC     low_price,
# MAGIC     close_price,
# MAGIC     volume,
# MAGIC     variation_1h_pct
# MAGIC FROM uc_sa_br_dev.gold_finance_investments_ohlcv_metrics.hourly
# MAGIC ORDER BY symbol, open_time
# MAGIC LIMIT 10;
# MAGIC ```
# MAGIC
# MAGIC #### 5. Verificação Pontual Entre Camadas (Verificação Manual)
# MAGIC
# MAGIC Escolher uma vela específica, verificar que métricas Gold foram calculadas corretamente:
# MAGIC
# MAGIC ```sql
# MAGIC -- Comparar uma vela Silver vs Gold
# MAGIC SELECT 
# MAGIC     'Silver' as layer,
# MAGIC     symbol,
# MAGIC     open_time,
# MAGIC     open,
# MAGIC     high,
# MAGIC     low,
# MAGIC     close,
# MAGIC     volume,
# MAGIC     NULL as variation_1h_pct
# MAGIC FROM uc_sa_br_dev.silver_market_ohlcv.hourly
# MAGIC WHERE symbol = 'BTC_USDT' 
# MAGIC   AND open_time = '2026-06-26 12:00:00'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Gold' as layer,
# MAGIC     symbol,
# MAGIC     open_time,
# MAGIC     open_price as open,
# MAGIC     high_price as high,
# MAGIC     low_price as low,
# MAGIC     close_price as close,
# MAGIC     volume,
# MAGIC     variation_1h_pct
# MAGIC FROM uc_sa_br_dev.gold_finance_investments_ohlcv_metrics.hourly
# MAGIC WHERE symbol = 'BTC_USDT' 
# MAGIC   AND open_time = '2026-06-26 12:00:00';
# MAGIC ```
# MAGIC
# MAGIC Verificar: 
# MAGIC * OHLCV iguais entre Silver e Gold
# MAGIC * `variation_1h_pct = (close - open) / open * 100`

# COMMAND ----------

# DBTITLE 1,Exercise 6: Final Validation - IMPLEMENTATION
# TODO: Implementar validações finais

# 1. Verificar schema da tabela

# 2. Checar contagens de registros por símbolo

# 3. Comparar contagens Silver vs Gold

# 4. Amostrar registros Gold

# 5. Verificação pontual entre camadas (opcional: verificar agregação de um símbolo)
