# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Documentação
# MAGIC %md
# MAGIC # Camada Bronze: Ingestão de Dados Binance OHLCV
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Ingerir candles horários de OHLCV da API pública da Binance US para BTC, ETH, SOL, ADA e LINK.
# MAGIC
# MAGIC ## Estratégia de Ingestão
# MAGIC
# MAGIC - **D-1 Incremental:** Coleta apenas o dia anterior (D-1) a cada execução
# MAGIC - **Cadência:** Agendado para rodar uma vez por dia
# MAGIC - **Idempotência:** Pode rodar múltiplas vezes no mesmo dia sem duplicar dados
# MAGIC - **Backfill:** Suporta reprocessamento de dias específicos alterando TARGET_DATE
# MAGIC - **Imutabilidade:** Candles OHLCV são imutáveis após fechamento do período
# MAGIC
# MAGIC **Destino:** `uc_sa_br_dev.bronze_binance_ohlcv.hourly`
# MAGIC
# MAGIC ## Princípios da Camada Bronze
# MAGIC
# MAGIC - Selecionar campos relevantes (descartar campos desnecessários da API)
# MAGIC - Manter tipos de dados originais (sem casting, sem conversões)
# MAGIC - Adicionar metadata para auditoria: `ingested_at`
# MAGIC - Adicionar `rate_date` para particionamento físico (otimização de storage)
# MAGIC - Adiar transformações de negócio para camada Silver
# MAGIC - Identificação da fonte implícita no nome do schema
# MAGIC
# MAGIC ## Campos Armazenados
# MAGIC
# MAGIC - **Campos da API:** symbol, low, high, open, close, volume, open_time_ms, close_time_ms, interval, trade_count (tipos originais)
# MAGIC - **Metadata de Auditoria:** ingested_at (timestamp do processo)
# MAGIC - **Partição Física:** rate_date (derivado de open_time_ms)
# MAGIC - **Fonte:** Implícita no nome do schema (bronze_binance_ohlcv)
# MAGIC
# MAGIC ## Benefícios da Estratégia D-1 Incremental
# MAGIC
# MAGIC - **Performance:** ~95% menos dados escritos por execução (120 vs 2500 registros)
# MAGIC - **Custo:** API calls menores, menos compute time, redução de transferência de dados
# MAGIC - **Idempotência:** Múltiplas execuções no mesmo dia não duplicam dados
# MAGIC - **Backfill Preciso:** Reprocessar dias específicos sem afetar histórico
# MAGIC - **Resiliência:** Proteção contra falhas com retry seguro
# MAGIC
# MAGIC ## Adição de Transformações
# MAGIC
# MAGIC Transformações de negócio (type casting, conversão de timestamp, validações de qualidade) são adiadas para camada Silver. O campo `exchange` será adicionado na Silver para analytics multi-exchange.

# COMMAND ----------

# DBTITLE 1,Imports
import json
import logging
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone, date

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

ZORDON_SRC_PATH = "/Workspace/Repos/CryptoLake/zordon-data-utils/src"
if ZORDON_SRC_PATH not in sys.path:
    sys.path.append(ZORDON_SRC_PATH)

import zordon
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Constantes e configurações
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "ADAUSDT", "LINKUSDT"]
INTERVAL = "1h"
TABLE_NAME = "hourly"
MAX_LIMIT = 500

TARGET_DATE = date.today() - timedelta(days=1)

logging.info(f"Configuration loaded - Target date: {TARGET_DATE}")

# COMMAND ----------

# DBTITLE 1,Funções de API Binance
def fetch_binance_candles(symbol, start_time_ms, end_time_ms, limit=MAX_LIMIT):
    """Fetch hourly candles from Binance US public API."""
    base_url = "https://api.binance.us/api/v3/klines"
    params = urllib.parse.urlencode({
        "symbol": symbol,
        "interval": INTERVAL,
        "startTime": start_time_ms,
        "endTime": end_time_ms,
        "limit": limit,
    })
    url = f"{base_url}?{params}"

    with urllib.request.urlopen(url, timeout=30) as response:
        if response.status != 200:
            raise RuntimeError(f"Binance US API error: HTTP {response.status}")
        payload = response.read().decode("utf-8")

    return json.loads(payload)


def parse_candle(symbol, row):
    """Extract relevant fields for CryptoLake project.

    Pragmatic Bronze approach:
    - Select only fields needed for OHLCV analysis
    - Keep original data types (no casting, no conversions)
    - Discard fields not used in the project (taker buy volumes, etc.)

    Binance US API response (11 fields):
    [0] open_time, [1] open, [2] high, [3] low, [4] close, [5] volume,
    [6] close_time, [7] quote_asset_volume, [8] number_of_trades,
    [9] taker_buy_base_asset_volume, [10] taker_buy_quote_asset_volume.
    """
    return {
        "symbol": symbol,
        "open": row[1],
        "high": row[2],
        "low": row[3],
        "close": row[4],
        "volume": row[5],
        "open_time_ms": row[0],
        "close_time_ms": row[6],
        "interval": INTERVAL,
        "trade_count": row[8],
    }

# COMMAND ----------

# DBTITLE 1,Orchestration function
def collect_hourly_candles(symbols, target_date):
    """Collect hourly candles for multiple symbols for a specific date.

    Args:
        symbols: List of symbol pairs (e.g., ["BTCUSDT", "ETHUSDT"])
        target_date: Date to collect data for (date object)

    Returns:
        List of raw candle records with metadata.

    Note:
        D-1 Incremental Strategy: Collects only 24 hourly candles per symbol.
        For backfill, pass a specific date. For normal operation, use D-1.
        Current limitation: MAX_LIMIT (500) is sufficient for 1-day collection.
    """
    # Calculate start and end timestamps for the target date (00:00 to 23:59:59 UTC)
    start_time_ms = int(
        datetime.combine(target_date, datetime.min.time(), timezone.utc).timestamp() * 1000
    )
    end_time_ms = int(
        datetime.combine(target_date, datetime.max.time(), timezone.utc).timestamp() * 1000
    )

    all_records = []

    for symbol in symbols:
        try:
            logging.info(f"Fetching {symbol} candles...")
            candles = fetch_binance_candles(
                symbol=symbol,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
                limit=MAX_LIMIT,
            )

            for candle_row in candles:
                record = parse_candle(symbol, candle_row)
                all_records.append(record)

            logging.info(f"Collected {len(candles)} candles for {symbol}")

        except Exception as e:
            logging.error(f"Failed to fetch {symbol}: {e}")
            raise

    return all_records

# COMMAND ----------

# DBTITLE 1,Criar client Bronze via zordon
proj = zordon.Project(
    spark=spark,
    country="br",
    region="sa",
    environment="dev",
)

bronze_binance = proj.client(
    layer="bronze",
    domain="binance",
    subdomain="ohlcv",
)

target_fqn = bronze_binance.governance.fqn(TABLE_NAME)
valid = zordon.is_valid_name(TABLE_NAME)

logging.info(f"Target table: {target_fqn}")
logging.info(f"Table name valid: {valid}")

# COMMAND ----------

# DBTITLE 1,Buscar dados da Binance
logging.info(f"Starting D-1 incremental collection for date: {TARGET_DATE}")
logging.info(f"Symbols: {SYMBOLS}")

records = collect_hourly_candles(SYMBOLS, TARGET_DATE)

logging.info(f"Total records collected: {len(records)} ({len(records) // len(SYMBOLS)} per symbol)")

if not records:
    raise RuntimeError("No records returned from Binance US API")

# COMMAND ----------

# DBTITLE 1,Criar DataFrame Spark
df = (
    spark.createDataFrame(records)
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn(
        "rate_date",
        F.to_date(F.from_unixtime(F.col("open_time_ms") / 1000))
    )
)

total_rows = df.count()
logging.info(f"DataFrame created with {total_rows} records")

# COMMAND ----------

# DBTITLE 1,Escrever Delta Bronze
partitions_in_df = df.select("rate_date").distinct().collect()
partition_dates = sorted([row.rate_date for row in partitions_in_df])
logging.info(f"Writing {len(partition_dates)} partition(s): {partition_dates}")

written_fqn = bronze_binance.write_table(
    df=df,
    table_name=TABLE_NAME,
    mode="overwrite",
    partition_cols=["rate_date"],
    dynamic_partition_overwrite=True,
)

logging.info(f"Bronze table written: {written_fqn}")

# COMMAND ----------

# DBTITLE 1,Ler e validar contagens
try:
    df_read = bronze_binance.read_table(TABLE_NAME)

    validation = (
        df_read
        .groupBy("symbol")
        .agg(
            F.count("*").alias("rows"),
            F.min("rate_date").alias("min_date"),
            F.max("rate_date").alias("max_date"),
        )
        .orderBy("symbol")
        .collect()
    )

    for row in validation:
        logging.info(f"Symbol: {row.symbol} | Rows: {row.rows} | Date range: {row.min_date} to {row.max_date}")

    logging.info("Bronze ingestion completed successfully")
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        logging.warning(f"Table {bronze_binance.governance.fqn(TABLE_NAME)} does not exist yet. Run cells 7-9 first to create the table.")
    else:
        raise

# COMMAND ----------

# DBTITLE 1,Exemplo: Backfill de dias específicos
# MAGIC %md
# MAGIC ## Backfill: Reprocessamento de Dias Específicos
# MAGIC
# MAGIC A estratégia D-1 incremental permite reprocessar dias específicos sem perder histórico.
# MAGIC
# MAGIC ### Caso 1: Reprocessar Um Dia Específico
# MAGIC ```python
# MAGIC # Na célula 3 (Constantes), alterar TARGET_DATE:
# MAGIC TARGET_DATE = date(2026, 6, 20)  # Reprocessar 2026-06-20
# MAGIC
# MAGIC # Executar células 7-10
# MAGIC # Resultado: apenas a partição 2026-06-20 será reescrita
# MAGIC ```
# MAGIC
# MAGIC ### Caso 2: Backfill de Múltiplos Dias
# MAGIC ```python
# MAGIC start_date = date(2026, 6, 15)
# MAGIC end_date = date(2026, 6, 20)
# MAGIC
# MAGIC current_date = start_date
# MAGIC while current_date <= end_date:
# MAGIC     logging.info(f"Processing {current_date}")
# MAGIC     records = collect_hourly_candles(SYMBOLS, current_date)
# MAGIC     df = (
# MAGIC         spark.createDataFrame(records)
# MAGIC         .withColumn("ingested_at", F.current_timestamp())
# MAGIC         .withColumn("rate_date", F.to_date(F.from_unixtime(F.col("open_time_ms") / 1000)))
# MAGIC     )
# MAGIC     bronze_binance.write_table(
# MAGIC         df, TABLE_NAME, 
# MAGIC         mode="overwrite", 
# MAGIC         partition_cols=["rate_date"], 
# MAGIC         dynamic_partition_overwrite=True
# MAGIC     )
# MAGIC     current_date += timedelta(days=1)
# MAGIC ```
# MAGIC
# MAGIC ### Caso 3: Detecção de Lacunas
# MAGIC ```python
# MAGIC # Verificar quais datas estão na tabela
# MAGIC df_dates = (
# MAGIC     bronze_binance.read_table(TABLE_NAME)
# MAGIC     .select("rate_date")
# MAGIC     .distinct()
# MAGIC     .orderBy("rate_date")
# MAGIC )
# MAGIC display(df_dates)
# MAGIC
# MAGIC # Identificar lacunas e reprocessar dias faltantes
# MAGIC ```
