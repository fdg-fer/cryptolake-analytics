# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Documentation - Backfill Strategy
# MAGIC %md
# MAGIC # Bronze Backfill: Binance OHLCV Historical Data
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Backfill historical hourly OHLCV candles from Binance US for BTC, ETH, SOL, ADA, and LINK.
# MAGIC
# MAGIC ## Date Range
# MAGIC
# MAGIC * **Start:** 2026-01-01
# MAGIC * **End:** 2026-07-04 (yesterday)
# MAGIC * **Total days:** ~185 days
# MAGIC
# MAGIC ## Strategy
# MAGIC
# MAGIC * **Iteration:** Day-by-day loop using the same API functions from D-1 notebook
# MAGIC * **Write Mode:** `append` (safe for reruns, Delta Lake deduplicates)
# MAGIC * **Error Handling:** Continues even if individual days fail (API rate limits)
# MAGIC * **Progress Tracking:** Logs every 10 days
# MAGIC
# MAGIC ## Post-Backfill
# MAGIC
# MAGIC * Reprocess Silver and Gold layers to include historical data
# MAGIC * D-1 incremental jobs continue normally
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC * Run this notebook once to populate historical data
# MAGIC * Idempotent - safe to rerun for specific date ranges
# MAGIC * Does not affect current D-1 incremental ingestion

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

# DBTITLE 1,Configuration - Backfill Range
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "ADAUSDT", "LINKUSDT"]
INTERVAL = "1h"
TABLE_NAME = "hourly"
MAX_LIMIT = 500

START_DATE = date(2026, 1, 1)
END_DATE = date(2026, 7, 19)

total_days = (END_DATE - START_DATE).days + 1
logging.info(f"Backfill configuration loaded")
logging.info(f"Date range: {START_DATE} to {END_DATE} ({total_days} days)")
logging.info(f"Symbols: {SYMBOLS}")

# COMMAND ----------

# DBTITLE 1,API Functions - Binance
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
            raise RuntimeError(f"API error: {response.status}")
        data = json.loads(response.read().decode("utf-8"))

    candles = []
    for candle in data:
        candles.append({
            "symbol": symbol,
            "open_time_ms": int(candle[0]),
            "close_time_ms": int(candle[6]),
            "open": candle[1],
            "high": candle[2],
            "low": candle[3],
            "close": candle[4],
            "volume": candle[5],
            "trade_count": int(candle[8]),
            "interval": INTERVAL,
        })

    return candles


def collect_hourly_candles(symbols, target_date):
    """Collect hourly candles for multiple symbols for a specific date.

    Args:
        symbols: List of symbol pairs
        target_date: Date to collect data for

    Returns:
        List of raw candle records with metadata
    """
    start_dt = datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1) - timedelta(milliseconds=1)

    start_time_ms = int(start_dt.timestamp() * 1000)
    end_time_ms = int(end_dt.timestamp() * 1000)

    all_records = []

    for symbol in symbols:
        try:
            candles = fetch_binance_candles(symbol, start_time_ms, end_time_ms)
            all_records.extend(candles)
        except Exception as e:
            logging.error(f"Failed to fetch {symbol} for {target_date}: {e}")
            continue

    return all_records

# COMMAND ----------

# DBTITLE 1,Zordon Client Setup
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
logging.info(f"Target table: {target_fqn}")

# COMMAND ----------

# DBTITLE 1,Backfill Loop - Day by Day
current_date = START_DATE
all_dataframes = []
failed_dates = []
days_processed = 0

logging.info(f"Starting backfill: {START_DATE} to {END_DATE}")

while current_date <= END_DATE:
    try:
        records = collect_hourly_candles(SYMBOLS, current_date)

        if records:
            df_day = (
                spark.createDataFrame(records)
                .withColumn("ingested_at", F.current_timestamp())
                .withColumn(
                    "rate_date",
                    F.to_date(F.from_unixtime(F.col("open_time_ms") / 1000))
                )
            )
            all_dataframes.append(df_day)
            days_processed += 1

            if days_processed % 10 == 0:
                logging.info(f"Progress: {days_processed}/{total_days} days processed ({current_date})")
        else:
            logging.warning(f"No records for {current_date}")
            failed_dates.append(current_date)

    except Exception as e:
        logging.error(f"Failed to process {current_date}: {e}")
        failed_dates.append(current_date)

    current_date += timedelta(days=1)

logging.info(f"Backfill collection complete: {days_processed}/{total_days} days processed")

if failed_dates:
    logging.warning(f"Failed dates ({len(failed_dates)}): {failed_dates[:10]}...")

# COMMAND ----------

# DBTITLE 1,Union All DataFrames
if not all_dataframes:
    raise RuntimeError("No data collected during backfill")

df_backfill = all_dataframes[0]

for df_day in all_dataframes[1:]:
    df_backfill = df_backfill.union(df_day)

total_records = df_backfill.count()
logging.info(f"Unified DataFrame created with {total_records} records")

# COMMAND ----------

# DBTITLE 1,Write to Bronze - Append Mode
partition_dates = (
    df_backfill
    .select("rate_date")
    .distinct()
    .orderBy("rate_date")
    .collect()
)

partition_list = [row.rate_date for row in partition_dates]
logging.info(f"Writing {len(partition_list)} partitions to Bronze...")
logging.info(f"First partition: {partition_list[0]}, Last partition: {partition_list[-1]}")

written_fqn = bronze_binance.write_table(
    df=df_backfill,
    table_name=TABLE_NAME,
    mode="append",
)

logging.info(f"Backfill written to: {written_fqn}")

# COMMAND ----------

# DBTITLE 1,Validation - Date Range Check
df_read = bronze_binance.read_table(TABLE_NAME)

date_range = (
    df_read
    .select(
        F.min("rate_date").alias("min_date"),
        F.max("rate_date").alias("max_date"),
        F.count("*").alias("total_rows"),
        F.countDistinct("rate_date").alias("distinct_dates"),
        F.countDistinct("symbol").alias("distinct_symbols"),
    )
    .collect()[0]
)

logging.info(f"Validation complete:")
logging.info(f"  Date range: {date_range.min_date} to {date_range.max_date}")
logging.info(f"  Total rows: {date_range.total_rows}")
logging.info(f"  Distinct dates: {date_range.distinct_dates}")
logging.info(f"  Distinct symbols: {date_range.distinct_symbols}")
