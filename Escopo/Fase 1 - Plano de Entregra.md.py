# Databricks notebook source
# DBTITLE 1,🪙 CryptoLake - Fase 1: Plano de Entrega
# MAGIC %md
# MAGIC # 🪙 CryptoLake - Fase 1: Foundation & Market Data
# MAGIC
# MAGIC **Duração:** 3 semanas  
# MAGIC **Objetivo:** Validar a arquitetura Bronze → Silver → Gold com dados OHLCV em granularidade horária e diária, disponibilizando as métricas básicas de mercado da Fase 1.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📋 Escopo Resumido
# MAGIC
# MAGIC | Item | Detalhes |
# MAGIC |------|----------|
# MAGIC | **Exchanges** | Binance + Poloniex |
# MAGIC | **Ativos** | BTC, ETH, SOL, ADA, LINK (5 moedas) |
# MAGIC | **Período** | Últimos 30 dias |
# MAGIC | **Granularidade** | Diária + Horária (2 fatos) |
# MAGIC | **Arquitetura** | Medalhão (Bronze/Silver/Gold) |
# MAGIC | **Modelagem** | Star Schema (4 dims + 2 fatos) |
# MAGIC | **Dashboard** | 3 visualizações essenciais |
# MAGIC | **Métricas** | OHLCV + Variação (24h/1h) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Entregáveis
# MAGIC
# MAGIC ✅ Pipeline Bronze (2 APIs → 2 tabelas)  
# MAGIC ✅ Pipeline Silver (merge + limpeza)  
# MAGIC ✅ Star Schema Gold (4 dimensões + 2 fatos)  
# MAGIC ✅ Dashboard MVP com filtros  
# MAGIC ✅ Documentação (arquitetura + dicionário)  
# MAGIC ✅ **Métricas de Negócio (OHLCV):**
# MAGIC    - **Preço** - Open, High, Low, Close (abertura, máximo, mínimo, fechamento)
# MAGIC    - **Volume** - Quantidade total negociada no período
# MAGIC    - **Variação diária (24h)** - Percentual de valorização/desvalorização em 24 horas
# MAGIC    - **Variação horária (1h)** - Percentual de valorização/desvalorização em 1 hora
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📐 Arquitetura
# MAGIC
# MAGIC ```
# MAGIC 📡 Binance API + Poloniex API
# MAGIC          ↓
# MAGIC 🥉 BRONZE (raw)
# MAGIC    ├─ bronze_binance_ohlcv.hourly
# MAGIC    └─ bronze_poloniex_ohlcv.hourly
# MAGIC          ↓
# MAGIC 🥈 SILVER (conformed)
# MAGIC    └─ silver_market_ohlcv.hourly
# MAGIC          ↓
# MAGIC 🥇 GOLD (star schema)
# MAGIC    ├─ dim_symbol
# MAGIC    ├─ dim_date
# MAGIC    ├─ dim_datetime
# MAGIC    ├─ dim_source
# MAGIC    ├─ fact_market_daily
# MAGIC    └─ fact_market_hourly
# MAGIC          ↓
# MAGIC 📊 DASHBOARD
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,📐 Modelo Lógico - Star Schema com Relacionamentos
# MAGIC %md
# MAGIC ## 📐 Modelo Lógico - Star Schema
# MAGIC
# MAGIC ### Diagrama de Relacionamentos (1:N)
# MAGIC
# MAGIC **Nota:** O projeto inclui 2 fatos com granularidades diferentes:
# MAGIC - `fact_market_daily` - Agregação diária (para análises de tendência)
# MAGIC - `fact_market_hourly` - Detalhe horário (para análises intraday)
# MAGIC
# MAGIC ```
# MAGIC          dim_date                                     dim_datetime
# MAGIC       ┌──────────────┐                            ┌──────────────────┐
# MAGIC       │ date_key PK  │                            │ datetime_key PK  │
# MAGIC       │ year         │                            │ date             │
# MAGIC       │ quarter      │                            │ hour             │
# MAGIC       │ is_weekend   │                            │ is_weekend       │
# MAGIC       └──────┬───────┘                            └────────┬─────────┘
# MAGIC              │ 1                                           │ 1
# MAGIC              |                                             | 
# MAGIC              |                                             |
# MAGIC              ↓ n                 dim_symbol                ↓ n         
# MAGIC     fact_market_daily         ┌──────────────┐         fact_market_hourly
# MAGIC     ┌─────────────────┐       │ symbol_id PK │        ┌─────────────────┐
# MAGIC     │ PK: symbol_id   │  n  1 │ symbol_name  │ 1   n  │ PK: symbol_id   │
# MAGIC     │ PK: date_key    │◄──────┤ category     ├──────► │ PK: datetime_key│
# MAGIC     │ PK: source_id   │       └──────────────┘        │ PK: source_id   │
# MAGIC     │                 │                               │                 │
# MAGIC     │ FK: symbol_id   │                               │ FK: symbol_id   │
# MAGIC     │ FK: date_key    │                               │ FK: datetime_key│
# MAGIC     │ FK: source_id   │        ┌──────────────┐       │ FK: source_id   │
# MAGIC     │                 │        │ source_id PK │       │                 │
# MAGIC     │ open_price      │  n   1 │ source_name  │ 1  n  | open_price      │
# MAGIC     │ high_price      │◄───────┤ is_active    ├──────►│ high_price      │
# MAGIC     │ low_price       │        └──────────────┘       │ low_price       │
# MAGIC     │ close_price     │          dim_source           │ close_price     │
# MAGIC     │ volume          │                               │ volume          │
# MAGIC     │ variation_24h%  │                               │ variation_1h%   │
# MAGIC     │ ingested_at     │                               │ ingested_at     │
# MAGIC     │ processed_at    │                               │ processed_at    │
# MAGIC     └─────────────────┘                               └─────────────────┘                               
# MAGIC  
# MAGIC ```
# MAGIC ### Leitura da Cardinalidade
# MAGIC
# MAGIC - `dim_date` possui relacionamento **1:N** com `fact_market_daily`.
# MAGIC - `dim_datetime` possui relacionamento **1:N** com `fact_market_hourly`.
# MAGIC - `dim_symbol` é uma dimensão compartilhada e possui relacionamento **1:N** com as duas fatos.
# MAGIC - `dim_source` também é uma dimensão compartilhada e possui relacionamento **1:N** com as duas fatos.

# COMMAND ----------

# DBTITLE 1,📊 DDLs Completos - Camada Gold
# MAGIC %md
# MAGIC ## 📊 Estrutura das Tabelas Gold
# MAGIC
# MAGIC ### Resumo das Tabelas
# MAGIC
# MAGIC | Tabela | Tipo | PK | Descrição |
# MAGIC |--------|------|----|-------------|
# MAGIC | **dim_symbol** | Dimensão | symbol_id (STRING) | Catálogo de criptomoedas (BTC, ETH, SOL, ADA, LINK) |
# MAGIC | **dim_date** | Dimensão | date_key (DATE) | Calendário diário |
# MAGIC | **dim_datetime** | Dimensão | datetime_key (TIMESTAMP) | Calendário horário |
# MAGIC | **dim_source** | Dimensão | source_id (STRING) | Exchanges (binance, poloniex) |
# MAGIC | **fact_market_daily** | Fato | (symbol_id, date_key, source_id) | Métricas OHLCV diárias (agregadas) |
# MAGIC | **fact_market_hourly** | Fato | (symbol_id, datetime_key, source_id) | Métricas OHLCV horárias (detalhe) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### dim_symbol - Dimensão de Criptomoedas
# MAGIC
# MAGIC **Colunas principais:**
# MAGIC - `symbol_id` (PK): BTC, ETH, SOL, ADA, LINK
# MAGIC - `symbol_name`: Nome completo (Bitcoin, Ethereum, Solana, Cardano, Chainlink)
# MAGIC - `category`: Layer 1, Oracle, Smart Contract Platform
# MAGIC
# MAGIC **Tipo:** SCD Tipo 1 (sobrescreve)
# MAGIC
# MAGIC **Nota:** Mapeamentos de pares específicos de exchanges (BTCUSDT, BTC_USDT) são tratados na camada Silver durante a transformação, não armazenados na dimensão.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### dim_date - Dimensão Calendário
# MAGIC
# MAGIC **Colunas principais:**
# MAGIC - `date_key` (PK): Data (2026-06-20)
# MAGIC - `year`, `quarter`, `month`, `day`
# MAGIC - `day_of_week`, `day_of_week_name`
# MAGIC - `is_weekend`, `is_month_start`, `is_month_end`
# MAGIC
# MAGIC **Range:** 2024-01-01 a 2027-12-31
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### dim_datetime - Dimensão Calendário Horário
# MAGIC
# MAGIC **Colunas principais:**
# MAGIC - `datetime_key` (PK): Timestamp (2026-06-20 15:00:00)
# MAGIC - `date`: Data (2026-06-20)
# MAGIC - `hour`: Hora (0-23)
# MAGIC - `day_of_week`, `day_of_week_name`
# MAGIC - `is_weekend`: TRUE se sábado ou domingo
# MAGIC - `is_business_hour`: TRUE se hora comercial (9h-18h)
# MAGIC
# MAGIC **Range:** 2024-01-01 00:00 a 2027-12-31 23:00
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### dim_source - Dimensão de Exchanges
# MAGIC
# MAGIC **Colunas principais:**
# MAGIC - `source_id` (PK): binance, poloniex
# MAGIC - `source_name`: Nome da exchange
# MAGIC - `is_active`: Exchange ativa para coleta
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### fact_market_daily - Fato de Mercado Diário
# MAGIC
# MAGIC **Chave composta (PK):** `(symbol_id, date_key, source_id)`
# MAGIC
# MAGIC **Métricas OHLCV:**
# MAGIC - `open_price`: Preço de abertura
# MAGIC - `high_price`: Preço máximo
# MAGIC - `low_price`: Preço mínimo
# MAGIC - `close_price`: Preço de fechamento
# MAGIC - `volume`: Volume negociado
# MAGIC - `variation_24h_pct`: Variação percentual em 24h
# MAGIC
# MAGIC **Metadados:**
# MAGIC - `ingested_at`: Timestamp de ingestão (Bronze)
# MAGIC - `processed_at`: Timestamp de processamento (Gold)
# MAGIC
# MAGIC **Granularidade:** 1 linha = 1 ativo + 1 dia + 1 exchange  
# MAGIC **Particionamento:** Por `date_key`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### fact_market_hourly - Fato de Mercado Horário
# MAGIC
# MAGIC **Chave composta (PK):** `(symbol_id, datetime_key, source_id)`
# MAGIC
# MAGIC **Métricas OHLCV:**
# MAGIC - `open_price`: Preço de abertura
# MAGIC - `high_price`: Preço máximo
# MAGIC - `low_price`: Preço mínimo
# MAGIC - `close_price`: Preço de fechamento
# MAGIC - `volume`: Volume negociado
# MAGIC - `variation_1h_pct`: Variação percentual em 1h
# MAGIC
# MAGIC **Metadados:**
# MAGIC - `ingested_at`: Timestamp de ingestão (Bronze)
# MAGIC - `processed_at`: Timestamp de processamento (Gold)
# MAGIC
# MAGIC **Granularidade:** 1 linha = 1 ativo + 1 hora + 1 exchange  
# MAGIC **Particionamento:** Por `date` (extraída de datetime_key)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Relacionamentos (FKs)
# MAGIC
# MAGIC **Fato Diário:**
# MAGIC ```
# MAGIC fact_market_daily.symbol_id → dim_symbol.symbol_id
# MAGIC fact_market_daily.date_key → dim_date.date_key
# MAGIC fact_market_daily.source_id → dim_source.source_id
# MAGIC ```
# MAGIC
# MAGIC **Fato Horário:**
# MAGIC ```
# MAGIC fact_market_hourly.symbol_id → dim_symbol.symbol_id
# MAGIC fact_market_hourly.datetime_key → dim_datetime.datetime_key
# MAGIC fact_market_hourly.source_id → dim_source.source_id
# MAGIC ```
# MAGIC
# MAGIC **Validação:** 100% de integridade referencial em ambos os fatos (nenhum órfão)
