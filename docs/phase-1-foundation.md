# 🪙 CryptoLake - Fase 1: Foundation & Market Data

**Duração:** 3 semanas  
**Objetivo:** Validar a arquitetura Bronze → Silver → Gold com dados OHLCV em granularidade horária e diária, disponibilizando as métricas básicas de mercado da Fase 1.

---

## 📋 Escopo Resumido

| Item | Detalhes |
|------|----------|
| **Exchanges** | Binance + Poloniex |
| **Ativos** | BTC, ETH, SOL, ADA, LINK (5 moedas) |
| **Período** | Últimos 30 dias |
| **Granularidade** | Diária + Horária (2 fatos) |
| **Arquitetura** | Medalhão (Bronze/Silver/Gold) |
| **Modelagem** | Star Schema (4 dims + 2 fatos) |
| **Dashboard** | 3 visualizações essenciais |
| **Métricas** | OHLCV + Variação (24h/1h) |

---

## 🎯 Entregáveis

✅ Pipeline Bronze (2 APIs → 2 tabelas)  
✅ Pipeline Silver (merge + limpeza)  
✅ Star Schema Gold (4 dimensões + 2 fatos)  
✅ Dashboard MVP com filtros  
✅ Documentação (arquitetura + dicionário)  
✅ **Métricas de Negócio (OHLCV):**
   - **Preço** - Open, High, Low, Close (abertura, máximo, mínimo, fechamento)
   - **Volume** - Quantidade total negociada no período
   - **Variação diária (24h)** - Percentual de valorização/desvalorização em 24 horas
   - **Variação horária (1h)** - Percentual de valorização/desvalorização em 1 hora

---

## 📐 Arquitetura

```
📡 Binance API + Poloniex API
         ↓
🥉 BRONZE (raw)
   ├─ bronze_binance_ohlcv.hourly
   └─ bronze_poloniex_ohlcv.hourly
         ↓
🥈 SILVER (conformed)
   └─ silver_market_ohlcv.hourly
         ↓
🥇 GOLD (star schema)
   ├─ dim_symbol
   ├─ dim_date
   ├─ dim_datetime
   ├─ dim_source
   ├─ fact_market_daily
   └─ fact_market_hourly
         ↓
📊 DASHBOARD

```
## 📐 Modelo Lógico - Star Schema

### Diagrama de Relacionamentos (1:N)

**Nota:** O projeto inclui 2 fatos com granularidades diferentes:
- `fact_market_daily` - Agregação diária (para análises de tendência)
- `fact_market_hourly` - Detalhe horário (para análises intraday)

```
         dim_date                                     dim_datetime
      ┌──────────────┐                            ┌──────────────────┐
      │ date_key PK  │                            │ datetime_key PK  │
      │ year         │                            │ date             │
      │ quarter      │                            │ hour             │
      │ is_weekend   │                            │ is_weekend       │
      └──────┬───────┘                            └────────┬─────────┘
             │ 1                                           │ 1
             |                                             | 
             |                                             |
             ↓ n                 dim_symbol                ↓ n         
    fact_market_daily         ┌──────────────┐         fact_market_hourly
    ┌─────────────────┐       │ symbol_id PK │        ┌─────────────────┐
    │ PK: symbol_id   │  n  1 │ symbol_name  │ 1   n  │ PK: symbol_id   │
    │ PK: date_key    │◄──────┤ category     ├──────► │ PK: datetime_key│
    │ PK: source_id   │       └──────────────┘        │ PK: source_id   │
    │                 │                               │                 │
    │ FK: symbol_id   │                               │ FK: symbol_id   │
    │ FK: date_key    │                               │ FK: datetime_key│
    │ FK: source_id   │        ┌──────────────┐       │ FK: source_id   │
    │                 │        │ source_id PK │       │                 │
    │ open_price      │  n   1 │ source_name  │ 1  n  | open_price      │
    │ high_price      │◄───────┤ is_active    ├──────►│ high_price      │
    │ low_price       │        └──────────────┘       │ low_price       │
    │ close_price     │          dim_source           │ close_price     │
    │ volume          │                               │ volume          │
    │ variation_24h%  │                               │ variation_1h%   │
    │ ingested_at     │                               │ ingested_at     │
    │ processed_at    │                               │ processed_at    │
    └─────────────────┘                               └─────────────────┘                               
 
```
### Leitura da Cardinalidade

- `dim_date` possui relacionamento **1:N** com `fact_market_daily`.
- `dim_datetime` possui relacionamento **1:N** com `fact_market_hourly`.
- `dim_symbol` é uma dimensão compartilhada e possui relacionamento **1:N** com as duas fatos.
- `dim_source` também é uma dimensão compartilhada e possui relacionamento **1:N** com as duas fatos.

## 📊 Estrutura das Tabelas Gold

### Resumo das Tabelas

| Tabela | Tipo | PK | Descrição |
|--------|------|----|-------------|
| **dim_symbol** | Dimensão | symbol_id (STRING) | Catálogo de criptomoedas (BTC, ETH, SOL, ADA, LINK) |
| **dim_date** | Dimensão | date_key (DATE) | Calendário diário |
| **dim_datetime** | Dimensão | datetime_key (TIMESTAMP) | Calendário horário |
| **dim_source** | Dimensão | source_id (STRING) | Exchanges (binance, poloniex) |
| **fact_market_daily** | Fato | (symbol_id, date_key, source_id) | Métricas OHLCV diárias (agregadas) |
| **fact_market_hourly** | Fato | (symbol_id, datetime_key, source_id) | Métricas OHLCV horárias (detalhe) |

---

### dim_symbol - Dimensão de Criptomoedas

**Colunas principais:**
- `symbol_id` (PK): BTC, ETH, SOL, ADA, LINK
- `symbol_name`: Nome completo (Bitcoin, Ethereum, Solana, Cardano, Chainlink)
- `category`: Layer 1, Oracle, Smart Contract Platform

**Tipo:** SCD Tipo 1 (sobrescreve)

**Nota:** Mapeamentos de pares específicos de exchanges (BTCUSDT, BTC_USDT) são tratados na camada Silver durante a transformação, não armazenados na dimensão.

---

### dim_date - Dimensão Calendário

**Colunas principais:**
- `date_key` (PK): Data (2026-06-20)
- `year`, `quarter`, `month`, `day`
- `day_of_week`, `day_of_week_name`
- `is_weekend`, `is_month_start`, `is_month_end`

**Range:** 2024-01-01 a 2027-12-31

---

### dim_datetime - Dimensão Calendário Horário

**Colunas principais:**
- `datetime_key` (PK): Timestamp (2026-06-20 15:00:00)
- `date`: Data (2026-06-20)
- `hour`: Hora (0-23)
- `day_of_week`, `day_of_week_name`
- `is_weekend`: TRUE se sábado ou domingo
- `is_business_hour`: TRUE se hora comercial (9h-18h)

**Range:** 2024-01-01 00:00 a 2027-12-31 23:00

---

### dim_source - Dimensão de Exchanges

**Colunas principais:**
- `source_id` (PK): binance, poloniex
- `source_name`: Nome da exchange
- `is_active`: Exchange ativa para coleta

---

### fact_market_daily - Fato de Mercado Diário

**Chave composta (PK):** `(symbol_id, date_key, source_id)`

**Métricas OHLCV:**
- `open_price`: Preço de abertura
- `high_price`: Preço máximo
- `low_price`: Preço mínimo
- `close_price`: Preço de fechamento
- `volume`: Volume negociado
- `variation_24h_pct`: Variação percentual em 24h

**Metadados:**
- `ingested_at`: Timestamp de ingestão (Bronze)
- `processed_at`: Timestamp de processamento (Gold)

**Granularidade:** 1 linha = 1 ativo + 1 dia + 1 exchange  
**Particionamento:** Por `date_key`

---

### fact_market_hourly - Fato de Mercado Horário

**Chave composta (PK):** `(symbol_id, datetime_key, source_id)`

**Métricas OHLCV:**
- `open_price`: Preço de abertura
- `high_price`: Preço máximo
- `low_price`: Preço mínimo
- `close_price`: Preço de fechamento
- `volume`: Volume negociado
- `variation_1h_pct`: Variação percentual em 1h

**Metadados:**
- `ingested_at`: Timestamp de ingestão (Bronze)
- `processed_at`: Timestamp de processamento (Gold)

**Granularidade:** 1 linha = 1 ativo + 1 hora + 1 exchange  
**Particionamento:** Por `date` (extraída de datetime_key)

---

### Relacionamentos (FKs)

**Fato Diário:**
```
fact_market_daily.symbol_id → dim_symbol.symbol_id
fact_market_daily.date_key → dim_date.date_key
fact_market_daily.source_id → dim_source.source_id
```

**Fato Horário:**
```
fact_market_hourly.symbol_id → dim_symbol.symbol_id
fact_market_hourly.datetime_key → dim_datetime.datetime_key
fact_market_hourly.source_id → dim_source.source_id
```

**Validação:** 100% de integridade referencial em ambos os fatos (nenhum órfão)
