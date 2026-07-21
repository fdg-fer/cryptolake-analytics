# CryptoLake Analytics

Pipeline de dados para análise de mercado de criptomoedas, implementando arquitetura medallion (Bronze → Silver → Gold) no Databricks com Unity Catalog.

## Visão Geral

CryptoLake ingere dados OHLCV (Open, High, Low, Close, Volume) em granularidade horária de múltiplas exchanges (Binance, Poloniex) e os transforma em datasets prontos para análise, seguindo boas práticas de governança de dados e qualidade.

### Assets Suportados

* **Criptomoedas:** BTC, ETH, SOL, ADA, LINK
* **Moeda de Cotação:** USDT (Tether)
* **Granularidade:** Candles horários
* **Período:** 2026-01-01 em diante

---

## Arquitetura

### Arquitetura Medallion

```
┌─────────────────┐         ┌─────────────────┐         ┌──────────────────┐
│  Bronze Layer   │         │  Silver Layer   │         │   Gold Layer     │
│  (Raw Data)     │────────▶│  (Cleaned)      │────────▶│  (Analytics)     │
├─────────────────┤         ├─────────────────┤         ├──────────────────┤
│                 │         │                 │         │                  │
│ • Poloniex API  │         │ • Unified       │         │ • Star Schema    │
│   hourly        │         │   Market OHLCV  │         │                  │
│                 │         │                 │         │ Dimensions:      │
│ • Binance API   │         │ • Type casting  │         │  - dim_date      │
│   hourly        │         │ • Validation    │         │  - dim_datetime  │
│                 │         │ • Normalization │         │  - dim_exchange  │
│ • Raw format    │         │   (symbols)     │         │  - dim_symbol    │
│ • Partitioned   │         │                 │         │                  │
│   by rate_date  │         │ • Enrichment    │         │ Facts:           │
│                 │         │   (exchange ID) │         │  - fact_hourly   │
│ • Append mode   │         │                 │         │  - fact_daily    │
│                 │         │ • Quality checks│         │                  │
└─────────────────┘         └─────────────────┘         └──────────────────┘
```

### Fluxo de Dados

#### 1. Bronze Layer (Ingestão)

**Objetivo:** Capturar dados brutos das APIs públicas sem transformações.

**Características:**
* Schemas separados por exchange (`bronze_binance_ohlcv`, `bronze_poloniex_ohlcv`)
* Preserva estrutura original da API
* Particionado por `rate_date` (data do candle)
* Modo `append` com `dynamic_partition_overwrite=True`
* Campo `ingested_at` para rastreabilidade

**Notebooks:**
* `01_bronze_binance_ohlcv_ingestion.py` - Ingestão incremental D-1
* `01_bronze_binance_ohlcv_backfill.py` - Carga histórica
* `01_bronze_poloniex_ohlcv_ingestion.py` - Ingestão incremental D-1
* `01_bronze_poloniex_ohlcv_backfill.py` - Carga histórica

#### 2. Silver Layer (Transformação)

**Objetivo:** Unificar, limpar e padronizar dados de múltiplas fontes.

**Transformações:**
* Union de Binance + Poloniex
* Type casting: `string → DECIMAL(18,8)`, `long → TIMESTAMP`
* Normalização de símbolos: `BTCUSDT → BTC_USDT`
* Adição de identificador de exchange
* Validações de qualidade (OHLC consistency, null checks)

**Schema unificado:** `silver_market_ohlcv.hourly`

**Notebook:**
* `02_silver_market_ohlcv_transform.py`

#### 3. Gold Layer (Analytics)

**Objetivo:** Modelo dimensional otimizado para BI e analytics.

**Estrutura:**
* **Star Schema** com dimensões conformadas
* Fatos em granularidade horária e diária
* Foreign keys para dimensões
* Métricas calculadas (variações, volatilidade)

**Notebooks:**
* `03_gold_dim_datetime.py` - Dimensões temporais
* `03_gold_dim_exchange.py` - Dimensão de exchanges
* `03_gold_dim_symbol.py` - Dimensão de símbolos
* `03_gold_fact_ohlcv.py` - Fatos horários
* `03_gold_fact_ohlcv_daily.py` - Fatos diários agregados

---

## Unity Catalog Structure

**Catalog:** `uc_sa_br_dev` (South America / Brazil / Development)

```
uc_sa_br_dev
│
├── bronze_binance_ohlcv
│   └── hourly                    (24,120 registros, 201 partições)
│
├── bronze_poloniex_ohlcv
│   └── hourly                    (24,120 registros, 201 partições)
│
├── silver_market_ohlcv
│   └── hourly                    (48,240 registros, 201 partições)
│
└── gold_finance_investments_market_analysis
    ├── dim_date                  (201 registros)
    ├── dim_datetime              (4,824 registros)
    ├── dim_exchange              (2 registros)
    ├── dim_symbol                (5 registros)
    ├── fact_market_hourly        (48,240 registros)
    └── fact_market_daily         (2,010 registros)
```

---

## Pipeline de Execução

### Job Databricks

**Nome:** CryptoLake Gold - Market Analysis Star Schema  
**Job ID:** 323958807775286

**DAG de Execução:**

```
┌─────────────────────────┐  ┌─────────────────────────┐
│ Ingestion_bronze_binance│  │Ingestion_bronze_poloniex│
└───────────┬─────────────┘  └───────────┬─────────────┘
            │                            │
            └────────────┬───────────────┘
                         ▼
            ┌────────────────────────────┐
            │ Transform_silver_market    │
            └────────────┬───────────────┘
                         │
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
  ┌──────────┐   ┌──────────┐   ┌──────────┐
  │gold_dim_ │   │gold_dim_ │   │gold_dim_ │
  │datetime  │   │exchange  │   │symbol    │
  └─────┬────┘   └─────┬────┘   └─────┬────┘
        │              │              │
        └──────────────┼──────────────┘
                       ▼
          ┌────────────────────────┐
          │  gold_fact_market_     │
          │  hourly + daily        │
          └────────────────────────┘
```

**Frequência:** Diária (D-1 ingestion)

### Execução Manual

```python
# 1. Ingestão Bronze (incremental D-1)
dbutils.notebook.run("/Repos/CryptoLake/cryptolake-analytics/src/01_bronze_binance_ohlcv_ingestion", 1800)
dbutils.notebook.run("/Repos/CryptoLake/cryptolake-analytics/src/01_bronze_poloniex_ohlcv_ingestion", 1800)

# 2. Transformação Silver
dbutils.notebook.run("/Repos/CryptoLake/cryptolake-analytics/src/02_silver_market_ohlcv_transform", 1800)

# 3. Gold Layer
dbutils.notebook.run("/Repos/CryptoLake/cryptolake-analytics/src/03_gold_dim_datetime", 600)
dbutils.notebook.run("/Repos/CryptoLake/cryptolake-analytics/src/03_gold_dim_exchange", 600)
dbutils.notebook.run("/Repos/CryptoLake/cryptolake-analytics/src/03_gold_dim_symbol", 600)
dbutils.notebook.run("/Repos/CryptoLake/cryptolake-analytics/src/03_gold_fact_ohlcv", 1800)
dbutils.notebook.run("/Repos/CryptoLake/cryptolake-analytics/src/03_gold_fact_ohlcv_daily", 1800)
```

---

## Configuração

### Zordon Governance

Todos os notebooks utilizam a biblioteca Zordon para governança de nomenclatura e metadata:

```python
import zordon

proj = zordon.Project(
    spark=spark,
    country="br",
    region="sa",
    environment="dev",
)

client = proj.client(
    layer="bronze|silver|gold",
    domain="exchange_name|market|finance",
    subdomain="ohlcv|investments",
)
```

**Path Zordon:** `/Workspace/Repos/CryptoLake/zordon-data-utils/src`

### Estratégia de Particionamento

* **Bronze/Silver:** Particionado por `rate_date` (diário)
* **Gold Facts:** Particionado por `date_id`
* **Gold Dimensions:** Não particionado (tabelas pequenas)

**Modo de Escrita:** `dynamic_partition_overwrite=True` para todas as camadas
* ✅ Idempotência (reexecutar sem duplicação)
* ✅ Eficiência (sobrescreve apenas partições afetadas)
* ✅ Atomicidade no nível de partição

---

## Qualidade de Dados

### Validações Bronze
* Campos obrigatórios não-nulos (symbol, timestamps, OHLCV)
* Validação de resposta API (HTTP 200, JSON válido)
* Log de contagem de registros

### Validações Silver
* **Consistência OHLC:** `high >= low`, `high >= open`, `high >= close`
* **Timestamps:** `close_time > open_time`
* **Nulls:** Nenhum null em campos críticos
* **Type Casting:** Conversão string → DECIMAL validada

### Validações Gold
* **Integridade Referencial:** Todas FKs existem nas dimensões
* **Unicidade:** Sem duplicatas em business keys
* **Completude Temporal:** Sem gaps de datas
* **Agregação:** Totais diários = soma horária

---

## Performance

### Otimizações Implementadas
* Particionamento por data para partition pruning
* Dynamic partition overwrite para eficiência
* Delta Lake para ACID e time travel
* Zordon metadata caching

### Métricas de Execução

| Task | Tempo Médio | Registros Processados |
|------|-------------|----------------------|
| Bronze Binance | 22-25s | ~120 records (D-1) |
| Bronze Poloniex | 23-26s | ~120 records (D-1) |
| Silver Transform | 26-30s | ~240 records (D-1) |
| Gold Dimensions | 5-6min | ~5,000 records total |
| Gold Facts Hourly | 24-30s | ~240 records (D-1) |
| Gold Facts Daily | 20-25s | ~10 records (D-1) |

**Job completo (D-1):** ~7-8 minutos

---

## Troubleshooting

### Erros Comuns

#### 1. DELTA_METADATA_MISMATCH
**Causa:** Incompatibilidade de nullability no schema Delta  
**Solução:** Garantir `F.current_timestamp()` para `ingested_at` (gera `nullable=false`)

#### 2. API Rate Limiting
**Causa:** HTTP 429 das APIs Binance/Poloniex  
**Solução:** Implementar exponential backoff, reduzir concorrência

#### 3. Gaps de Data
**Causa:** Falha em execução incremental  
**Solução:** Executar backfill para período faltante

#### 4. Schema Evolution
**Causa:** Mudança de estrutura na API  
**Solução:** Revisar notebooks Bronze, atualizar mappings

---

## Estrutura do Projeto

```
cryptolake-analytics/
├── src/
│   ├── 01_bronze_binance_ohlcv_ingestion.py
│   ├── 01_bronze_binance_ohlcv_backfill.py
│   ├── 01_bronze_poloniex_ohlcv_ingestion.py
│   ├── 01_bronze_poloniex_ohlcv_backfill.py
│   ├── 02_silver_market_ohlcv_transform.py
│   ├── 03_gold_dim_datetime.py
│   ├── 03_gold_dim_exchange.py
│   ├── 03_gold_dim_symbol.py
│   ├── 03_gold_fact_ohlcv.py
│   └── 03_gold_fact_ohlcv_daily.py
├── jobs/
│   └── cryptolake_gold_pipeline.json
└── README.md
```

---

## Queries de Exemplo

### Comparação de Preço Cross-Exchange

```sql
SELECT 
    d.date,
    s.symbol,
    e.exchange_name,
    f.close_price,
    f.volume
FROM gold_finance_investments_market_analysis.fact_market_daily f
JOIN gold_finance_investments_market_analysis.dim_date d ON f.date_id = d.date_id
JOIN gold_finance_investments_market_analysis.dim_exchange e ON f.exchange_id = e.exchange_id
JOIN gold_finance_investments_market_analysis.dim_symbol s ON f.symbol_id = s.symbol_id
WHERE s.symbol = 'BTC_USDT'
  AND d.date BETWEEN '2026-01-01' AND '2026-01-31'
ORDER BY d.date, e.exchange_name;
```

### Top 5 Dias Mais Voláteis

```sql
SELECT 
    d.date,
    s.symbol,
    (f.high_price - f.low_price) / f.open_price * 100 AS volatility_pct,
    f.volume
FROM gold_finance_investments_market_analysis.fact_market_daily f
JOIN gold_finance_investments_market_analysis.dim_date d ON f.date_id = d.date_id
JOIN gold_finance_investments_market_analysis.dim_symbol s ON f.symbol_id = s.symbol_id
WHERE d.date BETWEEN '2026-01-01' AND '2026-07-20'
ORDER BY volatility_pct DESC
LIMIT 5;
```

---

# Dicionário de Dados

## CryptoLake – Camada Gold

A camada Gold concentra as tabelas analíticas do projeto, estruturadas para consumo em ferramentas de Business Intelligence. Nesta camada, os dados já passaram pelos processos de limpeza, padronização e modelagem, estando prontos para análises e visualizações.

---

# Modelo Dimensional

A camada Gold é composta pelas seguintes tabelas:

## Tabelas Fato

- fact_market_daily
- fact_market_hourly

## Tabelas Dimensão

- dim_symbol
- dim_exchange
- dim_date
- dim_datetime

---

# fact_market_daily

**Granularidade**

Uma linha por:

- Data
- Símbolo de criptomoeda
- Exchange

Esta tabela é utilizada pelo dashboard principal do projeto.

| Coluna           | Tipo     | Descrição                                              |
|------------------|----------|--------------------------------------------------------|
| date_id          | DATE     | Data de referência do candle diário.                   |
| symbol_id        | INT      | Chave do símbolo da criptomoeda.                       |
| exchange_id      | INT      | Chave da exchange.                                     |
| open_price       | DECIMAL  | Primeiro preço negociado no dia.                       |
| high_price       | DECIMAL  | Maior preço registrado no dia.                         |
| low_price        | DECIMAL  | Menor preço registrado no dia.                         |
| close_price      | DECIMAL  | Último preço negociado no dia.                         |
| volume           | DECIMAL  | Volume total negociado no dia.                         |
| daily_return_pct | DECIMAL  | Variação percentual em relação ao fechamento anterior. |

---

# fact_market_hourly

**Granularidade**

Uma linha por:

- Data e Hora
- Símbolo de criptomoeda
- Exchange

Esta tabela foi construída para suportar análises em granularidade horária e futuras evoluções do dashboard.

| Coluna      | Tipo      | Descrição                                 |
|-------------|-----------|-------------------------------------------|
| datetime_id | TIMESTAMP | Data e hora de referência.                |
| symbol_id   | INT       | Chave do símbolo da criptomoeda.          |
| exchange_id | INT       | Chave da exchange.                        |
| open_price  | DECIMAL   | Primeiro preço da hora.                   |
| high_price  | DECIMAL   | Maior preço da hora.                      |
| low_price   | DECIMAL   | Menor preço da hora.                      |
| close_price | DECIMAL   | Último preço da hora.                     |
| volume      | DECIMAL   | Volume negociado na hora.                 |

---

# dim_symbol

Tabela responsável pela identificação dos símbolos negociados.

| Coluna      | Descrição                                      |
|-------------|------------------------------------------------|
| symbol_id   | Chave substituta do símbolo da criptomoeda.    |
| symbol_name | Símbolo do ativo (BTCUSDT, ETHUSDT...).        |

---

# dim_exchange

Tabela responsável pela identificação da exchange de origem.

| Coluna      | Descrição                                      |
|-------------|------------------------------------------------|
| exchange_id | Chave substituta da exchange.                  |
| exchange_name | Nome da exchange (Binance ou Poloniex).      |

---

# dim_date

Dimensão calendário utilizada para filtros e análises temporais.

Exemplos de atributos:

- Ano
- Trimestre
- Mês
- Nome do mês
- Semana
- Dia
- Dia da semana

---

# dim_datetime

Dimensão utilizada nas análises em nível horário.

Além dos atributos da dimensão de datas, possui:

- Hora
- Ano-Mês
- Data/Hora completa

---

# Métricas do Dashboard

## Preço Atual

Último preço de fechamento disponível para o símbolo selecionado.

---

## Retorno Acumulado (%)

Representa a variação percentual entre o primeiro e o último preço do período selecionado.

**Objetivo:** Permitir comparar o desempenho dos símbolos dentro do intervalo escolhido pelo usuário.

---

## Índice Base 100

Normaliza todos os símbolos para uma base inicial igual a 100.

**Fórmula:**


Índice = (Preço Atual / Preço Inicial) × 100


**Objetivo:** Permitir comparar símbolos com preços absolutos muito diferentes.

---

## Volume Negociado

Soma do volume negociado durante o período selecionado.

---

## Ranking por Retorno

Classificação dos símbolos com base no retorno acumulado durante o período filtrado.

---

## Variação Diária (%)

Representa a variação percentual entre o fechamento atual e o fechamento do dia anterior.

---

## Dados Disponíveis Até

Corresponde à maior data existente na camada Gold.

Essa informação é utilizada no dashboard para indicar até qual data os dados estão disponíveis para análise.

---

# Regras de Negócio

- Os dados são obtidos através das APIs públicas da Binance e Poloniex.
- Os candles diários representam dados consolidados após o fechamento do período.
- A camada Silver realiza padronização de tipos, nomenclaturas e estrutura dos dados.
- A camada Gold organiza os dados em modelo dimensional para consumo analítico.
- O dashboard da Fase 1 utiliza a tabela `fact_market_daily`.
- A tabela `fact_market_hourly` foi construída para suportar futuras evoluções do projeto.

---

# Glossário

| Termo      | Definição                                                                 |
|------------|---------------------------------------------------------------------------|
| OHLC       | Open, High, Low e Close de um candle.                                     |
| Candle     | Agrupamento das negociações ocorridas durante um intervalo de tempo.       |
| Exchange   | Plataforma de negociação de criptomoedas.                                 |
| Base 100   | Índice utilizado para comparar símbolos com preços diferentes.             |
| Volume     | Quantidade negociada de determinado símbolo durante o período.             |
| Granularidade | Nível de detalhe dos dados (diário ou horário).                        |
