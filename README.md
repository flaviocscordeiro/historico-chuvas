# ⛈️ Análise Histórica de Chuvas e Impacto em Petrópolis-RJ

![Status](https://img.shields.io/badge/Status-Em_Desenvolvimento-yellow)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Power BI](https://img.shields.io/badge/Power_BI-Star_Schema-gold)

Projeto de Engenharia de Dados e Analytics para cruzar histórico climático (INMET) com desastres naturais (S2iD) na região serrana do Rio de Janeiro, com foco na identificação de padrões em grandes tragédias.

## 📂 Estrutura de Engenharia (ETL)

Os scripts foram desenhados para execução local simplificada, convertendo dados brutos governamentais em formato **Parquet** de alta performance.

### 1. Dados Climáticos (INMET)
* **Script:** `src/etl_inmet.py`
* **Função:** Processa CSVs da estação Pico do Couto, trata encodings legados e unifica a série histórica desde 2007.
* **Output:** `dados_climaticos_unificados.parquet`

### 2. Dados de Desastres (S2iD)
* **Script:** `src/etl_s2id.py`
* **Função:** Extrai dados de óbitos e desabrigados dos relatórios do Ministério da Integração, filtrando apenas eventos hidrológicos em Petrópolis.
* **Output:** `s2id_petropolis_consolidado.parquet`

## 📊 Modelagem de Dados (Power BI)

O projeto utiliza um **Star Schema (Esquema Estrela)** para garantir performance e facilidade de análise temporal.

### Tabela Dimensão: `dCalendario`
Gerada via linguagem M, serve como eixo temporal único para cruzar as duas tabelas fato. Possui colunas estratégicas para a região:
* **Estação:** Sazonalidade (Verão, Outono, etc).
* **EhPeriodoCritico:** Flag booleana para filtrar meses de alto risco (Dez, Jan, Fev, Mar).

### Relacionamentos
* `dCalendario[Data]` (1) -> (*) `fDadosClimaticos[DATA]`
* `dCalendario[Data]` (1) -> (*) `fDesastres[data]`

## 🚀 Como Executar
1. Instale as dependências: `pip install pandas pyarrow fastparquet`
2. Coloque os scripts `src/` nas respectivas pastas de dados brutos (`data/raw/...`).
3. Execute os scripts para gerar os arquivos Parquet.
4. Importe a pasta `data/processed` no Power BI.

## 📝 Licença
MIT License - Flávio Cordeiro 2025.