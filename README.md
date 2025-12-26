# ⛈️ Análise Histórica de Chuvas e Impacto em Petrópolis-RJ

![Status](https://img.shields.io/badge/Status-Em_Desenvolvimento-yellow)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)

Projeto de Engenharia de Dados para cruzar histórico climático (INMET) com desastres naturais (S2iD) na região serrana do Rio de Janeiro.

## 📂 Estrutura Simplificada

Os scripts de ETL foram desenhados para serem **executados diretamente na pasta onde estão os dados**.

### 1. Dados do INMET (Clima)
* **Fonte:** [BDMEP INMET](https://portal.inmet.gov.br/)
* **Script:** `src/etl_inmet.py`
* **Como usar:**
    1. Coloque o script na mesma pasta dos arquivos `.CSV` do INMET.
    2. Execute: `python etl_inmet.py`
    3. O arquivo `dados_climaticos_unificados.parquet` será criado lá.

### 2. Dados do S2iD (Desastres/Mortes)
* **Fonte:** [S2iD Dados Abertos](https://dadosabertos.mdr.gov.br/dataset/s2id_sedec)
* **Script:** `src/etl_s2id.py`
* **Como usar:**
    1. Coloque o script na mesma pasta dos arquivos `.csv` do governo.
    2. Execute: `python etl_s2id.py`
    3. O arquivo `s2id_petropolis_consolidado.parquet` será criado lá.

## 📊 Dashboard (Em breve)
O objetivo final é conectar esses dois arquivos Parquet no **Power BI** para visualização da correlação entre volume de chuva e fatalidades.

## 📝 Licença
MIT License - Flávio Cordeiro 2025.