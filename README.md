# ⛈️ Análise Histórica de Chuvas e Impacto em Petrópolis-RJ

![Status do Projeto](https://img.shields.io/badge/Status-Em_Desenvolvimento-yellow)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Power BI](https://img.shields.io/badge/Power_BI-Dashboard-gold)

Este projeto é uma iniciativa de Engenharia e Análise de Dados focada em cruzar dados meteorológicos históricos com eventos de tragédias climáticas na cidade de Petrópolis, Rio de Janeiro.

O objetivo é criar uma base de dados robusta e performática para identificar padrões climáticos críticos que precederam desastres naturais na região serrana.

## 🎯 Objetivos
- **Engenharia de Dados:** Construir um pipeline ETL (Extração, Transformação e Carga) robusto para processar grandes volumes de dados brutos.
- **Performance:** Converter dados de arquivos de texto (CSV) legados para formatos colunaies modernos (Parquet).
- **Analytics:** Desenvolver um dashboard interativo no Power BI para visualização da correlação entre precipitação acumulada e ocorrências de desastres.

## 🗂️ Fonte dos Dados
Os dados meteorológicos brutos foram extraídos do **Banco de Dados Meteorológicos para Ensino e Pesquisa (BDMEP)** do INMET (Instituto Nacional de Meteorologia).
- **Fonte:** [BDMEP - INMET](https://bdmep.inmet.gov.br/)
- **Estação:** PICO DO COUTO (A610)
- **Período:** 2007 a 2025
- **Granularidade:** Horária

## 🛠️ Tecnologias Utilizadas
- **Linguagem:** Python
- **Bibliotecas:** Pandas, PyArrow, Glob, OS
- **Armazenamento:** Parquet (Compressão Snappy)
- **Visualização:** Microsoft Power BI
- **Versionamento:** Git & GitHub

## ⚙️ Arquitetura da Solução

```mermaid
graph LR
    A[INMET (CSVs Brutos)] -->|Script Python| B(Limpeza & Tratamento)
    B -->|Encoding & Tipagem| C{Validacao}
    C -->|Sucesso| D[Arquivo Parquet Unificado]
    D -->|Import| E[Power BI / Analytics]