# Contexto para Assistente IA

**Projeto:** Análise Histórica de Chuvas e Tragédias em Petrópolis-RJ
**Autor:** Flávio Cordeiro (Engenheiro de Dados)
**Objetivo:** Cruzar dados do INMET com registros de óbitos/desastres.

**Status Atual:**
1. **ETL:** Script Python (`src/etl_inmet.py`) finalizado. Lê CSVs (trata encoding latin1/utf8), limpa headers e converte para Parquet unificado.
2. **Dados:** Arquivos brutos em `data/raw` (ignorados pelo git), Parquet final em `data/processed`.
3. **Git:** Repositório configurado com `.gitignore` robusto, README profissional e Licença MIT.
4. **Próximo Passo:** Modelagem dimensional (Star Schema) para Power BI.

**Stack Tecnológica:**
- Python (Pandas, PyArrow)
- VS Code
- Power BI
- Git/GitHub