import pandas as pd
import glob
import re
import unicodedata
import os

# CONFIGURAÇÕES
MUNICIPIO_ALVO = 'PETROPOLIS' # Sem acento
UF_ALVO = 'RJ'
ARQUIVO_SAIDA = 's2id_petropolis_consolidado.parquet'

def remover_acentos(texto):
    """
    Transforma 'Município' em 'MUNICIPIO' e 'Petrópolis' em 'PETROPOLIS'.
    Remove acentos e força maiúsculas.
    """
    if not isinstance(texto, str):
        return str(texto)
    return unicodedata.normalize('NFKD', texto)\
           .encode('ASCII', 'ignore')\
           .decode('ASCII').upper().strip()

def extrair_data_protocolo(protocolo):
    """
    Extrai a data do final do protocolo.
    Ex: 'RJ-F-3303906-13214-20220215' -> 2022-02-15
    """
    try:
        match = re.search(r'(\d{8})$', str(protocolo).strip())
        if match:
            return pd.to_datetime(match.group(1), format='%Y%m%d', errors='coerce')
    except:
        pass
    return pd.NaT

def encontrar_coluna_por_parte(df, termo):
    """Busca uma coluna que contenha o termo (ex: procura 'MORT' acha 'DH_MORTOS')"""
    for col in df.columns:
        if termo in col:
            return col
    return None

def processar_desastres():
    # Pega todos os CSVs na pasta onde o script for rodado
    arquivos = glob.glob('*.csv')
    
    if not arquivos:
        print("ERRO: Nenhum arquivo .csv encontrado. Rode este script na pasta dos arquivos!")
        return

    print(f"Encontrados {len(arquivos)} arquivos. Iniciando processamento...")
    lista_dfs = []

    for arquivo in arquivos:
        print(f"\nArquivo: {arquivo}")
        try:
            # 1. Descobrir dinamicamente onde começa o cabeçalho
            skip_lines = 0
            encoding_usado = 'utf-8'
            
            # Tenta ler as primeiras linhas para achar "UF;"
            try:
                with open(arquivo, 'r', encoding='utf-8') as f:
                    for i, linha in enumerate(f):
                        if linha.strip().startswith('UF;'):
                            skip_lines = i
                            break
            except UnicodeDecodeError:
                encoding_usado = 'latin1'
                with open(arquivo, 'r', encoding='latin1') as f:
                    for i, linha in enumerate(f):
                        if linha.strip().startswith('UF;'):
                            skip_lines = i
                            break
            
            print(f"  -> Cabeçalho detectado na linha {skip_lines} (Encoding: {encoding_usado})")

            # 2. Ler o DataFrame
            df = pd.read_csv(arquivo, sep=';', skiprows=skip_lines, encoding=encoding_usado, low_memory=False)

            # 3. Normalizar Nomes das Colunas (MUNICÍPIO -> MUNICIPIO)
            df.columns = [remover_acentos(c) for c in df.columns]
            
            # 4. Filtrar Petrópolis (Normalizando os dados também)
            if 'MUNICIPIO' in df.columns:
                df['MUNICIPIO_NORM'] = df['MUNICIPIO'].apply(remover_acentos)
                df_petro = df[
                    (df['UF'] == UF_ALVO) & 
                    (df['MUNICIPIO_NORM'] == MUNICIPIO_ALVO)
                ].copy()
                
                if not df_petro.empty:
                    print(f"  -> SUCESSO: {len(df_petro)} registros de Petrópolis encontrados.")
                    
                    # 5. Extração Inteligente de Dados
                    
                    # Data (Do Protocolo)
                    if 'PROTOCOLO' in df_petro.columns:
                        df_petro['data'] = df_petro['PROTOCOLO'].apply(extrair_data_protocolo)
                    else:
                        df_petro['data'] = pd.NaT

                    # Mortos (Procura coluna que tenha MORT)
                    col_mortos = encontrar_coluna_por_parte(df_petro, 'MORT')
                    df_petro['mortos'] = df_petro[col_mortos] if col_mortos else 0
                    
                    # Desabrigados
                    col_desabrigados = encontrar_coluna_por_parte(df_petro, 'DESABRIGADO')
                    df_petro['desabrigados'] = df_petro[col_desabrigados] if col_desabrigados else 0
                    
                    # Código do Desastre (Cobrade)
                    col_cobrade = encontrar_coluna_por_parte(df_petro, 'COBRADE')
                    df_petro['cod_desastre'] = df_petro[col_cobrade] if col_cobrade else ''

                    # Selecionar apenas o necessário
                    df_final = df_petro[['data', 'mortos', 'desabrigados', 'cod_desastre']].copy()
                    lista_dfs.append(df_final)
                    
                else:
                    print("  -> Nenhum registro para Petrópolis neste arquivo.")
            else:
                print("  -> ERRO: Coluna 'Município' não encontrada.")

        except Exception as e:
            print(f"  -> FALHA CRÍTICA: {e}")

    # Consolidação
    if lista_dfs:
        df_consolidado = pd.concat(lista_dfs, ignore_index=True)
        
        # Limpeza Final
        df_consolidado = df_consolidado.dropna(subset=['data']) # Remove se não conseguiu extrair data
        df_consolidado['mortos'] = df_consolidado['mortos'].fillna(0).astype(int)
        df_consolidado['desabrigados'] = df_consolidado['desabrigados'].fillna(0).astype(int)
        df_consolidado = df_consolidado.sort_values('data')

        print("\n" + "="*40)
        print("RESUMO DO PROCESSAMENTO")
        print("="*40)
        print(f"Total de Eventos Processados: {len(df_consolidado)}")
        print(f"Total de Mortos Contabilizados: {df_consolidado['mortos'].sum()}")
        print("\nMaiores tragédias identificadas:")
        print(df_consolidado.groupby('data')['mortos'].sum().sort_values(ascending=False).head(5))
        
        df_consolidado.to_parquet(ARQUIVO_SAIDA, index=False)
        print(f"\nArquivo salvo: {ARQUIVO_SAIDA}")
    else:
        print("\nNenhum dado foi gerado. Verifique se os CSVs estão na pasta.")

if __name__ == "__main__":
    processar_desastres()