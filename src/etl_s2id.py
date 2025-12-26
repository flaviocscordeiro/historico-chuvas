import pandas as pd
import glob
import re
import unicodedata

# CONFIGURAÇÃO
ARQUIVO_SAIDA = 's2id_petropolis_consolidado.parquet'
MUNICIPIO_ALVO = 'PETROPOLIS'
UF_ALVO = 'RJ'

def normalizar_texto(texto):
    """Remove acentos e passa para maiúsculo"""
    if not isinstance(texto, str): return str(texto)
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII').upper()

def extrair_data_protocolo(protocolo):
    """Extrai a data real do protocolo (ex: ...20220215)"""
    try:
        match = re.search(r'(\d{8})$', str(protocolo))
        if match:
            return pd.to_datetime(match.group(1), format='%Y%m%d', errors='coerce')
    except:
        pass
    return pd.NaT

def ler_csv_robusto(arquivo, linhas_pular):
    """Tenta ler com utf-8, se falhar tenta latin1"""
    try:
        return pd.read_csv(arquivo, sep=';', skiprows=linhas_pular, encoding='utf-8', low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(arquivo, sep=';', skiprows=linhas_pular, encoding='latin1', low_memory=False)

def processar_desastres():
    arquivos = glob.glob('*.csv')
    
    if not arquivos:
        print("ERRO: Nenhum arquivo .csv encontrado nesta pasta.")
        return

    print(f"Processando {len(arquivos)} arquivos na pasta atual...")
    lista_dfs = []

    for arquivo in arquivos:
        try:
            print(f"Lendo: {arquivo}...", end=" ")
            
            # 1. Detectar cabeçalho
            skip_lines = 0
            # Abre como latin1 apenas para contar linhas (funciona pra utf8 tb nesse caso)
            with open(arquivo, 'r', encoding='latin1') as f:
                for i, linha in enumerate(f):
                    if linha.strip().startswith('UF;'):
                        skip_lines = i
                        break
            
            # 2. Ler Arquivo (com tentativa de encoding duplo)
            df = ler_csv_robusto(arquivo, skip_lines)
            
            # 3. Normalizar colunas
            df.columns = [normalizar_texto(c).replace(' ', '_') for c in df.columns]
            
            # 4. Filtrar Município
            if 'MUNICIPIO' in df.columns:
                df['MUNICIPIO_NORM'] = df['MUNICIPIO'].apply(normalizar_texto)
                df = df[(df['UF'] == UF_ALVO) & (df['MUNICIPIO_NORM'] == MUNICIPIO_ALVO)].copy()
            
            if not df.empty:
                # 5. Arrumar Data
                if 'PROTOCOLO' in df.columns:
                    df['data_evento'] = df['PROTOCOLO'].apply(extrair_data_protocolo)
                # Fallback: Se não achou data no protocolo, usa a data do registro
                if 'REGISTRO' in df.columns:
                     df['data_registro'] = pd.to_datetime(df['REGISTRO'], dayfirst=True, errors='coerce')
                     df['data_evento'] = df['data_evento'].fillna(df['data_registro'])

                # 6. Filtro Chuva (com regex=True para evitar warning)
                if 'COBRADE' in df.columns:
                    df = df[df['COBRADE'].astype(str).str.contains(r'^(1\.1\.3|1\.2|1\.3)', regex=True)]
                
                lista_dfs.append(df)
                print("OK")
            else:
                print("Ignorado (sem dados de Petrópolis)")
                
        except Exception as e:
            print(f"Erro: {e}")

    # CONSOLIDAÇÃO FINAL
    if lista_dfs:
        df_final = pd.concat(lista_dfs, ignore_index=True)
        
        # Mapeamento Flexível (Aceita variações de nomes de colunas)
        # Cria um DF novo limpo
        df_clean = pd.DataFrame()
        
        # Data
        df_clean['data'] = df_final['data_evento']
        
        # Código do Desastre
        df_clean['cod_desastre'] = df_final['COBRADE'] if 'COBRADE' in df_final.columns else None
        
        # Mortos (Procura por DH_MORTOS ou MORTOS)
        if 'DH_MORTOS' in df_final.columns:
            df_clean['mortos'] = df_final['DH_MORTOS']
        elif 'MORTOS' in df_final.columns:
            df_clean['mortos'] = df_final['MORTOS']
        else:
            df_clean['mortos'] = 0
            
        # Desabrigados
        if 'DH_DESABRIGADOS' in df_final.columns:
            df_clean['desabrigados'] = df_final['DH_DESABRIGADOS']
        elif 'DESABRIGADOS' in df_final.columns:
             df_clean['desabrigados'] = df_final['DESABRIGADOS']
        else:
            df_clean['desabrigados'] = 0

        # Tratamento final
        df_clean = df_clean.dropna(subset=['data']) # Remove linhas sem data
        df_clean['mortos'] = df_clean['mortos'].fillna(0).astype(int)
        df_clean['desabrigados'] = df_clean['desabrigados'].fillna(0).astype(int)
        
        # Remove duplicatas exatas
        df_clean = df_clean.drop_duplicates()
        
        # Salva
        df_clean.to_parquet(ARQUIVO_SAIDA, index=False)
        print(f"\nSUCESSO! Arquivo '{ARQUIVO_SAIDA}' gerado.")
        
        # Validação Rápida
        total_mortos = df_clean['mortos'].sum()
        print(f"Total de mortos identificados: {total_mortos}")
        print("Top 3 dias com mais fatalidades:")
        print(df_clean.groupby('data')['mortos'].sum().sort_values(ascending=False).head(3))
        
    else:
        print("Nenhum dado foi gerado.")

if __name__ == "__main__":
    processar_desastres()