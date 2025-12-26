import pandas as pd
import glob
import os

# CONFIGURAÇÕES GERAIS
# ====================
DIRETORIO_ENTRADA = '.'  
PADRAO_ARQUIVO = '*.CSV' 
ARQUIVO_SAIDA = 'dados_climaticos_unificados.parquet'

def processar_arquivos():
    """
    Lê múltiplos arquivos CSV, processa, limpa e consolida em um único arquivo Parquet.
    Robusto contra diferentes codificações (UTF-8, Latin1, CP1252).
    """
    caminho_busca = os.path.join(DIRETORIO_ENTRADA, PADRAO_ARQUIVO)
    arquivos = glob.glob(caminho_busca)
    
    if not arquivos:
        print(f"Nenhum arquivo encontrado no padrão: {caminho_busca}")
        return

    print(f"Encontrados {len(arquivos)} arquivos para processar.")
    
    lista_dfs = []

    for arquivo in arquivos:
        nome_arquivo = os.path.basename(arquivo)
        print(f"Lendo: {nome_arquivo}...", end=" ")
        
        df = None
        # Lista de codificações para tentar (do mais comum para o legado)
        codificacoes = ['utf-8', 'latin1', 'cp1252']
        
        for encoding in codificacoes:
            try:
                # Tenta ler com a codificação atual do loop
                df = pd.read_csv(
                    arquivo,
                    sep=';',
                    decimal=',',
                    skiprows=8,
                    encoding=encoding,
                    engine='c'
                )
                print(f"[OK] (Codificação usada: {encoding})")
                break # Sucesso! Sai do loop de tentativas
            except UnicodeDecodeError:
                # Se falhar, continua para a próxima codificação da lista
                continue
            except Exception as e:
                print(f"\nERRO genérico em {nome_arquivo}: {e}")
                break

        # Se após todas as tentativas o df ainda for None, avisar o erro
        if df is None:
            print(f"\nFALHA FATAL: Não foi possível ler o arquivo {nome_arquivo} com nenhuma das codificações previstas.")
            continue

        try:
            # LIMPEZA E PADRONIZAÇÃO
            # ======================
            
            # 1. Remove colunas vazias (comuns devido ao ; extra no final da linha)
            df = df.dropna(axis=1, how='all')
            
            # 2. Remove colunas 'Unnamed' explicitamente se sobrarem
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

            # 3. Cria a coluna de origem
            df['arquivo_origem'] = nome_arquivo
            
            # 4. Garante que nomes de colunas não tenham espaços extras
            df.columns = df.columns.str.strip()

            lista_dfs.append(df)
            
        except Exception as e:
            print(f"ERRO ao processar dados de {nome_arquivo}: {e}")

    # CONSOLIDAÇÃO
    # ============
    if lista_dfs:
        print("\nUnindo dataframes...")
        # concat une todos os pedaços em um só gigante
        df_final = pd.concat(lista_dfs, ignore_index=True)
        
        print(f"Total de registros importados: {len(df_final)}")
        print(f"Salvando em: {ARQUIVO_SAIDA}")
        
        # Salva em Parquet
        df_final.to_parquet(ARQUIVO_SAIDA, index=False)
        print("Concluído com sucesso!")
    else:
        print("Nenhum dado foi processado corretamente.")

if __name__ == "__main__":
    processar_arquivos()