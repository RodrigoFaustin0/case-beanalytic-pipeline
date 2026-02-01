import os
import pandas as pd
from datetime import datetime

BRONZE_PATH = "data/bronze/mobilidade_bh"
SILVER_PATH = "data/silver/mobilidade_bh"

def processar_mco(df):
    """Lógica específica para a base MCO"""
    
    # viagem e Data Fechamento são datas
    for col in ['viagem', 'data fechamento']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")
    
    # saída e Chegada são horários (HH:MM)
    for col in ['saida', 'chegada']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format='%H:%M', errors="coerce").dt.time
    return df

def processar_tempo_real(df):
    """Lógica específica para a base de Tempo Real."""
    
    # converte o formato 20260201010644 
    df['hr'] = pd.to_datetime(
        df['hr'].astype(str).str.split('.').str[0], # remove .0 se existir
        format='%Y%m%d%H%M%S', 
        errors="coerce"
    )
    
    return df

def run_silver():
    """Direciona cada arquivo para sua função de processamento."""
    os.makedirs(SILVER_PATH, exist_ok=True)

    for file in os.listdir(BRONZE_PATH):
        if not file.endswith(".csv"):
            continue

        print(f"Silver - lendo {file}")
        
        # tenta ler com separador padrão, se falhar ou vier 1 coluna só, tenta ';'
        df = pd.read_csv(os.path.join(BRONZE_PATH, file), sep=None, engine='python')

        # padronizar nomes de colunas
        df.columns = [c.lower().strip() for c in df.columns]

        # edentifica o tipo de arquivo pelas colunas e aplica a função correta
        if 'viagem' in df.columns and 'saida' in df.columns:
            print(f"Aplicando lógica MCO em {file}")
            df = processar_mco(df)
            
        elif 'hr' in df.columns:
            print(f"Aplicando lógica Tempo Real em {file}")
            df = processar_tempo_real(df)

        # limpezas comuns
        df.dropna(how="all", inplace=True) # remove linhas totalmente vazias
        df.drop_duplicates(inplace=True) # remove linhas duplicadas
        df["dt_ingestao"] = datetime.now() # adiciona coluna de data de ingestão

        # salvar em Parquet
        output_file = file.replace(".csv", ".parquet")
        df.to_parquet(os.path.join(SILVER_PATH, output_file), index=False)
        print(f"Arquivo silver gerado: {output_file}")

