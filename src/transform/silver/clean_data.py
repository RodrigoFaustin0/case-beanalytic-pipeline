import os
import pandas as pd
from datetime import datetime

BRONZE_PATH = "data/bronze/mobilidade_bh"
SILVER_PATH = "data/silver/mobilidade_bh"


def run_silver():
    
    """Processa os arquivos da camada bronze e salva na camada silver."""
    
    
    os.makedirs(SILVER_PATH, exist_ok=True)

    for file in os.listdir(BRONZE_PATH):
        if not file.endswith(".csv"):
            continue

        print(f"Silver - processando {file}")

        df = pd.read_csv(f"{BRONZE_PATH}/{file}")

        # padronizar nomes de colunas
        df.columns = [c.lower().strip() for c in df.columns]

        # remover linhas completamente nulas
        df.dropna(how="all", inplace=True)

        # remover duplicados
        df.drop_duplicates(inplace=True)

        # tipagem básica

        # lista de colunas
        
        # lógica para base mco
        if 'viagem' in df.columns and 'saida' in df.columns:
            
            # Viagem e Data Fechamento são datas
            for col in ['viagem', 'data fechamento']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")
                    
            
            # Saída e Chegada são horários
            for col in ['saida', 'chegada']:
                if col in df.columns:
                    
                    # transforma em datetime e depois pega o horário (.dt.time)
                    df[col] = pd.to_datetime(df[col], format='%H:%M', errors="coerce").dt.time
                    

        # lógica para base tempo real
        elif 'hr' in df.columns:
            # O formato "AAAAMMDDHHMMSS"
            df['hr'] = pd.to_datetime(df['hr'], format='%Y%m%d%H%M%S', errors="coerce")

        # metadado técnico (tada da ingestão)
        df["dt_ingestao"] = datetime.now()

        # salvar em Parquet
        output_file = file.replace(".csv", ".parquet")
        df.to_parquet(
            os.path.join(SILVER_PATH, output_file),
            index=False
        )

        print(f"silver gerado: {output_file}")
