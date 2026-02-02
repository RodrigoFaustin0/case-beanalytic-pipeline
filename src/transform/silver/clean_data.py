import os
import pandas as pd
from datetime import datetime

bronze_path = "data/bronze/mobilidade_bh"
silver_path = "data/silver/mobilidade_bh"

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
    
    # renomeia colunas  
    df = df.rename(columns={       
        'viagem': 'viagem_data',
        'linha': 'linha_numero',
        'sublinha': 'sublinha_numero',
        'pc': 'ponto_controle_numero',
        'concessionaria': 'concessionaria_numero',
        'saida': 'hora_saida',
        'veiculo': 'numero_ordem_veiculo',
        'chegada': 'hora_chegada',
        'catraca saida': 'catraca_saida',
        'catraca chegada': 'catraca_chegada',
        'ocorrencia': 'indicador_ocorrencia',
        'justificativa': 'indicador_justificativa',
        'tipo dia': 'tipo_dia',
        'extensao': 'extensao_viagem',
        'falha mecanica': 'indicador_falha_mecanica',
        'evento inseguro': 'indicador_evento_inseguro',
        'indicador fechamento': 'indicador_fechamento',
        'data fechamento': 'data_fechamento_viagem',
        'total usuarios': 'total_usuarios_viagem',
        'empresa operadora': 'empresa_operadora'
    })
    
    return df

def processar_tempo_real(df):
    """Lógica específica para a base de Tempo Real."""
    
    # converte o formato 20260201010644 
    df['hr'] = pd.to_datetime(
        df['hr'].astype(str).str.split('.').str[0], # remove .0 se existir
        format='%Y%m%d%H%M%S', 
        errors="coerce"
    )
    
    # renomeia colunas
    df = df.rename(columns={       
        'ev': 'codigo_evento',
        'hr': 'data_hora',
        'lt': 'latitude',
        'lg': 'longitude',
        'nv': 'numero_ordem_veiculo',
        'vl': 'velocidade_instantanea',
        'nl': 'codigo_numero_linha',
        'dg': 'direcao_veiculo',
        'sv': 'sentido_veiculo',
        'dt': 'distancia_percorrida'    
    })

    
    # correção de tipo, lat/long
    cols_geo = ['latitude', 'longitude']
    
    for col in cols_geo:
        if col in df.columns:
            # Substitui vírgula por ponto | Se der erro na conversão, vira NaT/NaN 
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(',', '.'), 
                errors='coerce'
            )

    return df

def run_silver():
    """Direciona cada arquivo para sua função de processamento."""
    os.makedirs(silver_path, exist_ok=True)

    for file in os.listdir(bronze_path):
        if not file.endswith(".csv"):
            continue

        print(f"Silver - lendo {file}")
        
        # tenta ler com separador padrão, se falhar ou vier 1 coluna só, tenta ';'
        df = pd.read_csv(os.path.join(bronze_path, file), sep=None, engine='python')

        # padronizar nomes de colunas (minusculo e espaços)
        df.columns = [c.lower().strip() for c in df.columns]

        # remove colunas totalmente vazias
        df = df.dropna(axis=1, how='all')

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
        df.to_parquet(os.path.join(silver_path, output_file), index=False)
        print(f"Arquivo silver gerado: {output_file}")

