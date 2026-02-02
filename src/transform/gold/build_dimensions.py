"""
Construir tabelas de dimensão para a camada gold do data warehouse.

Este módulo cria tabelas de dimensão normalizadas a partir de dados da camada silver,
seguindo um padrão de esquema em estrela para o pipeline de análise de mobilidade.
"""
from pathlib import Path
import pandas as pd
import hashlib

SILVER_DIR = Path("data/silver/mobilidade_bh")
GOLD_DIR = Path("data/gold/mobilidade_bh")
GOLD_DIR.mkdir(parents=True, exist_ok=True)

MCO_PATH = SILVER_DIR / "mco_consolidado.parquet"
TR_PATH = SILVER_DIR / "onibus_tempo_real.parquet"

def generate_hash_key(val) -> int:
    """Gera um ID numérico consistente baseado no valor (seja ele texto ou número)."""
    if pd.isna(val) or val == "":
        return -1 
    
    val_str = str(val) 
    
    return int(hashlib.sha256(val_str.encode('utf-8')).hexdigest(), 16) % 10**8


def build_dim_data(mco: pd.DataFrame, tr: pd.DataFrame) -> pd.DataFrame:
    """
    Construir dimensão de data a partir de dados MCO e tempo real.

    Args:
        mco: Dados consolidados do MCO.
        tr: Dados de ônibus em tempo real.

    Returns:
        Dimensão de data com atributos temporais.
    """
    # Extrair datas de dados em tempo real (coluna data_hora)
    tr_dates = pd.to_datetime(tr["data_hora"], errors="coerce").dt.date

    # Extrair datas de dados MCO (coluna viagem_data)
    mco_dates = (
        pd.to_datetime(mco["viagem_data"], errors="coerce")
        .dt.date
    )

    # Combinar, desduplicar e ordenar datas
    dim_data = (
        pd.concat(
            [tr_dates.dropna(), mco_dates.dropna()],
            ignore_index=True
        )
        .drop_duplicates()
        .sort_values()
    )

    # Criar dimensão com atributos temporais
    dim_data = pd.DataFrame({"data": dim_data})
    dim_data["data_key"] = (
        pd.to_datetime(dim_data["data"])
        .dt.strftime("%Y%m%d")
        .astype(int)
    )
    dim_data["ano"] = pd.to_datetime(dim_data["data"]).dt.year
    dim_data["mes"] = pd.to_datetime(dim_data["data"]).dt.month
    dim_data["dia"] = pd.to_datetime(dim_data["data"]).dt.day
    dim_data["dia_semana"] = pd.to_datetime(dim_data["data"]).dt.day_name()

    return dim_data


def build_dim_linha(mco: pd.DataFrame, tr: pd.DataFrame) -> pd.DataFrame:
    """
    Construir dimensão de linha a partir de dados MCO e tempo real.

    Args:
        mco: Dados consolidados do MCO.
        tr: Dados de ônibus em tempo real.

    Returns:
        Dimensão de linha com rotas únicas.
    """
    # Extrair linhas do MCO (alfanumérico)
    mco_lin = mco["linha_numero"].astype(str).str.strip()

    # Extrair linhas de dados em tempo real (código numérico)
    tr_lin = (
        tr["codigo_numero_linha"]
        .dropna()
        .astype(int)
        .astype(str)
        .str.strip()
    )

    # Combinar, desduplicar e ordenar
    dim_linha = (
        pd.concat([mco_lin, tr_lin], ignore_index=True)
        .dropna()
        .drop_duplicates()
    )
    dim_linha = (
        dim_linha.to_frame(name="linha")
        .sort_values("linha")
        .reset_index(drop=True)
    )
    
    dim_linha["linha_key"] = dim_linha["linha"].apply(generate_hash_key)

    return dim_linha


def build_dim_concessionaria(mco: pd.DataFrame) -> pd.DataFrame:
    """
    Construir dimensão de concessionária a partir de dados MCO.

    Args:
        mco: Dados consolidados do MCO.

    Returns:
        Dimensão de concessionária.
    """
    dim = (
        mco[["concessionaria_numero"]]
        .dropna()
        .drop_duplicates()
        .sort_values("concessionaria_numero")
        .reset_index(drop=True)
    )
    
    dim["concessionaria_key"] = dim["concessionaria_numero"].apply(generate_hash_key)

    return dim


def build_dim_empresa(mco: pd.DataFrame) -> pd.DataFrame:
    """
    Construir dimensão de empresa a partir de dados MCO.

    Args:
        mco: Dados consolidados do MCO.

    Returns:
        Dimensão de empresa.
    """
    dim = (
        mco[["empresa_operadora"]]
        .dropna()
        .drop_duplicates()
        .sort_values("empresa_operadora")
        .reset_index(drop=True)
    )

    dim["empresa_key"] = dim["empresa_operadora"].apply(generate_hash_key)

    return dim


def build_dim_veiculo(mco: pd.DataFrame, tr: pd.DataFrame) -> pd.DataFrame:
    """
    Construir dimensão de veículo a partir de dados MCO e tempo real.

    Args:
        mco: Dados consolidados do MCO.
        tr: Dados de ônibus em tempo real.

    Returns:
        Dimensão de veículo com IDs de veículo únicos.
    """
    # Extrair IDs de veículo de ambas as fontes
    mco_v = mco["numero_ordem_veiculo"].dropna()
    tr_v = tr["numero_ordem_veiculo"].dropna()

    # Combinar, desduplicar e ordenar
    dim = (
        pd.concat([mco_v, tr_v], ignore_index=True)
        .dropna()
        .drop_duplicates()
        .astype(int)
        .astype(str)
        .to_frame(name="veiculo_id")
        .sort_values("veiculo_id")
        .reset_index(drop=True)
    )
    
    dim["veiculo_key"] = dim["veiculo_id"].apply(generate_hash_key)

    return dim


def main() -> None:
    """
    Função principal para construir e salvar todas as tabelas de dimensão.

    Lê dados da camada silver e gera tabelas de dimensão na camada gold.
    """
    
    print("Iniciando a construção das dimensões Gold...")
    
    # Ler dados da camada silver
    print("Lendo dados da camada silver...")
    mco = pd.read_parquet(MCO_PATH)
    tr = pd.read_parquet(TR_PATH)

    # Construir dimensões
    print("Construindo dimensões...")
    dim_data = build_dim_data(mco, tr)
    dim_linha = build_dim_linha(mco, tr)
    dim_concessionaria = build_dim_concessionaria(mco)
    dim_empresa = build_dim_empresa(mco)
    dim_veiculo = build_dim_veiculo(mco, tr)

    # Salvar dimensões na camada gold
    print("Salvando dimensões na camada gold...")
    dim_data.to_parquet(GOLD_DIR / "dim_data.parquet", index=False)
    dim_linha.to_parquet(GOLD_DIR / "dim_linha.parquet", index=False)
    dim_concessionaria.to_parquet(
        GOLD_DIR / "dim_concessionaria.parquet", index=False
    )
    dim_empresa.to_parquet(GOLD_DIR / "dim_empresa.parquet", index=False)
    dim_veiculo.to_parquet(GOLD_DIR / "dim_veiculo.parquet", index=False)

    print("Dimensões Gold geradas com sucesso.")


if __name__ == "__main__":
    main()
