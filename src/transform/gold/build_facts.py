"""
Construir fatos para a camada gold.

Este módulo cria tabelas de fatos para MCO (viagens consolidadas) e eventos
em tempo real, enriquecendo dados da camada silver com chaves de dimensões.
"""

import pandas as pd
from pathlib import Path

# Caminhos para dados da camada silver
SILVER_DIR = Path("data/silver/mobilidade_bh")
GOLD_DIR = Path("data/gold/mobilidade_bh")
GOLD_DIR.mkdir(parents=True, exist_ok=True)

MCO_SILVER = SILVER_DIR / "mco_consolidado.parquet"
TR_SILVER = SILVER_DIR / "onibus_tempo_real.parquet"

# Caminhos para dimensões da camada gold
DIM_LINHA = GOLD_DIR / "dim_linha.parquet"
DIM_DATA = GOLD_DIR / "dim_data.parquet"
DIM_CONC = GOLD_DIR / "dim_concessionaria.parquet"
DIM_EMP = GOLD_DIR / "dim_empresa.parquet"
DIM_VEIC = GOLD_DIR / "dim_veiculo.parquet"

# Caminhos para fatos da camada gold
FATO_MCO = GOLD_DIR / "fato_mco_viagem.parquet"
FATO_TR = GOLD_DIR / "fato_tempo_real_evento.parquet"


def incremental_append_by_data_key(
    df_new: pd.DataFrame,
    path: Path,
    key_col: str = "data_key",
) -> pd.DataFrame:
    """
    Adicionar registros incrementalmente, evitando duplicatas por chave de data.

    Args:
        df_new: Novo dataframe a adicionar.
        path: Caminho para arquivo parquet existente.
        key_col: Nome da coluna usada como chave de data (padrão: "data_key").

    Returns:
        Dataframe concatenado com registros antigos e novos, ou apenas novos
        se o arquivo não existir.
    """
    if path.exists():
        old = pd.read_parquet(path)
        existing = set(old[key_col].unique().tolist())
        inc = df_new[~df_new[key_col].isin(existing)]
        return pd.concat([old, inc], ignore_index=True)
    return df_new


def main() -> None:
    """
    Construir tabelas de fatos combinando dados silver com tabelas de dimensões.

    Processa dados de MCO (viagens) e eventos em tempo real, enriquecendo-os
    com chaves de dimensões e salvando incrementalmente na camada gold.
    """
    # Carregar dimensões
    dim_linha = pd.read_parquet(DIM_LINHA)[["linha", "linha_key"]]
    dim_data = pd.read_parquet(DIM_DATA)[["data_key"]]
    dim_conc = pd.read_parquet(DIM_CONC)[["concessionaria_numero", "concessionaria_key"]]
    dim_emp = pd.read_parquet(DIM_EMP)[["empresa_operadora", "empresa_key"]]
    dim_veic = pd.read_parquet(DIM_VEIC)[["veiculo_id", "veiculo_key"]]

    # -----------------------------------------
    # FATO MCO (viagens)
    # -----------------------------------------
    mco = pd.read_parquet(MCO_SILVER)

    # Criar chaves analíticas
    mco["data_key"] = mco["viagem_data"].dt.strftime("%Y%m%d").astype(int)
    mco["linha"] = mco["linha_numero"].astype(str).str.strip()
    mco["veiculo_id"] = mco["numero_ordem_veiculo"].astype(int).astype(str)

    # Enriquecer com chaves de dimensões
    fato_mco = (
        mco
        .merge(dim_linha, on="linha", how="left")
        .merge(dim_conc, left_on="concessionaria_numero",
               right_on="concessionaria_numero", how="left")
        .merge(dim_emp, left_on="empresa_operadora",
               right_on="empresa_operadora", how="left")
        .merge(dim_veic, on="veiculo_id", how="left")
    )

    # Selecionar colunas finais
    fato_mco = fato_mco[[
        "data_key",
        "linha_key",
        "concessionaria_key",
        "empresa_key",
        "veiculo_key",
        "sublinha_numero",
        "ponto_controle_numero",
        "hora_saida",
        "hora_chegada",
        "catraca_saida",
        "catraca_chegada",
        "indicador_ocorrencia",
        "indicador_justificativa",
        "tipo_dia",
        "extensao_viagem",
        "indicador_falha_mecanica",
        "indicador_evento_inseguro",
        "indicador_fechamento",
        "data_fechamento_viagem",
        "total_usuarios_viagem",
        "dt_ingestao",
    ]].copy()

    # Adicionar incrementalmente por dia
    fato_mco = fato_mco[fato_mco["data_key"].isin(dim_data["data_key"])]
    fato_mco_out = incremental_append_by_data_key(fato_mco, FATO_MCO, "data_key")
    fato_mco_out.to_parquet(FATO_MCO, index=False)

    # -----------------------------------------
    # FATO Tempo Real (eventos)
    # -----------------------------------------
    tr = pd.read_parquet(TR_SILVER)

    # Criar chaves analíticas
    tr["data_key"] = tr["data_hora"].dt.strftime("%Y%m%d").astype(int)
    tr["linha"] = tr["codigo_numero_linha"].dropna().astype(int).astype(str)\
        .str.strip()
    tr["veiculo_id"] = tr["numero_ordem_veiculo"].astype(int).astype(str)

    # Enriquecer com chaves de dimensões
    fato_tr = (
        tr
        .merge(dim_linha, on="linha", how="left")
        .merge(dim_veic, on="veiculo_id", how="left")
    )

    # Selecionar colunas finais
    fato_tr = fato_tr[[
        "data_key",
        "linha_key",
        "veiculo_key",
        "codigo_evento",
        "data_hora",
        "latitude",
        "longitude",
        "velocidade_instantanea",
        "direcao_veiculo",
        "sentido_veiculo",
        "distancia_percorrida",
        "dt_ingestao",
    ]].copy()

    # Adicionar incrementalmente por dia
    fato_tr = fato_tr[fato_tr["data_key"].isin(dim_data["data_key"])]
    fato_tr_out = incremental_append_by_data_key(fato_tr, FATO_TR, "data_key")
    fato_tr_out.to_parquet(FATO_TR, index=False)

    # Exibir resumo
    print("Gold: fatos gerados com sucesso.")
    print(f"- {FATO_MCO}")
    print(f"- {FATO_TR}")


if __name__ == "__main__":
    main()
