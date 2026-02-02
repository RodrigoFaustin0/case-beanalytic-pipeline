import pandas as pd
from pathlib import Path

GOLD = Path("data/gold/mobilidade_bh")

def test_dim_data_data_key_unica():
    df = pd.read_parquet(GOLD / "dim_data.parquet")
    assert df["data_key"].is_unique

def test_dim_linha_sem_linha_nula():
    df = pd.read_parquet(GOLD / "dim_linha.parquet")
    assert df["linha"].notna().all()

def test_fato_mco_tem_data_key():
    df = pd.read_parquet(GOLD / "fato_mco_viagem.parquet")
    assert df["data_key"].notna().all()

def test_fato_mco_linha_key_existe_na_dim_linha():
    fato = pd.read_parquet(GOLD / "fato_mco_viagem.parquet")
    dim = pd.read_parquet(GOLD / "dim_linha.parquet")

    assert set(fato["linha_key"].dropna()).issubset(set(dim["linha_key"]))

def test_fato_tempo_real_lat_long_validas():
   
    df = pd.read_parquet(GOLD / "fato_tempo_real_evento.parquet")

    if "latitude" in df.columns:
        assert df["latitude"].dropna().between(-90, 90).all()

    if "longitude" in df.columns:
        assert df["longitude"].dropna().between(-180, 180).all()
