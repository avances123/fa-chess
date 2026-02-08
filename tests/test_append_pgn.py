import pytest
import os
import polars as pl
from src.core.workers import PGNAppendWorker

def test_pgn_append_flow(temp_env, sample_parquet, sample_pgn):
    """
    Verifica que añadir un PGN a una base Parquet existente funciona,
    mantiene la integridad de los datos y ajusta los IDs correlativamente.
    """
    # 1. Verificar estado inicial
    old_df = pl.read_parquet(sample_parquet)
    assert old_df.height == 2

    # 2. Ejecutar lógica del worker de forma síncrona para el test
    worker = PGNAppendWorker(sample_pgn, sample_parquet)
    worker.run() 

    # 3. Verificar resultados tras la fusión
    assert os.path.exists(sample_parquet)
    combined_df = pl.read_parquet(sample_parquet)
    
    # original(2) + nuevo(2) = 4
    assert combined_df.height == 4
    
    # IDs únicos y correlativos
    ids = combined_df["id"].to_list()
    assert len(set(ids)) == 4
    assert max(ids) == 4
    
    # Jugadores presentes
    whites = combined_df["white"].to_list()
    assert "Player 1" in whites
    assert "Carlsen" in whites