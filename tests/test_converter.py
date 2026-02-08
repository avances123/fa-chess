import pytest
import polars as pl
import chess
import os
from src.converter import extract_game_data, convert_pgn_to_parquet

def test_extract_game_data_integrity():
    """Verifica que la extracción de datos captura correctamente hashes y metadatos."""
    pgn_text = """[White "Carlsen, Magnus"]
[Black "Nepomniachtchi, Ian"]
[Result "1-0"]
[WhiteElo "2850"]

1. e4 e5 2. Nf3 Nc6 1-0"""
    import io
    import chess.pgn
    game = chess.pgn.read_game(io.StringIO(pgn_text))
    
    data = extract_game_data(99, game)
    
    assert data["id"] == 99
    assert data["white"] == "Carlsen, Magnus"
    assert data["w_elo"] == 2850
    assert data["result"] == "1-0"
    # Verificar hashes: posición inicial + 4 movimientos = 5 hashes
    assert len(data["fens"]) == 5
    assert isinstance(data["fens"][0], int) # Zobrist hash debe ser entero

def test_convert_pgn_to_parquet_flow(tmp_path, sample_pgn):
    """Prueba el flujo completo de conversión de archivo PGN a Parquet."""
    output_parquet = str(tmp_path / "output.parquet")
    
    # Ejecutar conversión
    convert_pgn_to_parquet(sample_pgn, output_parquet)
    
    assert os.path.exists(output_parquet)
    df = pl.read_parquet(output_parquet)
    assert df.height == 2
    assert "fens" in df.columns
    assert df["white"][0] == "Player 1"
