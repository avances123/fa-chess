import pytest
import os
import polars as pl
from src.converter import convert_pgn_to_parquet, extract_game_data
from src.config import GAME_SCHEMA

def test_extract_game_data():
    pgn_text = """[Event "Test"]
[Site "Earth"]
[Date "2026.02.16"]
[Round "1"]
[White "Fabio"]
[Black "Gemini"]
[Result "1-0"]
[WhiteElo "2500"]
[BlackElo "2400"]

1. e4 e5 2. Nf3 *"""
    import io
    import chess.pgn
    game = chess.pgn.read_game(io.StringIO(pgn_text))
    
    data = extract_game_data(1, game)
    assert data["white"] == "Fabio"
    assert data["w_elo"] == 2500
    assert data["result"] == "1-0"
    assert len(data["fens"]) == 4 # Initial + 3 moves

def test_convert_pgn_to_parquet(tmp_path):
    pgn_path = tmp_path / "test.pgn"
    parquet_path = tmp_path / "test.parquet"
    
    pgn_content = """[Event "E"]
[White "W"]
[Black "B"]
[Result "1/2-1/2"]

1. d4 d5 1/2-1/2"""
    
    with open(pgn_path, "w") as f:
        f.write(pgn_content)
        
    convert_pgn_to_parquet(str(pgn_path), str(parquet_path))
    
    assert os.path.exists(parquet_path)
    df = pl.read_parquet(parquet_path)
    assert df.height == 1
    assert df["white"][0] == "W"
