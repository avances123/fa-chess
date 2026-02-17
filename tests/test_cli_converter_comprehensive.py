import pytest
import os
import polars as pl
import chess
from src.converter import convert_pgn_to_parquet
from src.config import GAME_SCHEMA

def test_full_pgn_conversion_flow(tmp_path):
    # 1. Crear un PGN real con múltiples partidas y metadatos
    pgn_content = """[Event "Test Event"]
[Site "Test Site"]
[Date "2024.01.01"]
[Round "1"]
[White "Player White"]
[Black "Player Black"]
[Result "1-0"]
[WhiteElo "2500"]
[BlackElo "2400"]

1. e4 e5 2. Nf3 Nc6 3. Bb5 a6 4. Ba4 Nf6 1-0

[Event "Test Event 2"]
[Site "Test Site 2"]
[Date "2024.01.02"]
[Round "2"]
[White "White 2"]
[Black "Black 2"]
[Result "0-1"]
[WhiteElo "2000"]
[BlackElo "2100"]

1. d4 d5 2. c4 c6 3. Nf3 Nf6 0-1
"""
    pgn_path = tmp_path / "test.pgn"
    pgn_path.write_text(pgn_content)
    
    output_path = tmp_path / "output.parquet"
    
    # 2. Ejecutar conversión (quitar max_workers que no es un parámetro de la función)
    convert_pgn_to_parquet(str(pgn_path), str(output_path))
    
    # 3. Verificar resultados
    assert os.path.exists(output_path)
    df = pl.read_parquet(output_path)
    
    assert df.height == 2
    assert "white" in df.columns
    assert df.filter(pl.col("white") == "Player White").height == 1
    
    # Verificar que los FENs se han generado
    assert "fens" in df.columns
    first_game_fens = df.row(0, named=True)["fens"]
    assert len(first_game_fens) > 5

def test_converter_invalid_pgn(tmp_path):
    # PGN corrupto
    bad_pgn = tmp_path / "bad.pgn"
    bad_pgn.write_text("Not a chess game at all")
    
    output_path = tmp_path / "bad.parquet"
    convert_pgn_to_parquet(str(bad_pgn), str(output_path))
    
    # Debería crear un archivo vacío o no crearlo
    if os.path.exists(output_path):
        df = pl.read_parquet(output_path)
        assert df.height == 0

def test_cli_execution(monkeypatch, tmp_path):

    import sys

    from src.cli import main

    

    pgn_path = tmp_path / "cli.pgn"

    # El conversor busca b"[Event " para contar partidas

    pgn_path.write_text('[Event "Test"]\n[White "W"]\n[Black "B"]\n[Result "1-0"]\n\n1. e4 1-0\n')



    

    out_path = tmp_path / "cli.parquet"

    

    # Mockear argumentos

    monkeypatch.setattr(sys, "argv", ["src/cli.py", str(pgn_path), str(out_path)])

    

    main()

    

    assert os.path.exists(out_path)

    df = pl.read_parquet(out_path)

    assert df.height == 1




