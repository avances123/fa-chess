import pytest
import os
import chess.pgn
import io
from src.converter import convert_pgn_to_parquet

def test_converter_basic(tmp_path):
    pgn_content = """[Event "Test"]
[Site "Online"]
[Date "2024.01.01"]
[Round "1"]
[White "Player1"]
[Black "Player2"]
[Result "1-0"]

1. e4 e5 2. Nf3 Nc6 1-0
"""
    pgn_path = tmp_path / "test.pgn"
    pgn_path.write_text(pgn_content)
    
    out_path = tmp_path / "test.parquet"
    # La función acepta parámetros directos
    convert_pgn_to_parquet(str(pgn_path), str(out_path))
    
    assert os.path.exists(out_path)
    assert os.path.getsize(out_path) > 100

def test_cli_entrypoint(tmp_path, monkeypatch):
    pgn_content = '[Event "Test"]\n[Result "*"]\n\n1. e4 *'
    pgn_path = tmp_path / "test.pgn"
    pgn_path.write_text(pgn_content)
    out_path = tmp_path / "out.parquet"
    
    import sys
    from src.cli import main
    
    # Simular argumentos
    monkeypatch.setattr(sys, "argv", ["fa-chess-convert", str(pgn_path), str(out_path)])
    
    # Ejecutar main
    main()
    assert os.path.exists(out_path)
