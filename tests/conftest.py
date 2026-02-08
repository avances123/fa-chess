import pytest
import os
import polars as pl
import tempfile
from src.core.db_manager import GAME_SCHEMA

@pytest.fixture
def temp_env(tmp_path, monkeypatch):
    """Configura un entorno temporal para evitar tocar archivos reales del usuario."""
    config_dir = tmp_path / ".config"
    config_dir.mkdir()
    config_file = config_dir / "fa-chess-test.json"
    clipbase_file = config_dir / "test-clipbase.parquet"
    
    # Inyectamos las rutas en los módulos correspondientes
    monkeypatch.setattr("src.config.CONFIG_FILE", str(config_file))
    monkeypatch.setattr("src.core.db_manager.CLIPBASE_FILE", str(clipbase_file))
    
    return {
        "config": str(config_file),
        "clipbase": str(clipbase_file),
        "tmp_dir": str(tmp_path)
    }

@pytest.fixture
def sample_pgn(tmp_path):
    """Crea un archivo PGN de prueba con 2 partidas."""
    pgn_path = tmp_path / "test.pgn"
    content = """[Event "Test Tournament"]
[Site "Testing"]
[Date "2024.01.01"]
[Round "1"]
[White "Player 1"]
[Black "Player 2"]
[Result "1-0"]
[WhiteElo "2500"]
[BlackElo "2400"]

1. e4 e5 2. Nf3 Nc6 3. Bb5 a6 4. Ba4 Nf6 1-0

[Event "Test Tournament"]
[Site "Testing"]
[Date "2024.01.02"]
[Round "2"]
[White "Player 2"]
[Black "Player 1"]
[Result "0-1"]
[WhiteElo "2400"]
[BlackElo "2500"]

1. d4 d5 2. c4 c6 3. Nf3 Nf6 0-1
"""
    pgn_path.write_text(content)
    return str(pgn_path)

@pytest.fixture
def sample_parquet(tmp_path):
    """Crea una base de datos Parquet de prueba con el esquema correcto."""
    path = tmp_path / "test_base.parquet"
    data = [
        {
            "id": 1, "white": "Carlsen", "black": "Nepo", 
            "w_elo": 2850, "b_elo": 2780, "result": "1-0",
            "date": "2021.12.01", "event": "World Match", "site": "Dubai",
            "line": "e2e4 e7e5 Nf3 Nc6 Bb5", "full_line": "e2e4 e7e5 Nf3 Nc6 Bb5 a6 Ba4", 
            "fens": [100, 200, 300]
        },
        {
            "id": 2, "white": "Kasparov", "black": "Karpov", 
            "w_elo": 2800, "b_elo": 2750, "result": "1/2-1/2",
            "date": "1985.01.01", "event": "World Match", "site": "Moscow",
            "line": "d2d4 d7d5 c2c4 c7c6", "full_line": "d2d4 d7d5 c2c4 c7c6 Nf3 Nf6", 
            "fens": [400, 500, 600]
        }
    ]
    # Forzamos el esquema al crear el DataFrame de prueba
    df = pl.DataFrame(data, schema=GAME_SCHEMA)
    df.write_parquet(str(path))
    return str(path)
