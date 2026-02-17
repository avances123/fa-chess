import pytest
import chess
import os
from src.core.workers import PGNWorker, StatsWorker, PGNExportWorker, PGNAppendWorker
from src.core.db_manager import DBManager
import polars as pl
from src.config import GAME_SCHEMA

@pytest.fixture
def temp_pgn(tmp_path):
    path = tmp_path / "test.pgn"
    path.write_text('[Event "T"]\n[White "W"]\n[Black "B"]\n[Result "1-0"]\n\n1. e4 1-0\n')
    return str(path)

def test_pgn_worker(temp_pgn, qtbot):
    worker = PGNWorker(temp_pgn)
    with qtbot.waitSignal(worker.finished, timeout=5000) as blocker:
        worker.start()
    assert blocker.args[0].endswith(".parquet")

def test_pgn_export_worker(tmp_path, qtbot):
    out_pgn = tmp_path / "out.pgn"
    df = pl.DataFrame([{"id": 1, "white": "W", "black": "B", "w_elo": 0, "b_elo": 0, "result": "1-0", "date": "2024", "event": "E", "site": "S", "line": "", "full_line": "e2e4", "fens": []}], schema=GAME_SCHEMA)
    
    worker = PGNExportWorker(df, str(out_pgn))
    with qtbot.waitSignal(worker.finished, timeout=5000):
        worker.start()
    assert os.path.exists(out_pgn)

def test_pgn_append_worker(tmp_path, temp_pgn, qtbot):
    target_parquet = tmp_path / "target.parquet"
    pl.DataFrame(schema=GAME_SCHEMA).write_parquet(target_parquet)
    
    # Corregido: solo 2 argumentos
    worker = PGNAppendWorker(str(temp_pgn), str(target_parquet))
    with qtbot.waitSignal(worker.finished, timeout=5000):
        worker.start()
    
    # Verificar que el archivo creció
    df = pl.read_parquet(target_parquet)
    assert df.height == 1
