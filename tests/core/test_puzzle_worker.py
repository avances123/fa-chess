import pytest
import polars as pl
import os
from src.core.workers import PuzzleGeneratorWorker
from src.config import GAME_SCHEMA

def test_puzzle_generator_worker(tmp_path, qtbot):
    db_path = tmp_path / "games.parquet"
    # Partida con mate de loco
    df = pl.DataFrame([{
        "id": 1, "white": "W", "black": "B", "w_elo": 0, "b_elo": 0, "result": "1-0",
        "date": "2024", "event": "E", "site": "S", "line": "", "full_line": "f2f3 e7e5 g2g4 d8h4", "fens": []
    }], schema=GAME_SCHEMA)
    df.write_parquet(db_path)
    
    out_path = tmp_path / "gen_puzzles.parquet"
    worker = PuzzleGeneratorWorker(str(db_path), str(out_path))
    
    with qtbot.waitSignal(worker.finished, timeout=5000):
        worker.start()
    
    assert os.path.exists(out_path)
