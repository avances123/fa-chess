import pytest
import polars as pl
from src.core.workers import StatsWorker
from src.core.db_manager import DBManager
from src.config import GAME_SCHEMA

@pytest.fixture
def db_with_games(tmp_path):
    path = tmp_path / "stats_test.parquet"
    df = pl.DataFrame([
        {
            "id": 1, "date": "2024.01.01", "white": "W", "w_elo": 2000,
            "black": "B", "b_elo": 2000, "result": "1-0", "event": "E",
            "site": "S", "line": "e4", "full_line": "e2e4", "fens": [1, 2]
        }
    ], schema=GAME_SCHEMA)
    df.write_parquet(path)
    db = DBManager()
    db.load_parquet(str(path))
    return db

def test_stats_worker(db_with_games, qtbot):
    # Corregido: current_hash en lugar de current_pos_hash
    worker = StatsWorker(db_with_games, current_line_uci="", is_white_turn=True, current_hash=1)
    
    with qtbot.waitSignal(worker.finished, timeout=5000) as blocker:
        worker.start()
    
    res_df, engine_eval = blocker.args
    assert res_df is not None
    assert "e2e4" in res_df["uci"].to_list()
