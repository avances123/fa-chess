import pytest
import chess
import polars as pl
from src.core.opening_service import OpeningService
from src.core.db_manager import DBManager, GAME_SCHEMA

def test_opening_service_flow(qtbot, tmp_path):
    # 1. Setup DB mínima
    db_path = tmp_path / "test.parquet"
    pl.DataFrame({
        "id": [1], "date": ["?"], "event": ["?"], "site": ["?"],
        "white": ["A"], "w_elo": [2000], "black": ["B"], "b_elo": [2000],
        "result": ["1-0"], "line": [""], "full_line": ["e2e4"], "fens": [[123]]
    }, schema=GAME_SCHEMA).write_parquet(db_path)

    db = DBManager()
    db.load_parquet(str(db_path))

    # 2. Setup Service
    service = OpeningService(db)
    board = chess.Board()

    # 3. Request stats y esperar señal
    with qtbot.waitSignal(service.stats_ready, timeout=2000) as blocker:
        service.request_stats(board)

    # Ahora la señal devuelve 3 valores: stats_df, name, eval_msg
    stats_df, name, eval_msg = blocker.args
    assert name != "Variante Desconocida"
    assert isinstance(stats_df, pl.DataFrame)
