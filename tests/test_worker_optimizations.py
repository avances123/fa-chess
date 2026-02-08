import pytest
import polars as pl
import chess
from src.core.workers import StatsWorker
from unittest.mock import MagicMock

@pytest.fixture
def mock_db():
    db = MagicMock()
    # Simular una vista Lazy con una partida que termina en la posición buscada
    # y otra que continúa
    df = pl.DataFrame({
        "full_line": ["e2e4", "e2e4 e7e5"],
        "fens": [[100, 101], [100, 101, 102]],
        "result": ["1-0", "1/2-1/2"],
        "w_elo": [2500, 2600],
        "b_elo": [2500, 2600]
    })
    db.get_current_view.return_value = df.lazy()
    db.get_cached_stats.return_value = None
    db.get_stats_from_tree.return_value = None
    return db

def test_stats_worker_out_of_bounds_protection(mock_db, qtbot):
    """Verifica que el worker no explota si una partida termina en la posición actual"""
    # Buscamos la posición tras e2e4 (hash 101)
    # Partida 1: Termina ahí (sin jugada siguiente) -> Debe ser ignorada
    # Partida 2: Sigue con e7e5 -> Debe ser contada
    worker = StatsWorker(mock_db, "e2e4", False, current_hash=101)
    
    with qtbot.wait_signal(worker.finished, timeout=2000) as blocker:
        worker.start()
        
    res = blocker.args[0]
    assert res is not None
    # Solo debe haber una fila (la jugada e7e5)
    assert res.height == 1
    assert res.row(0, named=True)["uci"] == "e7e5"
    assert res.row(0, named=True)["c"] == 1
