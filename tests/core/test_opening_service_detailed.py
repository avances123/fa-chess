import pytest
import chess
import polars as pl
from src.core.opening_service import OpeningService, format_chess_score
from src.core.db_manager import DBManager

def test_format_chess_score():
    assert format_chess_score(0.5) == "+0.50"
    assert format_chess_score(-1.2) == "-1.20"
    assert format_chess_score(9999) == "M1"
    assert format_chess_score(-9998) == "-M2"
    assert format_chess_score(None) == ""

def test_opening_service_live_signals(qtbot):
    db = DBManager()
    service = OpeningService(db)
    
    results = []
    service.tree_eval_ready.connect(lambda uci, score: results.append((uci, score)))
    
    # Emitimos la señal directamente (simulando al worker)
    service.tree_eval_ready.emit("e2e4", "+0.55")
    
    assert len(results) == 1
    assert results[0] == ("e2e4", "+0.55")

def test_opening_service_stats_flow(qtbot):
    db = DBManager()
    service = OpeningService(db)
    df = pl.DataFrame({"uci": ["e2e4"], "c": [10]})
    
    with qtbot.waitSignal(service.stats_ready) as blocker:
        # El valor 0.5 debe formatearse como +0.50
        service._on_stats_worker_finished(df, 0.5, "Apertura")
    
    assert blocker.args[1] == "Apertura"
    assert blocker.args[2] == "+0.50"
