import pytest
import chess
import polars as pl
from src.core.opening_service import OpeningService
from src.core.db_manager import DBManager

def test_opening_service_injects_legal_moves_on_empty_db(qtbot):
    db = DBManager() # DB vacía por defecto
    service = OpeningService(db)
    
    board = chess.Board() # Posición inicial
    
    # Configuramos el last_request manualmente para simular el flujo
    service.last_request = {
        "board_copy": board,
        "fen": board.fen()
    }
    
    captured_df = None
    def on_stats(df, name, ev):
        nonlocal captured_df
        captured_df = df
        
    service.stats_ready.connect(on_stats)
    
    # Simulamos que el StatsWorker termina con un DF vacío
    service._on_stats_worker_finished(pl.DataFrame(), None, "Initial Position")
    
    assert captured_df is not None
    assert not captured_df.is_empty()
    assert captured_df.height > 0
    assert "uci" in captured_df.columns
    # La posición inicial tiene 20 movimientos legales, capturamos hasta 15
    assert captured_df.height <= 20
    assert len(captured_df["uci"].to_list()) > 0
    assert captured_df["c"][0] == 0 # Las partidas deben ser 0
