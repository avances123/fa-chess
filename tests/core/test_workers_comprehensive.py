import pytest
import chess
import polars as pl
import os
from PySide6.QtCore import QThread
from src.core.engine_worker import EngineWorker, FullAnalysisWorker
from src.core.workers import (PGNWorker, StatsWorker, PGNExportWorker, 
                            PGNAppendWorker, PuzzleGeneratorWorker, 
                            CachePopulatorWorker, RefutationWorker)
from src.config import GAME_SCHEMA

# Mock de la respuesta de un motor de ajedrez
class MockAnalysisInfo:
    def __init__(self, cp=50, mate=None):
        self.data = {
            "score": chess.engine.PovScore(chess.engine.Cp(cp) if mate is None else chess.engine.Mate(mate), chess.WHITE),
            "pv": [chess.Move.from_uci("e2e4")],
            "depth": 10,
            "nps": 100000
        }
    def get(self, key, default=None):
        return self.data.get(key, default)

class MockEngine:
    def configure(self, params): pass
    def analyse(self, board, limit): return MockAnalysisInfo()
    def quit(self): pass
    def is_terminated(self): return False

def test_engine_worker_logic(qtbot, monkeypatch):
    # Mockear SimpleEngine para evitar hilos reales bloqueados
    monkeypatch.setattr("chess.engine.SimpleEngine.popen_uci", lambda *args, **kwargs: MockEngine())
    
    worker = EngineWorker(engine_path="fake")
    worker.update_position(chess.STARTING_FEN)
    
    # Probamos el formateo de scores manualmente para cobertura
    score_cp = chess.engine.PovScore(chess.engine.Cp(50), chess.WHITE)
    assert worker._format_score(score_cp, chess.WHITE) == "+0.50"
    
    score_mate = chess.engine.PovScore(chess.engine.Mate(1), chess.WHITE)
    assert worker._format_score(score_mate, chess.WHITE) == "M1"

def test_stats_worker_fixed(qtbot, tmp_path):
    db_path = tmp_path / "stats.parquet"
    # Datos completos para el esquema
    data = {
        "id": [1], "date": ["2024.01.01"], "event": ["?"], "site": ["?"],
        "white": ["A"], "w_elo": [2000], "black": ["B"], "b_elo": [2000],
        "result": ["1-0"], "line": ["e4"], "full_line": ["e2e4"], "fens": [[123]]
    }
    df = pl.DataFrame(data, schema=GAME_SCHEMA)
    df.write_parquet(db_path)
    
    from src.core.db_manager import DBManager
    db = DBManager()
    db.load_parquet(str(db_path))
    
    worker = StatsWorker(db, "", True, current_hash=123)
    # Capturar resultado de la señal
    results = []
    worker.finished.connect(lambda df, ev: results.append(df))
    
    # Ejecutar en el mismo hilo para evitar cuelgues
    worker.run() 
    assert len(results) > 0

def test_pgn_export_worker_fixed(qtbot, tmp_path):
    data = {
        "id": [1], "date": ["2024.01.01"], "event": ["E"], "site": ["S"],
        "white": ["A"], "w_elo": [2000], "black": ["B"], "b_elo": [2000],
        "result": ["1-0"], "line": [""], "full_line": ["e2e4 e7e5"], "fens": [[]]
    }
    df = pl.DataFrame(data, schema=GAME_SCHEMA)
    out_pgn = tmp_path / "export.pgn"
    
    worker = PGNExportWorker(df.lazy(), str(out_pgn))
    worker.run()
    assert os.path.exists(out_pgn)

def test_cache_populator_worker_fixed(qtbot, tmp_path):
    db_path = tmp_path / "cache.parquet"
    data = {
        "id": [1], "date": ["2024.01.01"], "event": [""], "site": [""],
        "white": ["A"], "w_elo": [2000], "black": ["B"], "b_elo": [2000],
        "result": ["1-0"], "line": ["e4"], "full_line": ["e2e4"], "fens": [[123]]
    }
    df = pl.DataFrame(data, schema=GAME_SCHEMA)
    df.write_parquet(db_path)
    
    from src.core.db_manager import DBManager
    from src.core.app_db import AppDBManager
    db = DBManager()
    db.load_parquet(str(db_path))
    app_db = AppDBManager(str(tmp_path / "app.db"))
    
    worker = CachePopulatorWorker(db, app_db, min_games=0)
    worker.run()
