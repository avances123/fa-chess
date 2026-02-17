import pytest
import os
import polars as pl
import chess
from src.core.db_manager import DBManager
from src.core.workers import StatsWorker
from src.config import GAME_SCHEMA

def test_db_manager_save_error(tmp_path, monkeypatch):
    path = tmp_path / "error.parquet"
    pl.DataFrame(schema=GAME_SCHEMA).write_parquet(path)
    db = DBManager()
    db.load_parquet(str(path))
    db.set_readonly(db.active_db_name, False)
    db.set_dirty(db.active_db_name, True)
    
    # Mockear write_parquet para que falle
    def mock_fail(*args, **kwargs): raise Exception("Simulated disk error")
    monkeypatch.setattr(pl.DataFrame, "write_parquet", mock_fail)
    
    with pytest.raises(Exception):
        db.save_active_db()

def test_stats_worker_empty_hash(qapp):
    db = DBManager()
    worker = StatsWorker(db, "", True, current_hash=None)
    # Ejecutar run directamente para cubrir la rama del if not self.current_hash
    worker.run()
    # No debería explotar y emitir None

def test_db_manager_filter_edge_cases(tmp_path):
    path = tmp_path / "edge.parquet"
    pl.DataFrame([{"id": 1, "white": "W", "black": "B", "w_elo": 2000, "b_elo": 1800, "result": "1-0", "date": "2024.01.01", "event": "E", "site": "S", "line": "", "full_line": "", "fens": [1]}], schema=GAME_SCHEMA).write_parquet(path)
    db = DBManager()
    db.load_parquet(str(path))
    
    # Filtro por Elo
    db.filter_db({"min_elo": "1900"})
    assert db.current_view_count == 1
    
    # Filtro por fecha
    db.filter_db({"date_from": "2024.01.01", "date_to": "2024.01.01"})
    assert db.current_view_count == 1
    
    # Filtro por resultado
    db.filter_db({"result": "1-0"})
    assert db.current_view_count == 1
