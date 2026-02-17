import pytest
from unittest.mock import MagicMock, patch
import polars as pl
import os
import time
from src.core.db_manager import DBManager
from src.config import GAME_SCHEMA

@pytest.fixture
def db_manager():
    manager = DBManager()
    return manager

def test_db_manager_basic_ops(db_manager):
    path = "tests/data/ops.parquet"
    pl.DataFrame(schema=GAME_SCHEMA).write_parquet(path)
    db_manager.load_parquet(path)
    
    assert db_manager.active_db_name == "ops.parquet"
    assert db_manager.get_active_count() == 0
    
    # Probar readonly
    db_manager.set_readonly("ops.parquet", False)
    assert db_manager.db_metadata["ops.parquet"]["read_only"] == False
    
    # Probar dirty
    db_manager.set_dirty("ops.parquet", True)
    assert db_manager.is_dirty("ops.parquet")
    
    os.remove(path)

def test_db_manager_reload(db_manager):
    path = "tests/data/reload.parquet"
    pl.DataFrame(schema=GAME_SCHEMA).write_parquet(path)
    db_manager.load_parquet(path)
    
    # Simular cambio en disco
    pl.DataFrame([{"id": 1, "white": "W", "black": "B", "w_elo": 0, "b_elo": 0, "result": "*", "date": "D", "event": "E", "site": "S", "line": "", "full_line": "", "fens": []}], schema=GAME_SCHEMA).write_parquet(path)
    
    db_manager.reload_db("reload.parquet")
    assert db_manager.get_active_count() == 1
    os.remove(path)

def test_db_manager_delete_filtered(db_manager):
    df = pl.DataFrame([
        {"id": 1, "white": "X", "black": "Y", "w_elo": 0, "b_elo": 0, "result": "*", "date": "D", "event": "E", "site": "S", "line": "", "full_line": "", "fens": []},
        {"id": 2, "white": "A", "black": "B", "w_elo": 0, "b_elo": 0, "result": "*", "date": "D", "event": "E", "site": "S", "line": "", "full_line": "", "fens": []}
    ], schema=GAME_SCHEMA).lazy()
    
    db_manager.dbs["test"] = df
    db_manager.db_metadata["test"] = {"path": "test.p", "read_only": False}
    db_manager.active_db_name = "test"
    
    # Filtrar solo uno
    db_manager.filter_db({"white": "X"})
    db_manager.delete_filtered_games()
    
    assert db_manager.get_active_count() == 1
    assert db_manager.get_active_df()["white"][0] == "A"

def test_db_manager_player_report(db_manager):
    df = pl.DataFrame([
        {"id": 1, "white": "Fabio", "black": "Gemini", "w_elo": 2500, "b_elo": 2000, "result": "1-0", "date": "2026.01.01", "event": "E", "site": "S", "line": "e4", "full_line": "e2e4", "fens": []},
        {"id": 2, "white": "Gemini", "black": "Fabio", "w_elo": 2000, "b_elo": 2500, "result": "1/2-1/2", "date": "2026.01.02", "event": "E", "site": "S", "line": "d4", "full_line": "d2d4", "fens": []}
    ], schema=GAME_SCHEMA).lazy()
    db_manager.dbs["test"] = df
    db_manager.active_db_name = "test"
    db_manager.reset_to_full_base()
    
    report = db_manager.get_player_report("Fabio")
    assert report["name"] == "Fabio"
    assert report["as_white"]["w"] == 1
    assert report["as_black"]["d"] == 1
    assert len(report["elo_history"]) == 2

def test_db_manager_delete_db_file(db_manager):
    path = "tests/data/to_delete.parquet"
    pl.DataFrame(schema=GAME_SCHEMA).write_parquet(path)
    db_manager.load_parquet(path)
    
    db_manager.delete_database_from_disk("to_delete.parquet")
    assert not os.path.exists(path)
    assert "to_delete.parquet" not in db_manager.dbs
