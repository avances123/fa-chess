import pytest
import os
import polars as pl
from src.core.db_manager import DBManager
from src.core.app_db import AppDBManager
from src.config import GAME_SCHEMA

@pytest.fixture
def parquet_file(tmp_path):
    path = tmp_path / "test_db.parquet"
    df = pl.DataFrame([
        {
            "id": 1, "date": "2024.01.01", "white": "Carlsen", "w_elo": 2850,
            "black": "Caruana", "b_elo": 2830, "result": "1-0", "event": "WCC",
            "site": "London", "line": "e4 e5", "full_line": "e2e4 e7e5", "fens": [1, 2, 3]
        }
    ], schema=GAME_SCHEMA)
    df.write_parquet(path)
    return str(path)

def test_db_manager_load_and_filter(parquet_file):
    db = DBManager()
    name = db.load_parquet(parquet_file)
    assert name == os.path.basename(parquet_file)
    assert db.get_active_count() == 1
    
    # Probar filtro
    criteria = {"white": "Carlsen"}
    res = db.filter_db(criteria)
    assert res.height == 1
    
    criteria = {"white": "Kasparov"}
    res = db.filter_db(criteria)
    assert res.height == 0

def test_db_manager_add_and_save(parquet_file):
    db = DBManager()
    name = db.load_parquet(parquet_file)
    db.set_readonly(name, False)
    
    new_game = {
        "id": 2, "date": "2024.01.02", "white": "Nakamura", "w_elo": 2800,
        "black": "Nepomniachtchi", "b_elo": 2790, "result": "1/2-1/2", "event": "Speed",
        "site": "Online", "line": "d4 d5", "full_line": "d2d4 d7d5", "fens": [4, 5, 6]
    }
    db.add_game(name, new_game)
    assert db.get_active_count() == 2
    assert db.is_dirty(name)
    
    db.save_active_db()
    assert not db.is_dirty(name)
    
    # Verificar recarga
    db2 = DBManager()
    db2.load_parquet(parquet_file)
    assert db2.get_active_count() == 2

@pytest.fixture
def app_db(tmp_path):
    db_path = tmp_path / "app_test.db"
    return AppDBManager(str(db_path))

def test_app_db_config(app_db):
    app_db.set_config("test_key", {"a": 1})
    assert app_db.get_config("test_key") == {"a": 1}
    assert app_db.get_config("non_existent", "default") == "default"

def test_app_db_opening_cache(app_db):
    df = pl.DataFrame({"uci": ["e2e4"], "c": [10]})
    app_db.save_opening_stats("path/to/db", 12345, df, engine_eval=0.5)
    
    cached_df, eval_val = app_db.get_opening_stats("path/to/db", 12345)
    assert cached_df.height == 1
    assert eval_val == 0.5
    
    app_db.update_opening_eval("path/to/db", 12345, 0.75)
    _, eval_val = app_db.get_opening_stats("path/to/db", 12345)
    assert eval_val == 0.75
