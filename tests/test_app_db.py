import sqlite3
import pytest
from unittest.mock import MagicMock, patch
import polars as pl
import io
import os
import time
from src.core.app_db import AppDBManager

@pytest.fixture(scope="function")
def app_db(tmp_path):
    db_file = tmp_path / "test_app.db"
    manager = AppDBManager(str(db_file))
    yield manager

def test_app_db_config(app_db):
    app_db.set_config("test_key", {"a": 1})
    val = app_db.get_config("test_key")
    assert val == {"a": 1}
    assert app_db.get_config("non_existent", "default") == "default"

def test_app_db_puzzle_stats(app_db):
    app_db.save_puzzle_status("p1", "success")
    stats = app_db.get_all_puzzle_stats()
    assert stats["p1"] == "success"

def test_app_db_tactical_elo(app_db):
    app_db.set_tactical_elo(1500)
    assert app_db.get_tactical_elo() == 1500
    with app_db.get_connection() as conn:
        conn.execute("DELETE FROM app_config WHERE key = 'tactical_elo'")
    assert app_db.get_tactical_elo() == 1200

def test_app_db_opening_stats(app_db):
    df = pl.DataFrame({"uci": ["e2e4"], "c": [100]})
    app_db.save_opening_stats("db.parquet", "h1", df, 0.5)
    df_res, eval_res = app_db.get_opening_stats("db.parquet", "h1")
    assert df_res is not None
    assert df_res.height == 1
    assert eval_res == 0.5

def test_app_db_update_opening_eval(app_db):
    df = pl.DataFrame({"uci": ["e2e4"], "c": [100]})
    app_db.save_opening_stats("db.parquet", "h1", df, None)
    app_db.update_opening_eval("db.parquet", "h1", -0.75)
    _, eval_res = app_db.get_opening_stats("db.parquet", "h1")
    assert eval_res == -0.75

def test_app_db_error_handling(tmp_path):
    db_file = tmp_path / "error.db"
    manager = AppDBManager(str(db_file))
    with manager.get_connection() as conn:
        conn.execute("DROP TABLE opening_cache")
    manager.save_opening_stats("path", "hash", pl.DataFrame(), None)
    res, ev = manager.get_opening_stats("path", "hash")
    assert res is None
