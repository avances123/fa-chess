import pytest
import os
import polars as pl
from src.core.db_manager import DBManager
from src.config import GAME_SCHEMA

@pytest.fixture
def complex_db(tmp_path):
    path = tmp_path / "complex.parquet"
    games = [
        {"id": 1, "date": "2024.01.01", "white": "Carlsen", "w_elo": 2850, "black": "Caruana", "b_elo": 2830, "result": "1-0", "event": "E", "site": "S", "line": "e4", "full_line": "e2e4", "fens": [1, 2]},
        {"id": 2, "date": "2024.01.02", "white": "Nakamura", "w_elo": 2800, "black": "Carlsen", "b_elo": 2850, "result": "0-1", "event": "E", "site": "S", "line": "d4", "full_line": "d2d4", "fens": [3, 4]}
    ]
    df = pl.DataFrame(games, schema=GAME_SCHEMA)
    df.write_parquet(path)
    db = DBManager()
    db.load_parquet(str(path))
    return db

def test_player_report(complex_db):
    report = complex_db.get_player_report("Carlsen")
    assert report is not None
    assert report["name"] == "Carlsen"
    assert report["as_white"]["w"] == 1
    assert report["as_black"]["w"] == 1 # Ganó de negras contra Nakamura

def test_delete_game(complex_db):
    name = complex_db.active_db_name
    complex_db.delete_game(name, 1)
    assert complex_db.get_active_count() == 1
    assert complex_db.is_dirty(name)

def test_sort_db(complex_db):
    complex_db.sort_active_db("date", descending=True)
    df = complex_db.get_active_df()
    assert df["date"][0] == "2024.01.02"
