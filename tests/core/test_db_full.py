import pytest
import polars as pl
from src.core.db_manager import DBManager
from src.config import GAME_SCHEMA

def test_db_manager_reload_and_full(tmp_path):
    path = tmp_path / "reload.parquet"
    df = pl.DataFrame([{"id": 1, "white": "A", "black": "B", "w_elo": 0, "b_elo": 0, "result": "*", "date": "2024", "event": "E", "site": "S", "line": "", "full_line": "", "fens": []}], schema=GAME_SCHEMA)
    df.write_parquet(path)
    
    db = DBManager()
    db.load_parquet(str(path))
    name = db.active_db_name
    
    # Simular cambio en disco
    df2 = pl.DataFrame([
        {"id": 1, "white": "A", "black": "B", "w_elo": 0, "b_elo": 0, "result": "*", "date": "2024", "event": "E", "site": "S", "line": "", "full_line": "", "fens": []},
        {"id": 2, "white": "C", "black": "D", "w_elo": 0, "b_elo": 0, "result": "*", "date": "2024", "event": "E", "site": "S", "line": "", "full_line": "", "fens": []}
    ], schema=GAME_SCHEMA)
    df2.write_parquet(path)
    
    db.reload_db(name)
    assert db.get_active_count() == 2

def test_get_game_by_id(tmp_path):
    path = tmp_path / "id.parquet"
    df = pl.DataFrame([{"id": 999, "white": "Target", "black": "B", "w_elo": 0, "b_elo": 0, "result": "*", "date": "2024", "event": "E", "site": "S", "line": "", "full_line": "", "fens": []}], schema=GAME_SCHEMA)
    df.write_parquet(path)
    
    db = DBManager()
    db.load_parquet(str(path))
    game = db.get_game_by_id(db.active_db_name, 999)
    assert game["white"] == "Target"

def test_delete_filtered(tmp_path):
    path = tmp_path / "filter.parquet"
    df = pl.DataFrame([
        {"id": 1, "white": "X", "black": "B", "w_elo": 0, "b_elo": 0, "result": "1-0", "date": "2024", "event": "E", "site": "S", "line": "", "full_line": "", "fens": []},
        {"id": 2, "white": "Y", "black": "B", "w_elo": 0, "b_elo": 0, "result": "0-1", "date": "2024", "event": "E", "site": "S", "line": "", "full_line": "", "fens": []}
    ], schema=GAME_SCHEMA)
    df.write_parquet(path)
    
    db = DBManager()
    db.load_parquet(str(path))
    db.filter_db({"white": "X"})
    db.delete_filtered_games()
    assert db.get_active_count() == 1
    assert db.get_active_df()["white"][0] == "Y"
