import pytest
import polars as pl
import os
from src.core.db_manager import DBManager
from src.config import GAME_SCHEMA

def test_db_manager_delete_disk(tmp_path):
    path = tmp_path / "delete.parquet"
    pl.DataFrame(schema=GAME_SCHEMA).write_parquet(path)
    db = DBManager()
    name = db.load_parquet(str(path))
    
    assert os.path.exists(path)
    db.delete_database_from_disk(name)
    assert not os.path.exists(path)

def test_db_manager_reference_logic(tmp_path):
    path = tmp_path / "ref.parquet"
    pl.DataFrame(schema=GAME_SCHEMA).write_parquet(path)
    db = DBManager()
    db.load_parquet(str(path))
    
    db.set_reference_db("Base Activa")
    assert db.reference_db_name is None
    
    db.set_reference_db("Other")
    assert db.reference_db_name == "Other"
