import os
import pytest
import polars as pl
from src.core.db_manager import DBManager, GAME_SCHEMA

@pytest.fixture
def db_manager(tmp_path, monkeypatch):
    """Fixture que crea un DBManager con rutas temporales."""
    test_clipbase = str(tmp_path / "test_clipbase.parquet")
    # Forzamos la ruta de la clipbase a una temporal
    monkeypatch.setattr("src.core.db_manager.CLIPBASE_FILE", test_clipbase)
    
    manager = DBManager()
    test_db_path = str(tmp_path / "test_database.parquet")
    manager.create_new_database(test_db_path)
    return manager, test_db_path

def test_add_and_delete_game(db_manager):
    manager, _ = db_manager
    manager.set_active_db("Clipbase")
    
    game_data = {
        "id": 1,
        "white": "Carlsen",
        "black": "Caruana",
        "w_elo": 2850,
        "b_elo": 2830,
        "result": "1-0",
        "date": "2024.01.01",
        "event": "Wijk aan Zee",
        "site": "Netherlands",
        "line": "e2e4",
        "full_line": "e2e4 e7e5",
        "fens": [123456, 789012] 
    }
    
    manager.add_to_clipbase(game_data)
    assert manager.get_active_count() == 1
    
    manager.delete_game("Clipbase", 1)
    assert manager.get_active_count() == 0

def test_filter_games(db_manager):
    manager, _ = db_manager
    manager.set_active_db("Clipbase")
    
    # Insertar partidas con tipos consistentes
    for i in range(1, 4):
        g = {k: (None if GAME_SCHEMA[k] in [pl.Int64, pl.UInt64] else "") for k in GAME_SCHEMA.keys()}
        g.update({
            "id": i,
            "white": f"Player {i}",
            "black": "Opponent",
            "w_elo": 2700 + i*10,
            "fens": [1000 * i]
        })
        manager.add_to_clipbase(g)

    # Filtrar por blanca
    res = manager.filter_db({"white": "Player 1"})
    assert res.height == 1
    
    # Filtrar por ELO
    res = manager.filter_db({"min_elo": 2725})
    assert res.height == 1 # Solo Player 3 (2730)

def test_update_game(db_manager):
    manager, _ = db_manager
    manager.set_active_db("Clipbase")
    
    game = {k: (None if GAME_SCHEMA[k] in [pl.Int64, pl.UInt64] else "") for k in GAME_SCHEMA.keys()}
    game["fens"] = []
    game.update({"id": 10, "white": "Tal", "w_elo": 2700})
    manager.add_to_clipbase(game)
    
    manager.update_game("Clipbase", 10, {"w_elo": 2800})
    updated = manager.get_game_by_id("Clipbase", 10)
    assert updated["w_elo"] == 2800

def test_create_and_delete_db(db_manager):
    manager, path = db_manager
    name = os.path.basename(path)
    assert os.path.exists(path)
    manager.delete_database_from_disk(name)
    assert not os.path.exists(path)