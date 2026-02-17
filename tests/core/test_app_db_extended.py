import pytest
import chess
from src.core.app_db import AppDBManager

@pytest.fixture
def app_db(tmp_path):
    return AppDBManager(str(tmp_path / "app.db"))

def test_app_db_puzzle_methods(app_db):
    app_db.save_puzzle_status("p1", "success")
    stats = app_db.get_all_puzzle_stats()
    assert stats["p1"] == "success"
    
    app_db.set_tactical_elo(1500)
    assert app_db.get_tactical_elo() == 1500
