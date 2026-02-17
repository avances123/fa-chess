import pytest
import chess
import polars as pl
from src.core.puzzle_manager import PuzzleManager

@pytest.fixture
def puzzle_file(tmp_path):
    path = tmp_path / "puzzles.parquet"
    # Añadimos OpeningTags ya que prepare_puzzle_data lo requiere
    df = pl.DataFrame({
        "PuzzleId": ["p1", "p2"],
        "FEN": [chess.STARTING_FEN, chess.STARTING_FEN],
        "Moves": ["e2e4 e7e5", "d2d4 d7d5"],
        "Rating": [1000, 1500],
        "Themes": ["mate", "fork"],
        "OpeningTags": ["e4", "d4"]
    })
    df.write_parquet(path)
    return str(path)

def test_puzzle_manager_loading(puzzle_file):
    pm = PuzzleManager(puzzle_file)
    # pm.lf es el lazyframe
    assert pm.lf is not None
    
    p = pm.get_random_puzzle()
    assert p["id"] in ["p1", "p2"]

def test_puzzle_manager_filters(puzzle_file):
    pm = PuzzleManager(puzzle_file)
    pm.apply_filters(min_rating=1200)
    res = pm.get_sample()
    assert res.height == 1
    assert res["PuzzleId"][0] == "p2"
    
    pm.apply_filters(theme="mate")
    res = pm.get_sample()
    assert res.height == 1
    assert res["PuzzleId"][0] == "p1"
