import pytest
import polars as pl
import os
from src.core.puzzle_manager import PuzzleManager

@pytest.fixture
def puzzle_manager():
    path = "tests/data/puzzles.parquet"
    df = pl.DataFrame({
        "PuzzleId": ["p1", "p2"],
        "FEN": ["fen1", "fen2"],
        "Moves": ["e2e4 e7e5", "d2d4 d7d5"],
        "Rating": [1000, 1500],
        "Themes": ["fork mate", "pin"],
        "OpeningTags": ["", ""]
    })
    df.write_parquet(path)
    manager = PuzzleManager(path)
    yield manager
    if os.path.exists(path): os.remove(path)

def test_puzzle_manager_filters(puzzle_manager):
    # Filtro por ELO
    puzzle_manager.apply_filters(min_rating=1200, max_rating=1600)
    res = puzzle_manager.get_sample()
    assert res.height == 1
    assert res["PuzzleId"][0] == "p2"
    
    # Filtro por Tema (AND)
    puzzle_manager.apply_filters(theme="fork mate")
    res = puzzle_manager.get_sample()
    assert res.height == 1
    assert res["PuzzleId"][0] == "p1"

def test_puzzle_manager_prepare_data(puzzle_manager):
    row = puzzle_manager.get_sample().row(0, named=True)
    data = puzzle_manager.prepare_puzzle_data(row)
    assert data["id"] == "p1"
    assert data["opponent_move"] == "e2e4"
    assert data["solution"] == ["e7e5"]

def test_puzzle_manager_random(puzzle_manager):
    p = puzzle_manager.get_random_puzzle()
    assert p is not None
    assert p["id"] in ["p1", "p2"]
