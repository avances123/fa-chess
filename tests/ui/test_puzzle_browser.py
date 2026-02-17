import pytest
import polars as pl
import chess
import os
from src.ui.widgets.puzzle_browser import PuzzleBrowserWidget

@pytest.fixture
def puzzle_data(tmp_path):
    path = tmp_path / "puzzles.parquet"
    df = pl.DataFrame({
        "PuzzleId": ["p1"], 
        "FEN": ["rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1"], 
        "Moves": ["e2e4 e7e5"],
        "Rating": [1200], # Coincidir con el slider por defecto
        "Popularity": [100],
        "Themes": ["mate"], 
        "OpeningTags": ["e4"]
    })
    df.write_parquet(path)
    return path

def test_puzzle_browser_init(qapp, puzzle_data, monkeypatch):
    monkeypatch.setattr("src.ui.widgets.puzzle_browser.PUZZLE_FILE", str(puzzle_data))
    
    widget = PuzzleBrowserWidget()
    # Forzar carga y aplicación de filtros
    widget.load_db(str(puzzle_data))
    widget.slider_elo.setValue(1200)
    widget.apply_filters()
    
    assert widget.manager is not None
    assert widget.puzzle_df is not None
    assert widget.puzzle_df.height > 0
    
    widget.load_puzzle_by_index(0)
    assert widget.current_puzzle is not None
    assert widget.current_puzzle["id"] == "p1"
