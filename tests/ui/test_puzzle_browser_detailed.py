import pytest
import polars as pl
import chess
import os
from PySide6.QtCore import Qt, QSize
from PySide6.QtWidgets import QWidget
from src.ui.widgets.puzzle_browser import PuzzleBrowserWidget

@pytest.fixture
def puzzle_widget(qtbot, tmp_path, monkeypatch):
    # Crear un parquet de puzzles de prueba usando polars
    df = pl.DataFrame({
        "PuzzleId": ["p1", "p2"],
        "FEN": [chess.STARTING_FEN, chess.STARTING_FEN],
        "Moves": ["e2e4 e7e5", "d2d4 d7d5"],
        "Rating": [1000, 1500],
        "Themes": ["mate", "fork"],
        "OpeningTags": ["", ""],
        "RatingDeviation": [75, 75],
        "NbPlays": [100, 100],
        "GameUrl": ["", ""]
    })
    path = tmp_path / "puzzle.parquet"
    df.write_parquet(path)
    
    class MockBoard:
        def __init__(self):
            self.color_light = "#eee"
            self.color_dark = "#888"
            
    class MockAppDB:
        def get_all_puzzle_stats(self):
            return {"p1": "success"}
        def get_config(self, key, default):
            return default
        def get_tactical_elo(self):
            return 1200
        def set_tactical_elo(self, val):
            pass
        def set_config(self, key, val):
            pass

    class MockParent(QWidget):
        def __init__(self):
            super().__init__()
            self.app_db = MockAppDB()
            self.board_ana = MockBoard()
            self.engine_path = "/usr/bin/stockfish"
    
    parent = MockParent()
    # Mockear el path de los puzzles
    monkeypatch.setattr("src.ui.widgets.puzzle_browser.PUZZLE_FILE", str(path))
    
    # Mockear prepare_puzzle_data para que devuelva algo coherente
    from src.core.puzzle_manager import PuzzleManager
    def mock_prepare(self, row):
        return {
            "id": row["PuzzleId"],
            "initial_fen": row["FEN"],
            "opponent_move": row["Moves"].split()[0],
            "solution": row["Moves"].split()[1:],
            "rating": row["Rating"]
        }
    monkeypatch.setattr(PuzzleManager, "prepare_puzzle_data", mock_prepare)

    widget = PuzzleBrowserWidget(parent)
    qtbot.addWidget(widget)
    return widget

def test_puzzle_loading_and_navigation(puzzle_widget, qtbot):
    # Set ELO to match p1 (1000)
    puzzle_widget.slider_elo.setValue(1000)
    puzzle_widget.apply_filters()
    
    assert puzzle_widget.puzzle_df is not None
    
    # Cargar por índice
    puzzle_widget.load_puzzle_by_index(0)
    assert puzzle_widget.current_puzzle is not None
    
    # Navegar al siguiente
    puzzle_widget.load_next_puzzle()

def test_puzzle_solving_logic(puzzle_widget, qtbot):
    puzzle_widget.slider_elo.setValue(1000)
    puzzle_widget.apply_filters()
    puzzle_widget.load_puzzle_by_index(0)
    
    # El usuario debe hacer e7e5 (segunda jugada de Moves "e2e4 e7e5")
    puzzle_widget.check_move("e7e5")
    assert puzzle_widget.label_feedback.text() == "¡RESUELTO! 🏆"

def test_puzzle_filtering(puzzle_widget, qtbot):
    qtbot.mouseClick(puzzle_widget.btn_success, Qt.LeftButton)
    assert puzzle_widget.filter_status == "success"
    
    qtbot.mouseClick(puzzle_widget.btn_success, Qt.LeftButton) # Toggle off
    assert puzzle_widget.filter_status == "all"

def test_puzzle_hints(puzzle_widget, qtbot):
    puzzle_widget.slider_elo.setValue(1000)
    puzzle_widget.apply_filters()
    puzzle_widget.load_puzzle_by_index(0)
    
    qtbot.mouseClick(puzzle_widget.btn_hint_tension, Qt.LeftButton)
    qtbot.mouseClick(puzzle_widget.btn_hint_piece, Qt.LeftButton)
    qtbot.mouseClick(puzzle_widget.btn_hint_dest, Qt.LeftButton)
    
    assert puzzle_widget.hint_level >= 1

def test_puzzle_scroll_and_resize(puzzle_widget, qtbot):
    puzzle_widget.on_scroll(100)
    puzzle_widget.resize(800, 600)
