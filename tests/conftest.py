import pytest
from PySide6.QtWidgets import QApplication
import sys
import polars as pl
import chess

@pytest.fixture(scope="session", autouse=True)
def mock_puzzle_asset(tmp_path_factory):
    # Crear un archivo de puzzles mínimo para evitar KeyErrors globales
    tmp_dir = tmp_path_factory.mktemp("assets")
    puzzle_path = tmp_dir / "puzzle.parquet"
    df = pl.DataFrame({
        "PuzzleId": ["test"],
        "FEN": [chess.STARTING_FEN],
        "Moves": ["e2e4 e7e5"],
        "Rating": [1000],
        "Themes": ["mate"],
        "OpeningTags": [""],
        "RatingDeviation": [75],
        "NbPlays": [100],
        "GameUrl": [""]
    })
    df.write_parquet(puzzle_path)
    
    # Inyectar el path mockeado en el sistema
    import src.config
    src.config.PUZZLE_FILE = str(puzzle_path)
    import src.ui.widgets.puzzle_browser
    src.ui.widgets.puzzle_browser.PUZZLE_FILE = str(puzzle_path)
    return puzzle_path

@pytest.fixture(scope="session")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv)
    yield app
