import pytest
from PySide6.QtWidgets import QApplication
import sys
import polars as pl
import chess

@pytest.fixture(scope="session", autouse=True)
def mock_app_environment(tmp_path_factory):
    # Crear un entorno aislado para los archivos de la app
    tmp_dir = tmp_path_factory.mktemp("fa_chess_isolated")
    
    # Paths falsos para los tests
    fake_puzzle = tmp_dir / "puzzle.parquet"
    fake_app_db = tmp_dir / "app_test.db"
    fake_eco = tmp_dir / "scid_test.eco"
    
    # Crear archivos mínimos para que no fallen las lecturas
    import polars as pl
    import chess
    pl.DataFrame({
        "PuzzleId": ["test"], "FEN": [chess.STARTING_FEN], "Moves": ["e2e4 e7e5"],
        "Rating": [1000], "Themes": ["mate"], "OpeningTags": [""],
        "RatingDeviation": [75], "NbPlays": [100], "GameUrl": [""]
    }).write_parquet(fake_puzzle)
    
    with open(fake_eco, "w") as f:
        f.write("A00 \"Test ECO\" 1. e4")

    # Inyectar en el módulo de config ANTES de que nada lo use
    import src.config
    src.config.APP_DB_FILE = str(fake_app_db)
    src.config.PUZZLE_FILE = str(fake_puzzle)
    src.config.ECO_FILE = str(fake_eco)
    
    # También inyectar en widgets específicos
    import src.ui.widgets.puzzle_browser
    src.ui.widgets.puzzle_browser.PUZZLE_FILE = str(fake_puzzle)
    
    return tmp_dir

@pytest.fixture(scope="session")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv)
    yield app
