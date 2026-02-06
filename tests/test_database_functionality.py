import pytest
import os
import polars as pl
import chess
from PySide6.QtCore import Qt, QUrl
from ui.main_window import MainWindow
import ui.main_window

@pytest.fixture
def app(qtbot, tmp_path, monkeypatch):
    # Mock de la ruta de configuración para no tocar el archivo real del usuario
    test_config = tmp_path / "test_config.json"
    monkeypatch.setattr(ui.main_window, "CONFIG_FILE", str(test_config))
    
    window = MainWindow()
    qtbot.addWidget(window)
    return window

def test_database_real_game_flow(app, qtbot):
    parquet_path = os.path.abspath("tests/data/real_sample.parquet")
    
    # 1. Cargar la base
    app.load_parquet(parquet_path)
    base_name = os.path.basename(parquet_path)
    assert app.db.active_db_name == base_name
    assert app.db_table.rowCount() == 1
    
    # 2. Filtrar por jugador negro "yefealvaro"
    app.search_criteria = {"white": "", "black": "yefealvaro", "min_elo": "", "result": "Cualquiera"}
    filtered = app.db.filter_db(app.db.active_db_name, app.search_criteria)
    app.refresh_db_list(filtered)
    
    assert app.db_table.rowCount() == 1
    assert app.db_table.item(0, 3).text() == "yefealvaro"
    
    # 3. Doble click para cargar la partida
    item = app.db_table.item(0, 0)
    app.load_game_from_list(item)
    
    # Verificar estado del tablero tras carga
    assert app.tabs.currentIndex() == 0
    assert len(app.full_mainline) >= 4
    assert app.current_idx == 0 
    assert app.board.fen() == chess.Board().fen()
    
    # Avanzar unas jugadas y verificar FEN
    for _ in range(4): # 1. e4 b6 2. d4 Bb7
        app.step_forward()
    
    # Tras 2. d4 Bb7
    # La FEN de Owen's Defense tras e4 b6 d4 Bb7
    expected_fen_prefix = "rn1qkbnr/pbpppppp/1p6/8/3PP3/8/PPP2PPP/RNBQKBNR"
    assert app.board.fen().startswith(expected_fen_prefix)

def test_database_remove_and_stats(app, qtbot):
    parquet_path = os.path.abspath("tests/data/real_sample.parquet")
    app.load_parquet(parquet_path)
    base_name = os.path.basename(parquet_path)
    
    # Stats iniciales [1/1]
    assert app.label_db_stats.text() == "[1/1]"
    
    # Quitar base
    item_base = app.db_list_widget.findItems(base_name, Qt.MatchExactly)[0]
    app.remove_database(item_base)
    
    # Al estar mockeado el config, ahora sí debería estar vacío y volver a Clipbase
    assert app.db.active_db_name == "Clipbase"
    assert app.label_db_stats.text() == "[0/0]"
