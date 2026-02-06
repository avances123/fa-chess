import pytest
import os
import polars as pl
import chess
from PySide6.QtCore import Qt, QUrl
from ui.main_window import MainWindow
from PySide6.QtWidgets import QPushButton
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
    # Tras el refactor, filter_db toma solo los criterios, el nombre de la DB es interno
    filtered = app.db.filter_db(app.search_criteria)
    app.refresh_db_list(filtered)
    
    assert app.db_table.rowCount() == 1
    assert app.db_table.item(0, 3).text() == "yefealvaro"
    
    # 3. Doble click para cargar la partida
    item = app.db_table.item(0, 0)
    app.load_game_from_list(item)
    
    # Verificar estado del tablero tras carga
    assert app.tabs.currentIndex() == 0
    assert len(app.game.full_mainline) >= 4
    assert app.game.current_idx == 0 
    assert app.game.board.fen() == chess.Board().fen()
    
    # Avanzar unas jugadas y verificar FEN
    for _ in range(4): # 1. e4 b6 2. d4 Bb7
        app.game.step_forward()
    
    # Tras 2. d4 Bb7
    expected_fen_prefix = "rn1qkbnr/pbpppppp/1p6/8/3PP3/8/PPP2PPP/RNBQKBNR"
    assert app.game.board.fen().startswith(expected_fen_prefix)

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

def test_clear_filter_button(app, qtbot):
    parquet_path = os.path.abspath("tests/data/real_sample.parquet")
    app.load_parquet(parquet_path)
    
    # 1. Aplicar un filtro (ej: buscar alguien que NO existe)
    app.search_criteria = {"white": "Inexistente", "black": "", "min_elo": "", "result": "Cualquiera"}
    filtered = app.db.filter_db(app.search_criteria)
    app.refresh_db_list(filtered)
    
    assert app.db_table.rowCount() == 0
    assert app.label_db_stats.text() == "[0/1]"
    
    # 2. Buscar y pulsar el botón "Quitar Filtros"
    btn_clear = None
    for btn in app.findChildren(QPushButton):
        if "Quitar Filtros" in btn.text():
            btn_clear = btn
            break
    
    assert btn_clear is not None
    qtbot.mouseClick(btn_clear, Qt.LeftButton)
    
    # 3. Verificar que se han restablecido las partidas y las stats
    assert app.db_table.rowCount() == 1
    assert app.label_db_stats.text() == "[1/1]"
    assert app.search_criteria["white"] == ""
