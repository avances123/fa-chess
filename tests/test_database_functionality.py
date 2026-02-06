import pytest
import os
import polars as pl
import chess
from PySide6.QtCore import Qt, QUrl
from ui.main_window import MainWindow
from PySide6.QtWidgets import QPushButton
import ui.main_window
import core.db_manager

@pytest.fixture
def app(qtbot, tmp_path, monkeypatch):
    # MOCK TOTAL: Redirigir tanto config como clipbase a carpetas temporales
    test_config = tmp_path / "test_config.json"
    test_clipbase = tmp_path / "test_clipbase.parquet"
    
    monkeypatch.setattr(ui.main_window, "CONFIG_FILE", str(test_config))
    monkeypatch.setattr(core.db_manager, "CLIPBASE_FILE", str(test_clipbase))
    
    window = MainWindow()
    qtbot.addWidget(window)
    return window

def test_database_real_game_flow(app, qtbot):
    parquet_path = os.path.abspath("tests/data/real_sample.parquet")
    app.load_parquet(parquet_path)
    base_name = os.path.basename(parquet_path)
    assert app.db.active_db_name == base_name
    assert app.db_table.rowCount() == 1
    
    app.search_criteria = {"white": "", "black": "yefealvaro", "min_elo": "", "result": "Cualquiera"}
    filtered = app.db.filter_db(app.search_criteria)
    app.refresh_db_list(filtered)
    assert app.db_table.rowCount() == 1
    
    item = app.db_table.item(0, 0)
    app.load_game_from_list(item)
    assert app.tabs.currentIndex() == 0
    assert len(app.game.full_mainline) >= 4

def test_database_remove_and_stats(app, qtbot):
    parquet_path = os.path.abspath("tests/data/real_sample.parquet")
    app.load_parquet(parquet_path)
    base_name = os.path.basename(parquet_path)
    assert app.label_db_stats.text() == "[1/1]"
    
    item_base = app.db_list_widget.findItems(base_name, Qt.MatchExactly)[0]
    app.remove_database(item_base)
    assert app.db.active_db_name == "Clipbase"
    assert app.label_db_stats.text() == "[0/0]"

def test_clear_filter_button(app, qtbot):
    parquet_path = os.path.abspath("tests/data/real_sample.parquet")
    app.load_parquet(parquet_path)
    app.search_criteria = {"white": "Inexistente", "black": "", "min_elo": "", "result": "Cualquiera"}
    filtered = app.db.filter_db(app.search_criteria)
    app.refresh_db_list(filtered)
    assert app.db_table.rowCount() == 0
    
    btn_clear = None
    for btn in app.findChildren(QPushButton):
        if "Quitar Filtros" in btn.text():
            btn_clear = btn
            break
    assert btn_clear is not None
    qtbot.mouseClick(btn_clear, Qt.LeftButton)
    assert app.db_table.rowCount() == 1
    assert app.label_db_stats.text() == "[1/1]"