import pytest
import chess
import os
import time
from PySide6.QtCore import Qt, QPoint
from PySide6.QtWidgets import QFileDialog, QMessageBox, QApplication
from src.ui.main_window import MainWindow

@pytest.fixture
def main_win(qtbot, tmp_path, monkeypatch):
    # Mockear paths y base de datos
    monkeypatch.setattr("src.ui.main_window.APP_DB_FILE", str(tmp_path / "test_app.db"))
    
    # Mockear diálogos para que no bloqueen
    monkeypatch.setattr(QFileDialog, "getSaveFileName", lambda *args: (str(tmp_path / "new.parquet"), "Parquet (*.parquet)"))
    monkeypatch.setattr(QFileDialog, "getOpenFileName", lambda *args: (str(tmp_path / "open.parquet"), "Parquet (*.parquet)"))
    monkeypatch.setattr(QMessageBox, "about", lambda *args: None)
    monkeypatch.setattr(QMessageBox, "question", lambda *args: QMessageBox.Yes)
    monkeypatch.setattr(QMessageBox, "information", lambda *args: None)

    win = MainWindow()
    win.show()
    qtbot.addWidget(win)
    return win

def test_menu_file_actions(main_win, qtbot, tmp_path):
    # Probar "Nueva Base"
    main_win.create_new_db()
    assert "new.parquet" in main_win.db.dbs
    
    # Probar "Abrir Base" (creamos el archivo primero para que no falle la carga)
    open_path = tmp_path / "open.parquet"
    main_win.db.create_new_database(str(open_path))
    main_win.open_parquet_file()
    assert "open.parquet" in main_win.db.dbs
    
    # Probar "Guardar Base"
    main_win.save_to_active_db()
    
    # Probar "Configuración"
    # Simulamos que el diálogo se acepta
    from PySide6.QtWidgets import QDialog
    with pytest.MonkeyPatch().context() as m:
        m.setattr("src.ui.settings_dialog.SettingsDialog.exec_", lambda self: QDialog.Accepted)
        main_win.open_settings()

def test_menu_db_actions(main_win, qtbot, tmp_path):
    # Importar PGN
    pgn_path = tmp_path / "test.pgn"
    pgn_path.write_text('[White "W"] [Black "B"] [Result "1-0"] 1. e4 e5 2. Nf3 *')
    
    with pytest.MonkeyPatch().context() as m:
        m.setattr(QFileDialog, "getOpenFileName", lambda *args: (str(pgn_path), ""))
        main_win.import_pgn()
        qtbot.waitUntil(lambda: hasattr(main_win, 'worker'), timeout=5000)
        # Asegurar que el archivo existe para que load_parquet no falle
        final_parquet = tmp_path / "imported.parquet"
        main_win.db.create_new_database(str(final_parquet))
        main_win.worker.finished.emit(str(final_parquet))
    
    assert "imported.parquet" in main_win.db.dbs

def test_engine_interactions_coverage(main_win, qtbot):
    # Toggle Engine
    main_win.action_engine.setChecked(True)
    main_win.toggle_engine(True)
    assert hasattr(main_win, 'engine_worker')
    
    # Mockear actualizaciones del motor con diferentes scores (Mate, Negativo, etc)
    main_win.on_engine_update("M1", "e2e4", "e2e4")
    assert main_win.eval_bar.value() == 1000
    
    main_win.on_engine_update("-M2", "d2d4", "d2d4")
    assert main_win.eval_bar.value() == -1000
    
    main_win.on_engine_update("0.50 | +0.50", "g1f3", "g1f3")
    assert main_win.eval_bar.value() == 50

    # Caso de error en on_engine_update
    main_win.on_engine_update("invalid", None, "")

    main_win.toggle_engine(False)

def test_full_analysis_flow(main_win, qtbot):
    main_win.game.make_move(chess.Move.from_uci("e2e4"))
    
    with pytest.MonkeyPatch().context() as m:
        # Evitar que el thread se ejecute de verdad y oculte el progreso
        m.setattr("src.ui.main_window.FullAnalysisWorker.start", lambda self: None)
        main_win.start_full_analysis()
        assert main_win.progress.isVisible()
    
    # Simular progreso y fin
    main_win.on_analysis_update(0, 10)
    main_win.on_analysis_update(1, -20)
    main_win.on_analysis_finished()
    
    assert not main_win.progress.isVisible()

def test_db_sidebar_and_table_coverage(main_win, qtbot, tmp_path):
    # Añadir base
    db_path = tmp_path / "test_sidebar.parquet"
    main_win.db.create_new_database(str(db_path))
    main_win.load_parquet(str(db_path))
    
    # Forzar refresco de lista
    main_win.refresh_db_list()
    
    # Simular scroll en la tabla
    main_win.db_table.verticalScrollBar().setValue(100)
    main_win.on_db_scroll(100)
    
    # Simular cambio de base
    main_win.switch_database_with_feedback("test_sidebar.parquet")
    
    # Invertir filtro (aunque sea un pass)
    main_win.trigger_invert_filter()
    
    # Reset filtros
    main_win.reset_filters()

def test_misc_actions(main_win, qtbot):
    # About dialog
    main_win.show_about_dialog()
    
    # Flip boards
    main_win.flip_boards()
    
    # Search current position
    main_win.search_current_position()
    
    # Add current game to DB (sin base activa)
    main_win.db.active_db_name = None
    main_win.add_current_game_to_db()
    
    # Con base activa (creamos una fake con schema real)
    import polars as pl
    from src.core.db_manager import GAME_SCHEMA
    main_win.db.active_db_name = "test.parquet"
    main_win.db.dbs["test.parquet"] = pl.DataFrame([], schema=GAME_SCHEMA).lazy()
    main_win.db.db_metadata["test.parquet"] = {"read_only": False, "path": "test.parquet"}
    main_win.add_current_game_to_db()

def test_warm_up_cache(main_win, qtbot):
    with pytest.MonkeyPatch().context() as m:
        m.setattr(QMessageBox, "question", lambda *args: QMessageBox.Yes)
        main_win.warm_up_opening_cache()
        assert hasattr(main_win, 'warm_worker')
        main_win.stop_current_operation()
