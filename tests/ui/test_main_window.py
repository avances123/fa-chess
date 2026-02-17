import pytest
import os
import chess
from PySide6.QtCore import Qt, QTimer
from src.ui.main_window import MainWindow

@pytest.fixture
def main_window(qapp, tmp_path, monkeypatch):
    app_db = tmp_path / "app_test.db"
    eco_file = tmp_path / "test.eco"
    eco_file.write_text('A00 "Anderssen" 1. a3\n', encoding="utf-8")
    
    monkeypatch.setattr("src.ui.main_window.APP_DB_FILE", str(app_db))
    monkeypatch.setattr("src.ui.main_window.ECO_FILE", str(eco_file))
    
    window = MainWindow()
    window.show()
    return window

def test_main_window_init(main_window):
    assert main_window.windowTitle() == "fa-chess"
    assert main_window.tabs.count() >= 3

def test_navigation_shortcuts(main_window, qtbot):
    main_window.activateWindow()
    main_window.setFocus()
    main_window.game.load_uci_line("e2e4 e7e5")
    main_window.game.go_start()
    
    # Simular eventos de teclado directamente al objeto que tiene el foco
    qtbot.keyClick(main_window, Qt.Key_Right)
    qtbot.wait(50) # Esperar a que se procese
    assert main_window.game.current_idx == 1
    
    qtbot.keyClick(main_window, Qt.Key_End)
    qtbot.wait(50)
    assert main_window.game.current_idx == 2

def test_flip_board_action(main_window):
    initial_flipped = main_window.board_ana.flipped
    main_window.flip_boards()
    assert main_window.board_ana.flipped != initial_flipped

def test_new_game_button(main_window, qtbot):
    main_window.game.load_uci_line("e2e4 e7e5")
    qtbot.mouseClick(main_window.btn_new, Qt.LeftButton)
    assert main_window.game.current_idx == 0
    assert main_window.game.full_mainline == []

def test_menu_about(main_window, monkeypatch):
    called = False
    def mock_about(parent, title, text):
        nonlocal called
        called = True
    
    import PySide6.QtWidgets
    monkeypatch.setattr(PySide6.QtWidgets.QMessageBox, "about", mock_about)
    
    for action in main_window.menuBar().actions():
        if "Ayuda" in action.text():
            for sub_action in action.menu().actions():
                if "Acerca de" in sub_action.text():
                    sub_action.trigger()
                    break
    assert called

def test_engine_toggle(main_window, monkeypatch, qtbot):
    class MockEngineWorker:
        def __init__(self, **kwargs): pass
        def start(self): pass
        def stop(self): pass
        def wait(self): pass
        def isRunning(self): return False
        def update_position(self, fen): pass
        def connect(self, *args): pass
        def disconnect(self, *args): pass
        info_updated = type('obj', (object,), {'connect': lambda *a: None, 'disconnect': lambda *a: None})
    
    monkeypatch.setattr("src.ui.main_window.EngineWorker", MockEngineWorker)
    
    main_window.action_engine.setChecked(True)
    main_window.toggle_engine(True)
    
    main_window.toggle_engine(False)
