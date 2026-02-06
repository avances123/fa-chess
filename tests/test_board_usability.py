import pytest
import chess
from PySide6.QtCore import Qt, QUrl
from ui.main_window import MainWindow
import ui.main_window
import core.db_manager

@pytest.fixture
def app(qtbot, tmp_path, monkeypatch):
    test_config = tmp_path / "test_config.json"
    test_clipbase = tmp_path / "test_clipbase.parquet"
    monkeypatch.setattr(ui.main_window, "CONFIG_FILE", str(test_config))
    monkeypatch.setattr(core.db_manager, "CLIPBASE_FILE", str(test_clipbase))
    
    window = MainWindow()
    qtbot.addWidget(window)
    return window

def test_board_navigation_methods(app):
    app.game.make_move(chess.Move.from_uci("e2e4"))
    app.game.make_move(chess.Move.from_uci("e7e5"))
    assert app.game.current_idx == 2
    app.game.step_back()
    assert app.game.current_idx == 1
    app.game.step_forward()
    assert app.game.current_idx == 2

def test_board_shortcuts(app, qtbot):
    app.game.make_move(chess.Move.from_uci("d2d4"))
    btn_back = None
    for action in app.toolbar_ana.actions():
        if "Anterior" in action.toolTip():
            action.trigger(); break
    assert app.game.current_idx == 0

def test_board_flip_logic(app):
    assert app.board_ana.flipped is False
    app.flip_boards()
    assert app.board_ana.flipped is True

def test_board_wheel_navigation(app, qtbot):
    app.game.make_move(chess.Move.from_uci("e2e4"))
    from PySide6.QtGui import QWheelEvent
    from PySide6.QtCore import QPointF, QPoint
    event_back = QWheelEvent(QPointF(0,0), QPointF(0,0), QPoint(0,0), QPoint(0, 120), Qt.NoButton, Qt.NoModifier, Qt.ScrollUpdate, False)
    app.board_ana.wheelEvent(event_back)
    assert app.game.current_idx == 0

def test_jump_to_move_via_history(app):
    app.game.make_move(chess.Move.from_uci("e2e4"))
    app.jump_to_move_link(QUrl("1"))
    assert app.game.current_idx == 1

def test_overwrite_mainline(app):
    app.game.make_move(chess.Move.from_uci("e2e4"))
    app.game.step_back()
    app.game.make_move(chess.Move.from_uci("d2d4"))
    assert len(app.game.full_mainline) == 1
    assert app.game.full_mainline[0].uci() == "d2d4"