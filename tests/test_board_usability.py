import pytest
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

def test_board_navigation_methods(app):
    # Probar los métodos directamente para asegurar que la lógica interna funciona
    app.game.make_move(chess.Move.from_uci("e2e4"))
    app.game.make_move(chess.Move.from_uci("e7e5"))
    assert app.game.current_idx == 2
    
    app.game.step_back()
    assert app.game.current_idx == 1
    assert len(app.game.board.move_stack) == 1
    
    app.game.step_forward()
    assert app.game.current_idx == 2
    assert len(app.game.board.move_stack) == 2
    
    app.game.go_start()
    assert app.game.current_idx == 0
    assert len(app.game.board.move_stack) == 0
    
    app.game.go_end()
    assert app.game.current_idx == 2
    assert len(app.game.board.move_stack) == 2

def test_board_shortcuts(app, qtbot):
    app.game.make_move(chess.Move.from_uci("d2d4"))
    
    # Simular clic en el botón de la toolbar (Retroceder)
    btn_back = None
    for action in app.toolbar_ana.actions():
        if "Anterior" in action.toolTip():
            action.trigger()
            break
            
    assert app.game.current_idx == 0

def test_board_flip_logic(app):
    assert app.board_ana.flipped is False
    app.flip_boards()
    assert app.board_ana.flipped is True
    app.flip_boards()
    assert app.board_ana.flipped is False

def test_board_wheel_navigation(app, qtbot):
    # 1. Hacer un par de jugadas
    app.game.make_move(chess.Move.from_uci("e2e4"))
    app.game.make_move(chess.Move.from_uci("e7e5"))
    assert app.game.current_idx == 2
    
    # 2. Simular rueda arriba (retroceder)
    from PySide6.QtGui import QWheelEvent
    from PySide6.QtCore import QPoint, QPointF
    
    # Firma: (pos, globalPos, pixelDelta, angleDelta, buttons, modifiers, phase, inverted)
    event_back = QWheelEvent(QPointF(0,0), QPointF(0,0), QPoint(0,0), QPoint(0, 120), 
                             Qt.NoButton, Qt.NoModifier, Qt.ScrollUpdate, False)
    app.board_ana.wheelEvent(event_back)
    assert app.game.current_idx == 1
    
    # 3. Simular rueda abajo (avanzar)
    event_fwd = QWheelEvent(QPointF(0,0), QPointF(0,0), QPoint(0,0), QPoint(0, -120), 
                            Qt.NoButton, Qt.NoModifier, Qt.ScrollUpdate, False)
    app.board_ana.wheelEvent(event_fwd)
    assert app.game.current_idx == 2

def test_jump_to_move_via_history(app):
    moves = ["e2e4", "c7c5", "g1f3", "d7d6"]
    for m in moves:
        app.game.make_move(chess.Move.from_uci(m))
    
    # Saltamos a la jugada 2 (después de c5)
    app.jump_to_move_link(QUrl("2"))
    assert app.game.current_idx == 2
    assert app.game.board.fen().startswith("rnbqkbnr/pp1ppppp/8/2p5/4P3/8/PPPP1PPP/RNBQKBNR")

def test_overwrite_mainline(app):
    for m in ["e2e4", "c7c5", "g1f3"]:
        app.game.make_move(chess.Move.from_uci(m))
    assert len(app.game.full_mainline) == 3
    
    app.game.step_back()
    app.game.step_back() 
    assert app.game.current_idx == 1
    
    app.game.make_move(chess.Move.from_uci("e7e5"))
    
    assert len(app.game.full_mainline) == 2
    assert app.game.full_mainline == [chess.Move.from_uci("e2e4"), chess.Move.from_uci("e7e5")]
    assert app.game.current_idx == 2

def test_no_overwrite_if_same_move(app):
    for m in ["e2e4", "c7c5", "g1f3"]:
        app.game.make_move(chess.Move.from_uci(m))
    
    app.game.step_back()
    app.game.step_back() 
    
    app.game.make_move(chess.Move.from_uci("c7c5"))
    
    assert len(app.game.full_mainline) == 3
    assert app.game.current_idx == 2
