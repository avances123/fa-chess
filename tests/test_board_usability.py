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
    app.make_move(chess.Move.from_uci("e2e4"))
    app.make_move(chess.Move.from_uci("e7e5"))
    assert app.current_idx == 2
    
    app.step_back()
    assert app.current_idx == 1
    assert len(app.board.move_stack) == 1
    
    app.step_forward()
    assert app.current_idx == 2
    assert len(app.board.move_stack) == 2
    
    app.go_start()
    assert app.current_idx == 0
    assert len(app.board.move_stack) == 0
    
    app.go_end()
    assert app.current_idx == 2
    assert len(app.board.move_stack) == 2

def test_board_shortcuts(app, qtbot):
    app.make_move(chess.Move.from_uci("d2d4"))
    
    # Simular clic en el botón de la toolbar (Retroceder)
    btn_back = None
    for action in app.toolbar_ana.actions():
        if "Anterior" in action.toolTip():
            action.trigger()
            break
            
    assert app.current_idx == 0

def test_board_flip_logic(app):
    assert app.board_ana.flipped is False
    app.flip_boards()
    assert app.board_ana.flipped is True
    app.flip_boards()
    assert app.board_ana.flipped is False

def test_jump_to_move_via_history(app):
    moves = ["e2e4", "c7c5", "g1f3", "d7d6"]
    for m in moves:
        app.make_move(chess.Move.from_uci(m))
    
    # Saltamos a la jugada 2 (después de c5)
    app.jump_to_move(QUrl("2"))
    assert app.current_idx == 2
    assert app.board.fen().startswith("rnbqkbnr/pp1ppppp/8/2p5/4P3/8/PPPP1PPP/RNBQKBNR")

def test_overwrite_mainline(app):
    for m in ["e2e4", "c7c5", "g1f3"]:
        app.make_move(chess.Move.from_uci(m))
    assert len(app.full_mainline) == 3
    
    app.step_back()
    app.step_back() 
    assert app.current_idx == 1
    
    app.make_move(chess.Move.from_uci("e7e5"))
    
    assert len(app.full_mainline) == 2
    assert app.full_mainline == [chess.Move.from_uci("e2e4"), chess.Move.from_uci("e7e5")]
    assert app.current_idx == 2

def test_no_overwrite_if_same_move(app):
    for m in ["e2e4", "c7c5", "g1f3"]:
        app.make_move(chess.Move.from_uci(m))
    
    app.step_back()
    app.step_back() 
    
    app.make_move(chess.Move.from_uci("c7c5"))
    
    assert len(app.full_mainline) == 3
    assert app.current_idx == 2
