import pytest
import chess
from PySide6.QtCore import Qt, QUrl
from ui.main_window import MainWindow

@pytest.fixture
def app(qtbot):
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
    # 1. Hacer una línea: 1. e4 c5 2. Nf3
    for m in ["e2e4", "c7c5", "g1f3"]:
        app.make_move(chess.Move.from_uci(m))
    assert len(app.full_mainline) == 3
    
    # 2. Volver atrás hasta 1. e4
    app.step_back() # vuelve a 1... c5
    app.step_back() # vuelve a 1. e4
    assert app.current_idx == 1
    
    # 3. Hacer una jugada diferente: 1... e5
    # IMPORTANTE: Si ya existe e7e5 en el índice 1, make_move NO debería borrar nada, solo avanzar.
    # Pero si la jugada es NUEVA, entonces sí debe borrar el resto.
    app.make_move(chess.Move.from_uci("e7e5"))
    
    # 4. Verificar que la línea ahora es [e4, e5] y tiene longitud 2
    assert len(app.full_mainline) == 2
    assert app.full_mainline == [chess.Move.from_uci("e2e4"), chess.Move.from_uci("e7e5")]
    assert app.current_idx == 2

def test_no_overwrite_if_same_move(app):
    # Si hacemos la misma jugada que ya existe en la mainline, no borrar el resto
    for m in ["e2e4", "c7c5", "g1f3"]:
        app.make_move(chess.Move.from_uci(m))
    
    app.step_back()
    app.step_back() # estamos en 1. e4
    
    # Hacemos 1... c5 otra vez
    app.make_move(chess.Move.from_uci("c7c5"))
    
    # La línea debe seguir teniendo 3 jugadas (no se debe haber borrado 2. Nf3)
    assert len(app.full_mainline) == 3
    assert app.current_idx == 2