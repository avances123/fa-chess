import pytest
import chess
from src.core.game_controller import GameController

@pytest.fixture
def controller(qapp):
    return GameController()

def test_initial_state(controller):
    assert controller.board.fen() == chess.STARTING_FEN
    assert controller.current_idx == 0
    assert controller.full_mainline == []

def test_make_move(controller):
    move = chess.Move.from_uci("e2e4")
    controller.make_move(move)
    assert controller.board.fen() != chess.STARTING_FEN
    assert controller.current_idx == 1
    assert controller.full_mainline == [move]

def test_make_move_on_existing_mainline(controller):
    controller.load_uci_line("e2e4 e7e5")
    controller.go_start()
    
    # Hacer la misma jugada que ya existe
    controller.make_move(chess.Move.from_uci("e2e4"))
    assert controller.current_idx == 1
    assert len(controller.full_mainline) == 2
    
    # Hacer una jugada diferente (debería truncar la línea)
    controller.go_start()
    controller.step_forward() # e4
    controller.make_move(chess.Move.from_uci("c7c5")) # Siciliana
    assert controller.current_idx == 2
    assert len(controller.full_mainline) == 2
    assert controller.full_mainline[-1].uci() == "c7c5"

def test_navigation(controller):
    controller.load_uci_line("e2e4 e7e5 g1f3 b8c6")
    assert controller.current_idx == 4
    
    controller.step_back()
    assert controller.current_idx == 3
    
    controller.go_start()
    assert controller.current_idx == 0
    assert controller.board.fen() == chess.STARTING_FEN
    
    controller.go_end()
    assert controller.current_idx == 4
    
    controller.jump_to_move(2)
    assert controller.current_idx == 2
    assert controller.board.move_stack[-1].uci() == "e7e5"

def test_current_line_uci(controller):
    controller.load_uci_line("e2e4 e7e5")
    assert controller.current_line_uci == "e2e4 e7e5"
