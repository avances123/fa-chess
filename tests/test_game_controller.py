import chess
import pytest
from src.core.game_controller import GameController

def test_game_controller_init():
    gc = GameController()
    assert gc.board.fen() == chess.STARTING_FEN
    assert gc.full_mainline == []
    assert gc.current_idx == 0

def test_game_controller_load_uci_line():
    gc = GameController()
    gc.load_uci_line("e2e4 e7e5 g1f3")
    assert len(gc.full_mainline) == 3
    # Sincronizar el tablero al final de la línea cargada
    gc.go_end()
    assert gc.current_idx == 3
    assert gc.board.fen() == chess.Board("rnbqkbnr/pppp1ppp/8/4p3/4P3/5N2/PPPP1PPP/RNBQKB1R b KQkq - 1 2").fen()

def test_game_controller_make_move():
    gc = GameController()
    move = chess.Move.from_uci("e2e4")
    gc.make_move(move)
    assert gc.current_idx == 1
    assert gc.full_mainline == [move]
    
    gc.step_back()
    gc.make_move(move)
    assert gc.current_idx == 1
    assert len(gc.full_mainline) == 1

def test_game_controller_navigation():
    gc = GameController()
    gc.load_uci_line("e2e4 e7e5")
    gc.go_end() # IMPORTANTE: Sincronizar
    assert gc.current_idx == 2
    
    gc.step_back()
    assert gc.current_idx == 1
    assert gc.board.fen() == chess.Board("rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq - 0 1").fen()
    
    gc.step_forward()
    assert gc.current_idx == 2
    
    gc.go_start()
    assert gc.current_idx == 0
    assert gc.board.fen() == chess.STARTING_FEN

def test_game_controller_jump_to_move():
    gc = GameController()
    gc.load_uci_line("e2e4 e7e5 g1f3 b8c6")
    gc.jump_to_move(2)
    assert gc.current_idx == 2
    assert "e2e4 e7e5" in gc.current_line_uci

def test_game_controller_reset():
    gc = GameController()
    gc.load_uci_line("e2e4")
    gc.reset()
    assert gc.current_idx == 0
    assert gc.full_mainline == []
