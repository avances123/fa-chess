import pytest
import chess
from src.core.utils import uci_to_san, get_material_diff

def test_uci_to_san_legal_move():
    board = chess.Board()
    # e2e4 es legal en la posición inicial
    assert uci_to_san(board, "e2e4") == "e4"

def test_uci_to_san_illegal_move():
    board = chess.Board()
    # e2e5 es ilegal en la posición inicial
    assert uci_to_san(board, "e2e5") == "e2e5"

def test_uci_to_san_invalid_uci():
    board = chess.Board()
    assert uci_to_san(board, "invalid") == "invalid"

def test_get_material_diff_initial():
    board = chess.Board()
    diff = get_material_diff(board)
    assert diff[chess.WHITE]['score'] == 0
    assert diff[chess.BLACK]['score'] == 0
    assert not diff[chess.WHITE]['diffs']
    assert not diff[chess.BLACK]['diffs']

def test_get_material_diff_white_advantage():
    board = chess.Board("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
    board.remove_piece_at(chess.A7) # Quitar peón negro
    diff = get_material_diff(board)
    assert diff[chess.WHITE]['score'] == 1
    assert diff[chess.WHITE]['diffs'][chess.PAWN] == 1
    assert diff[chess.BLACK]['score'] == 0

def test_get_material_diff_black_advantage():
    board = chess.Board("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
    board.remove_piece_at(chess.A2) # Quitar peón blanco
    diff = get_material_diff(board)
    assert diff[chess.BLACK]['score'] == 1
    assert diff[chess.BLACK]['diffs'][chess.PAWN] == 1
    assert diff[chess.WHITE]['score'] == 0
