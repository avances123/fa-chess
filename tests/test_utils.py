import chess
import pytest
from src.core.utils import uci_to_san, get_material_diff

def test_uci_to_san_legal_move():
    board = chess.Board()
    # e2e4 es legal en la posición inicial
    assert uci_to_san(board, "e2e4") == "e4"

def test_uci_to_san_illegal_move():
    board = chess.Board()
    # e7e5 no es legal para las blancas en la pos inicial
    assert uci_to_san(board, "e7e5") == "e7e5"

def test_uci_to_san_invalid_format():
    board = chess.Board()
    assert uci_to_san(board, "not_a_move") == "not_a_move"

def test_get_material_diff_equal():
    board = chess.Board()
    diff = get_material_diff(board)
    assert diff[chess.WHITE]['score'] == 0
    assert diff[chess.BLACK]['score'] == 0
    assert diff[chess.WHITE]['diffs'] == {}
    assert diff[chess.BLACK]['diffs'] == {}

def test_get_material_diff_white_advantage():
    # Posición donde las blancas tienen una dama de más
    board = chess.Board("k7/8/8/8/8/8/8/Q1K5 w - - 0 1")
    diff = get_material_diff(board)
    assert diff[chess.WHITE]['score'] == 9
    assert diff[chess.WHITE]['diffs'][chess.QUEEN] == 1
    assert diff[chess.BLACK]['score'] == 0

def test_get_material_diff_complex():
    # Blanco: 2 torres (10), Negro: 1 dama (9) -> Blanco +1
    board = chess.Board("k7/8/8/8/8/8/rr6/Q1K5 w - - 0 1")
    # Espera, rr es negro.
    # Blanco: Dama(9). Negro: 2 Torres (10). -> Negro +1
    diff = get_material_diff(board)
    assert diff[chess.BLACK]['score'] == 1
    assert diff[chess.BLACK]['diffs'][chess.ROOK] == 2
    assert diff[chess.WHITE]['diffs'][chess.QUEEN] == 1
