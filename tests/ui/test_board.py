import pytest
import chess
from PySide6.QtCore import Qt, QPointF, QEvent
from PySide6.QtGui import QMouseEvent
from src.ui.board import ChessBoard
from src.core.game_controller import GameController

class MockMainWindow:
    def __init__(self):
        self.game = GameController()

@pytest.fixture
def board_widget(qapp):
    parent = MockMainWindow()
    board = chess.Board()
    widget = ChessBoard(board, parent)
    widget.square_size = 100
    widget.show()
    return widget

def test_mouse_interaction_make_move(board_widget, monkeypatch):
    # Sincronizar controlador y tablero
    board = board_widget.parent_main.game.board
    board.reset()
    board_widget.board = board
    
    def mock_get_square(pos):
        if pos.y() > 500: return chess.E2
        if pos.y() < 500: return chess.E4
        return None
    
    monkeypatch.setattr(board_widget, "get_square", mock_get_square)
    
    # PySide6 moderno espera: type, localPos, button, buttons, modifiers
    press = QMouseEvent(QEvent.Type.MouseButtonPress, QPointF(450, 600), Qt.LeftButton, Qt.LeftButton, Qt.NoModifier)
    board_widget.mousePressEvent(press)
    assert board_widget.selected_square == chess.E2
    
    release = QMouseEvent(QEvent.Type.MouseButtonRelease, QPointF(450, 400), Qt.LeftButton, Qt.LeftButton, Qt.NoModifier)
    board_widget.mouseReleaseEvent(release)
    
    assert board.piece_at(chess.E4) is not None
