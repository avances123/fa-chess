import pytest
import polars as pl
import chess
from PySide6.QtCore import Qt
from src.ui.widgets.opening_tree_table import OpeningTreeTable

@pytest.fixture
def tree_table(qapp):
    widget = OpeningTreeTable()
    widget.show()
    return widget

def test_tree_table_loading(tree_table):
    tree_table.set_loading(True)
    assert tree_table.stack.currentIndex() == 1
    tree_table.set_loading(False)
    assert tree_table.stack.currentIndex() == 0

def test_update_tree_with_data(tree_table):
    # Crear datos de ejemplo con polars
    df = pl.DataFrame({
        "uci": ["e2e4", "d2d4"],
        "w": [10, 5],
        "d": [5, 2],
        "b": [5, 3],
        "c": [20, 10],
        "avg_w_elo": [2500, 2400],
        "avg_b_elo": [2450, 2350]
    })
    
    board = chess.Board()
    tree_table.update_tree(df, board, "Apertura de Peón de Rey")
    
    assert tree_table.table.rowCount() == 2
    assert tree_table.label_eco.text() == "Apertura de Peón de Rey"
    # El primer movimiento debería ser e2e4 (index 1 en la tabla tras ordenación por 'c')
    # Nota:sortByColumn(3, Descending) pone e2e4 primero
    assert tree_table.table.item(0, 1).text() == "e4"

def test_update_branch_evals(tree_table):
    df = pl.DataFrame({
        "uci": ["e2e4"], "w": [10], "d": [5], "b": [5], "c": [20],
        "avg_w_elo": [2500], "avg_b_elo": [2450]
    })
    board = chess.Board()
    tree_table.update_tree(df, board, "Test")
    
    # Simular evaluación del motor para e4
    tree_table.update_branch_evals({"e2e4": "+0.50"}, is_white_turn=True)
    assert tree_table.table.item(0, 2).text() == "+0.50"

def test_tree_table_signals(tree_table, qtbot):
    df = pl.DataFrame({
        "uci": ["e2e4"], "w": [10], "d": [5], "b": [5], "c": [20],
        "avg_w_elo": [2500], "avg_b_elo": [2450]
    })
    tree_table.update_tree(df, chess.Board(), "Test")
    
    with qtbot.waitSignal(tree_table.move_selected) as blocker:
        # Doble clic en la primera fila
        tree_table.table.itemDoubleClicked.emit(tree_table.table.item(0, 1))
    
    assert blocker.args == ["e2e4"]
