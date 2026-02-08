import pytest
import polars as pl
import chess
import chess.polyglot
from src.ui.main_window import MainWindow
from PySide6.QtCore import Qt
from PySide6.QtTest import QTest

@pytest.fixture
def app(qtbot):
    """Fixture para inicializar la ventana con datos de prueba realistas"""
    window = MainWindow()
    qtbot.addWidget(window)
    
    # 1. Definir posiciones y sus hashes exactos
    board_start = chess.Board()
    h_start = chess.polyglot.zobrist_hash(board_start)
    
    board_e4 = chess.Board()
    board_e4.push_uci("e2e4")
    h_e4 = chess.polyglot.zobrist_hash(board_e4)
    
    # 2. Datos: 2 partidas con 1.e4 e7e5 y 1.e4 c7c5
    # IMPORTANTE: Usar UCIs reales de 4 letras (e7e5, c7c5)
    df = pl.DataFrame({
        "id": [1, 2, 3],
        "white": ["P1", "P2", "P3"],
        "black": ["O1", "O2", "O3"],
        "w_elo": [2500, 2500, 2500],
        "b_elo": [2500, 2500, 2500],
        "result": ["1-0", "0-1", "1/2-1/2"],
        "date": ["2024", "2024", "2024"],
        "event": ["E", "E", "E"],
        "site": ["", "", ""],
        "line": ["e2e4", "e2e4", "d2d4"],
        "full_line": ["e2e4 e7e5", "e2e4 c7c5", "d2d4 d7d5"],
        "fens": [
            [h_start, h_e4],
            [h_start, h_e4],
            [h_start] 
        ]
    })
    window.db.dbs["Clipbase"] = df.lazy()
    window.db.set_active_db("Clipbase")
    return window

def wait_for_ui(qtbot):
    """Espera suficiente para que los workers y timers terminen"""
    qtbot.wait(600)

def test_tree_updates_on_move(app, qtbot):
    """Verifica que al mover e2e4, el árbol muestra e5 y c5 (notación SAN)"""
    app.game.make_move(chess.Move.from_uci("e2e4"))
    wait_for_ui(qtbot)
    
    moves_found = []
    for r in range(app.opening_tree.table.rowCount()):
        item = app.opening_tree.table.item(r, 0)
        if item: moves_found.append(item.text())
    
    assert "e5" in moves_found
    assert "c5" in moves_found

def test_make_move_from_tree(app, qtbot):
    """Verifica que hacer clic en el árbol mueve la pieza correctamente"""
    app.game.make_move(chess.Move.from_uci("e2e4"))
    wait_for_ui(qtbot)
        
    # Obtener la primera jugada sugerida (e7e5 o c7c5)
    assert app.opening_tree.table.rowCount() > 0
    item = app.opening_tree.table.item(0, 0)
    uci_to_move = item.data(Qt.UserRole)
    
    # Simular doble clic vía señal
    app.opening_tree.move_selected.emit(uci_to_move)
    
    # Verificar que el motor ha avanzado a la jugada elegida
    assert app.game.board.move_stack[-1].uci() == uci_to_move
    assert len(app.game.board.move_stack) == 2

def test_eco_detection_flow(app, qtbot):
    """Verifica que el nombre de la apertura se actualiza en el árbol"""
    app.game.make_move(chess.Move.from_uci("e2e4"))
    app.game.make_move(chess.Move.from_uci("c7c5")) # Siciliana
    
    wait_for_ui(qtbot)
        
    eco_text = app.opening_tree.label_eco.text()
    assert "Sicilian" in eco_text or "B20" in eco_text