import pytest
import polars as pl
import chess
import chess.polyglot
from src.ui.main_window import MainWindow

@pytest.fixture
def app(qtbot):
    """Ventana con una partida que contiene una posición específica"""
    window = MainWindow()
    qtbot.addWidget(window)
    
    # Vamos a usar la posición tras 1.d4 d5 2.c4
    board = chess.Board()
    board.push_uci("d2d4")
    board.push_uci("d7d5")
    board.push_uci("c2c4")
    target_hash = chess.polyglot.zobrist_hash(board)
    
    df = pl.DataFrame({
        "id": [1],
        "white": ["Gambit Player"],
        "black": ["Defender"],
        "w_elo": [2500], "b_elo": [2500], "result": ["*"],
        "date": ["2024"], "event": ["E"], "site": [""],
        "line": ["d4 d5 c4"],
        "full_line": ["d2d4 d7d5 c2c4"],
        "fens": [[target_hash]] # Simplificado para el test
    })
    window.db.dbs["Clipbase"] = df.lazy()
    window.db.set_active_db("Clipbase")
    return window, target_hash

def test_transposition_search(app, qtbot):
    """Verifica que encontramos la posición aunque el orden de jugadas sea distinto"""
    window, target_hash = app
    
    # 1. Llegar a la MISMA posición por un orden distinto: 1.c4 d5 2.d4
    board_transpo = chess.Board()
    board_transpo.push_uci("c2c4")
    board_transpo.push_uci("d7d5")
    board_transpo.push_uci("d2d4")
    
    # El hash DEBE ser idéntico
    transpo_hash = chess.polyglot.zobrist_hash(board_transpo)
    assert transpo_hash == target_hash
    
    # 2. Poner el motor de juego en esa posición
    window.game.board = board_transpo
    
    # 3. Ejecutar búsqueda por posición (como si el usuario pulsara 'S')
    window.search_current_position()
    
    # 4. Verificar que se ha encontrado la partida
    assert window.db_table.rowCount() == 1
    assert "Gambit Player" in window.db_table.item(0, 2).text()