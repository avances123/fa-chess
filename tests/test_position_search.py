import pytest
import polars as pl
import chess
import chess.polyglot
from PySide6.QtCore import Qt
from ui.main_window import MainWindow
import ui.main_window

@pytest.fixture
def app(qtbot, tmp_path, monkeypatch):
    # Mock de la ruta de configuración
    test_config = tmp_path / "test_config.json"
    monkeypatch.setattr(ui.main_window, "CONFIG_FILE", str(test_config))
    
    window = MainWindow()
    qtbot.addWidget(window)
    return window

def test_position_search_flow(app, qtbot):
    # 1. Preparar una base de datos con una partida que conocemos
    board = chess.Board()
    h0 = chess.polyglot.zobrist_hash(board) # Posición inicial
    board.push_uci("e2e4")
    h1 = chess.polyglot.zobrist_hash(board)
    board.push_uci("b7b6")
    h2 = chess.polyglot.zobrist_hash(board)
    
    df = pl.DataFrame({
        "id": [1], "white": ["Jugador A"], "black": ["Jugador B"],
        "w_elo": [2500], "b_elo": [2500], "result": ["1-0"],
        "date": ["2023"], "event": ["Test"],
        "line": ["e2e4 b7b6"], 
        "full_line": ["e2e4 b7b6"], 
        "fens": [[h0, h1, h2]]
    })
    
    app.db.dbs["Clipbase"] = df
    app.db.set_active_db("Clipbase")
    
    # 2. Situar el tablero en la posición tras 1. e4
    app.game.reset()
    app.game.make_move(chess.Move.from_uci("e2e4"))
    
    # 3. Llamar directamente al método de búsqueda
    # Usamos el método directamente para evitar problemas de foco con el atajo 'S' en CI
    app.search_current_position()
    
    # 4. Verificar resultados
    assert app.tabs.currentIndex() == 1 # Cambia a la pestaña de Gestor
    assert app.db_table.rowCount() == 1
    assert app.label_db_stats.text() == "[1/1]"
    
    # 5. Mover a una posición que NO esté en la base (ej: h3)
    app.game.reset()
    app.game.make_move(chess.Move.from_uci("h2h3"))
    app.search_current_position()
    
    # No debería encontrar nada
    assert app.db_table.rowCount() == 0
    assert app.label_db_stats.text() == "[0/1]"

def test_position_search_button_visibility(app):
    # Verificar que el botón existe en la toolbar
    found = False
    for action in app.toolbar_ana.actions():
        if "Buscar partidas con esta posición" in action.toolTip():
            found = True
            break
    assert found
