import pytest
import polars as pl
import chess
import chess.polyglot
from PySide6.QtCore import Qt
from ui.main_window import MainWindow
import ui.main_window

@pytest.fixture
def app(qtbot, tmp_path, monkeypatch):
    test_config = tmp_path / "test_config.json"
    monkeypatch.setattr(ui.main_window, "CONFIG_FILE", str(test_config))
    window = MainWindow()
    qtbot.addWidget(window)
    return window

def test_full_position_search_logic(app, qtbot):
    # Generar hashes exactos
    board_italiana = chess.Board()
    h_init = int(chess.polyglot.zobrist_hash(board_italiana))
    
    # Italiana: 1. e4 e5 2. Nf3 Nc6 3. Bc4
    moves_it = ["e2e4", "e7e5", "g1f3", "b8c6", "f1c4"]
    hashes_it = [h_init]
    for m in moves_it:
        board_italiana.push_uci(m)
        hashes_it.append(int(chess.polyglot.zobrist_hash(board_italiana)))
    
    # Siciliana: 1. e4 c5
    board_siciliana = chess.Board()
    moves_sic = ["e2e4", "c7c5"]
    hashes_sic = [h_init]
    for m in moves_sic:
        board_siciliana.push_uci(m)
        hashes_sic.append(int(chess.polyglot.zobrist_hash(board_siciliana)))

    df = pl.DataFrame([
        {
            "id": 1, "white": "Blanco Italiana", "black": "Negro A",
            "w_elo": 2000, "b_elo": 2000, "result": "1-0",
            "date": "2023", "event": "Test",
            "line": " ".join(moves_it), 
            "full_line": " ".join(moves_it), 
            "fens": hashes_it
        },
        {
            "id": 2, "white": "Blanco Siciliana", "black": "Negro B",
            "w_elo": 2000, "b_elo": 2000, "result": "0-1",
            "date": "2023", "event": "Test",
            "line": " ".join(moves_sic), 
            "full_line": " ".join(moves_sic), 
            "fens": hashes_sic
        }
    ], schema_overrides={"fens": pl.List(pl.UInt64)})
    
    app.db.dbs["Clipbase"] = df
    app.db.set_active_db("Clipbase")
    app.refresh_db_list()
    assert app.db_table.rowCount() == 2

    # ESCENARIO A: Buscar posición tras 3. Bc4 (Solo Italiana)
    # Replicamos exactamente la posición final de moves_it
    app.game.reset()
    for m in moves_it:
        app.game.make_move(chess.Move.from_uci(m))
    
    app.search_current_position()
    assert app.db_table.rowCount() == 1
    assert app.db_table.item(0, 1).text() == "Blanco Italiana"

    # ESCENARIO B: Buscar posición tras 1. e4 (Ambas)
    app.btn_clear_filter.click()
    app.game.reset()
    app.game.make_move(chess.Move.from_uci("e2e4"))
    app.search_current_position()
    assert app.db_table.rowCount() == 2

    # ESCENARIO C: Buscar posición tras 1. d4 (Ninguna)
    app.btn_clear_filter.click()
    app.game.reset()
    app.game.make_move(chess.Move.from_uci("d2d4"))
    app.search_current_position()
    assert app.db_table.rowCount() == 0
