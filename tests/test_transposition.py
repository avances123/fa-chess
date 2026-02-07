import pytest
import polars as pl
import chess
import chess.polyglot
from ui.main_window import MainWindow
import ui.main_window
import core.db_manager

@pytest.fixture
def app(qtbot, tmp_path, monkeypatch):
    test_config = tmp_path / "test_config.json"
    test_clipbase = tmp_path / "test_clipbase.parquet"
    monkeypatch.setattr(ui.main_window, "CONFIG_FILE", str(test_config))
    monkeypatch.setattr(core.db_manager, "CLIPBASE_FILE", str(test_clipbase))
    
    window = MainWindow()
    qtbot.addWidget(window)
    return window

def get_hashes(moves):
    b = chess.Board()
    h = [int(chess.polyglot.zobrist_hash(b))]
    for m in moves:
        b.push_san(m)
        h.append(int(chess.polyglot.zobrist_hash(b)))
    return h

def test_transposition_detection(app, qtbot):
    # 1. Crear base con transposiciones
    moves_a = ["e4", "e5", "Nf3", "Nc6"]
    hashes_a = get_hashes(moves_a)
    
    moves_b = ["Nf3", "Nc6", "e4", "e5"]
    hashes_b = get_hashes(moves_b)
    
    # Ambas partidas siguen con d4 (Escocesa) para tener un movimiento común
    df = pl.DataFrame({
        "id": [1, 2],
        "white": ["A", "B"], "black": ["A", "B"],
        "w_elo": [2000, 2000], "b_elo": [2000, 2000],
        "result": ["*", "*"], "date": ["", ""], "event": ["", ""],
        "line": ["", ""],
        "full_line": ["e2e4 e7e5 g1f3 b8c6 d2d4", "g1f3 b8c6 e2e4 e7e5 d2d4"],
        "fens": [hashes_a, hashes_b],
        "evals": [[], []]
    }, schema_overrides={"id": pl.Int64, "fens": pl.List(pl.UInt64), "evals": pl.List(pl.Int64)})

    app.db.dbs["Clipbase"] = df
    app.db.set_active_db("Clipbase")
    
    # 2. Llegar a la posición por el Camino A
    app.game.reset()
    for m in moves_a:
        app.game.make_move(chess.Move.from_uci(app.game.board.parse_san(m).uci()))
        
    # Verificar hash actual
    current_hash = chess.polyglot.zobrist_hash(app.game.board)
    assert int(current_hash) in hashes_a
    assert int(current_hash) in hashes_b
    
    # 3. Ejecutar StatsWorker (Transposición)
    app.run_stats_worker()
    
    # Esperar a que termine
    with qtbot.waitSignal(app.stats_worker.finished, timeout=2000) as blocker:
        pass
        
    stats_df = blocker.args[0]
    
    # 4. Verificar Resultados
    assert stats_df is not None
    assert stats_df.height == 1 
    row = stats_df.row(0, named=True)
    assert row["uci"] == "d2d4"
    assert row["c"] == 2 # ¡ÉXITO!
