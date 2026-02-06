import pytest
import os
import polars as pl
import chess.pgn
import io
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

def test_export_filter_to_pgn_logic(app, tmp_path, qtbot, monkeypatch):
    # 1. Preparar datos en la base (2 partidas)
    df = pl.DataFrame({
        "id": [1, 2],
        "white": ["Carlsen", "Kasparov"],
        "black": ["Caruana", "Karpov"],
        "w_elo": [2850, 2800], "b_elo": [2830, 2750],
        "result": ["1-0", "1/2-1/2"],
        "date": ["2023.01.01", "1985.01.01"],
        "event": ["Test Event", "WC"],
        "line": ["e2e4", "d2d4"],
        "full_line": ["e2e4 e7e5", "d2d4 d5"],
        "fens": [[0, 1, 2], [0, 3, 4]]
    }, schema_overrides={"fens": pl.List(pl.UInt64)})
    
    app.db.dbs["Clipbase"] = df
    app.db.set_active_db("Clipbase")
    
    # 2. Aplicar un filtro (solo Carlsen)
    app.db.filter_db({"white": "Carlsen", "black": "", "min_elo": "", "result": "Cualquiera"})
    assert app.db.current_filter_df.height == 1
    
    # 3. Definir ruta de exportación temporal
    export_path = str(tmp_path / "exported.pgn")
    
    # 4. Mockear el QFileDialog para que devuelva nuestra ruta sin abrir ventana
    from PySide6.QtWidgets import QFileDialog
    def mock_get_save_filename(*args, **kwargs):
        return export_path, "Chess PGN (*.pgn)"
    
    import PySide6.QtWidgets
    
    # Mockear QMessageBox para evitar diálogos modales reales
    monkeypatch.setattr(PySide6.QtWidgets.QMessageBox, "information", lambda *args: None)
    monkeypatch.setattr(PySide6.QtWidgets.QMessageBox, "critical", lambda *args: None)

    with pytest.MonkeyPatch().context() as m:
        m.setattr(PySide6.QtWidgets.QFileDialog, "getSaveFileName", mock_get_save_filename)
        # Ejecutar la exportación
        app.export_filter_to_pgn()
        
        # Esperar a que el worker termine (asíncrono)
        with qtbot.waitSignal(app.export_worker.finished, timeout=5000):
            pass
    
    # 5. Verificar que el archivo existe
    assert os.path.exists(export_path)
    
    # 6. Verificar contenido del PGN exportado
    with open(export_path, "r") as f:
        pgn_content = f.read()
        # Debe contener a Carlsen pero NO a Kasparov
        assert "Carlsen" in pgn_content
        assert "Kasparov" not in pgn_content
        assert "1. e4 e5" in pgn_content or "1. e4" in pgn_content # Depende de cómo se guarde
        
    # Verificar legibilidad por la librería chess
    with open(export_path) as f:
        game = chess.pgn.read_game(f)
        assert game.headers["White"] == "Carlsen"
        assert game.headers["Result"] == "1-0"
