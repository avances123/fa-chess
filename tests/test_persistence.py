import pytest
import os
import polars as pl
import chess
from src.ui.main_window import MainWindow
from src.core.db_manager import DBManager
from PySide6.QtCore import Qt

@pytest.fixture
def app(qtbot, tmp_path, monkeypatch):
    """Fixture que redirige archivos de config y clipbase a una carpeta temporal"""
    # Mock de rutas para no tocar los archivos reales del usuario
    test_config = tmp_path / "test_config.json"
    test_clipbase = tmp_path / "test_clipbase.parquet"
    
    # Crear una clipbase vacía pero válida
    schema = {
        "id": pl.Int64, "white": pl.String, "black": pl.String,
        "w_elo": pl.Int64, "b_elo": pl.Int64, "result": pl.String,
        "date": pl.String, "event": pl.String, "site": pl.String,
        "line": pl.String, "full_line": pl.String, "fens": pl.List(pl.UInt64)
    }
    pl.DataFrame(schema=schema).write_parquet(test_clipbase)
    
    # Inyectar las rutas en los módulos correspondientes
    import src.config
    import src.core.db_manager
    monkeypatch.setattr(src.config, "CONFIG_FILE", str(test_config))
    monkeypatch.setattr(src.core.db_manager, "CLIPBASE_FILE", str(test_clipbase))
    
    window = MainWindow()
    qtbot.addWidget(window)
    return window

def test_add_to_clipbase_persistence(app, qtbot):
    """Verifica que guardar una partida en Clipbase funciona y actualiza la UI"""
    # 1. Hacer algunas jugadas
    app.game.load_uci_line("e2e4 e7e5 g1f3")
    
    # 2. Pulsar botón de guardar en Clipbase
    app.add_to_clipbase()
    
    # 3. Cambiar a Clipbase y verificar que la partida aparece
    app.db.set_active_db("Clipbase")
    app.refresh_db_list()
    
    assert app.db_table.rowCount() == 1
    
    # Verificar que el motor de datos tiene la partida
    df = app.db.get_active_df()
    assert df.height == 1
    assert df.row(0, named=True)["white"] == "Jugador Blanco"

def test_export_to_pgn(app, qtbot, tmp_path):
    """Verifica la exportación de partidas a formato PGN"""
    # 1. Inyectar un par de partidas en la base activa
    df = pl.DataFrame({
        "id": [1], "white": ["W1"], "black": ["B1"], "w_elo": [2000], "b_elo": [2000],
        "result": ["1-0"], "date": ["2024"], "event": ["E"], "site": [""],
        "line": ["e4"], "full_line": ["e2e4"], "fens": [[1, 2]]
    })
    app.db.dbs["Clipbase"] = df.lazy()
    app.db.set_active_db("Clipbase")
    
    # 2. Definir ruta de salida
    pgn_output = tmp_path / "output.pgn"
    
    # 3. Lanzar worker de exportación
    from src.core.workers import PGNExportWorker
    worker = PGNExportWorker(df, str(pgn_output))
    
    with qtbot.wait_signal(worker.finished, timeout=5000):
        worker.start()
        
    # 4. Verificar que el archivo existe y tiene contenido PGN
    assert os.path.exists(pgn_output)
    content = pgn_output.read_text()
    assert '[White "W1"]' in content
    assert "1-0" in content

def test_create_and_delete_db_file(app, qtbot, tmp_path):
    """Verifica la creación y borrado físico de bases de datos .parquet"""
    new_db_path = tmp_path / "new_test_db.parquet"
    
    # 1. Crear base
    app.db.create_new_database(str(new_db_path))
    assert os.path.exists(new_db_path)
    
    # 2. Cargarla en la UI
    app.load_parquet(str(new_db_path))
    name = os.path.basename(str(new_db_path))
    assert name in app.db.dbs
    
    # 3. Borrarla
    success = app.db.delete_database_from_disk(name)
    assert success
    assert not os.path.exists(new_db_path)
    assert name not in app.db.dbs