import pytest
import os
import polars as pl
from ui.main_window import MainWindow
import ui.main_window

@pytest.fixture
def app(qtbot, tmp_path, monkeypatch):
    # Mock de rutas para no tocar archivos reales
    test_config = tmp_path / "test_config.json"
    test_clipbase = tmp_path / "test_clipbase.parquet"
    
    monkeypatch.setattr(ui.main_window, "CONFIG_FILE", str(test_config))
    # Redirigir la Clipbase del DBManager a una ruta temporal
    import core.db_manager
    monkeypatch.setattr(core.db_manager, "CLIPBASE_FILE", str(test_clipbase))
    
    window = MainWindow()
    qtbot.addWidget(window)
    return window, test_clipbase

def test_clipbase_persistence(app, qtbot):
    window, clipbase_path = app
    
    # 1. Asegurar que Clipbase está vacía inicialmente
    assert window.db.dbs["Clipbase"].height == 0
    
    # 2. Añadir una partida a la Clipbase
    game_data = {
        "id": 1, "white": "Test", "black": "Test", 
        "w_elo": 2000, "b_elo": 2000, "result": "*", 
        "date": "2023", "event": "Test", "line": "", "full_line": "", "fens": []
    }
    window.db.add_to_clipbase(game_data)
    assert window.db.dbs["Clipbase"].height == 1
    
    # 3. Cerrar la ventana (esto dispara save_clipbase)
    window.close()
    
    # 4. Verificar que el archivo se ha creado físicamente
    assert os.path.exists(clipbase_path)
    
    # 5. Verificar contenido del archivo
    persisted_df = pl.read_parquet(clipbase_path)
    assert persisted_df.height == 1
    assert persisted_df.row(0, named=True)["white"] == "Test"

def test_create_new_database_file(app, tmp_path):
    window, _ = app
    new_db_path = str(tmp_path / "manual_new.parquet")
    name = os.path.basename(new_db_path)
    
    # Simular la acción de crear base (saltando el diálogo de archivo)
    window.db.create_new_database(new_db_path)
    window.load_parquet(new_db_path)
    
    # 1. Verificar existencia física
    assert os.path.exists(new_db_path)
    
    # 2. Verificar que aparece en la UI (list widget)
    items = [window.db_list_widget.item(i).text() for i in range(window.db_list_widget.count())]
    assert name in items
    
    # 3. Verificar que es la activa
    assert window.db.active_db_name == name
    
    # 4. Verificar esquema
    df = pl.read_parquet(new_db_path)
    assert "fens" in df.columns
    assert df.height == 0

def test_delete_database_from_disk(app, tmp_path, qtbot):
    window, _ = app
    db_to_delete = str(tmp_path / "temp_to_del.parquet")
    
    # 1. Crear y cargar una base
    window.db.create_new_database(db_to_delete)
    name = os.path.basename(db_to_delete)
    window.load_parquet(db_to_delete)
    assert name in window.db.dbs
    
    # 2. Borrar físicamente
    # Forzamos la llamada al manager (evitando el diálogo de confirmación de UI en test)
    success = window.db.delete_database_from_disk(name)
    
    # 3. Verificar resultados
    assert success is True
    assert not os.path.exists(db_to_delete)
    assert name not in window.db.dbs
    assert window.db.active_db_name == "Clipbase"
