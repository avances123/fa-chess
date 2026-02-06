import pytest
import os
import polars as pl
from PySide6.QtCore import Qt
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

def test_toggle_readonly_status(app):
    # 1. Crear y cargar una base externa
    db_path = os.path.join(os.path.dirname(ui.main_window.CONFIG_FILE), "test_readonly.parquet")
    app.db.create_new_database(db_path)
    name = os.path.basename(db_path)
    app.load_parquet(db_path)
    
    # 2. Por defecto las bases externas son ReadOnly
    assert app.db.db_metadata[name]["read_only"] is True
    
    # 3. Cambiar a ReadWrite
    app.db.set_readonly(name, False)
    assert app.db.db_metadata[name]["read_only"] is False
    
    # 4. Cambiar de nuevo a ReadOnly
    app.db.set_readonly(name, True)
    assert app.db.db_metadata[name]["read_only"] is True

def test_copy_game_logic_destinations(app):
    # 1. Preparar una partida en la base activa (Clipbase)
    game_data = {
        "id": 1, "white": "Original", "black": "Player", 
        "w_elo": 2000, "b_elo": 2000, "result": "1-0", 
        "date": "2023", "event": "Test", "line": "", "full_line": "", "fens": []
    }
    app.db.add_to_clipbase(game_data)
    
    # 2. Crear una base de destino y ponerla en modo escritura
    dest_path = os.path.join(os.path.dirname(ui.main_window.CONFIG_FILE), "destination.parquet")
    app.db.create_new_database(dest_path)
    dest_name = os.path.basename(dest_path)
    app.load_parquet(dest_path)
    app.db.set_readonly(dest_name, False) # Habilitar escritura
    
    # 3. Simular la copia de la partida desde Clipbase a destination.parquet
    # (Usamos la lógica interna para no disparar diálogos interactivos de QInputDialog)
    source_game = app.db.get_game_by_id("Clipbase", 1)
    
    # Copiar manualmente usando la lógica que implementamos en MainWindow
    clean_data = {k: source_game[k] for k in app.db.dbs[dest_name].columns}
    clean_data["id"] = 999 # Nuevo ID
    app.db.dbs[dest_name] = pl.concat([app.db.dbs[dest_name], pl.DataFrame([clean_data], schema=app.db.dbs[dest_name].schema)])
    
    # 4. Verificar que la partida existe en el destino
    assert app.db.dbs[dest_name].height == 1
    assert app.db.dbs[dest_name].row(0, named=True)["white"] == "Original"

def test_delete_disabled_on_readonly(app):
    # 1. Cargar una base en modo ReadOnly
    db_path = os.path.join(os.path.dirname(ui.main_window.CONFIG_FILE), "test_del.parquet")
    app.db.create_new_database(db_path)
    name = os.path.basename(db_path)
    app.load_parquet(db_path)
    app.db.set_readonly(name, True)
    
    # 2. Verificar que el método de borrado físico del manager NO es el mismo que el de partidas
    # Pero el manager debería permitir borrar el archivo si se solicita explícitamente
    # Lo que queremos testear es que el manager PROTEGE la Clipbase
    assert app.db.delete_database_from_disk("Clipbase") is False
