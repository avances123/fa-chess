import pytest
import os
import polars as pl
from src.core.db_manager import DBManager

def test_db_manager_init(temp_env):
    """Verifica que el DBManager se inicia con una Clipbase vacía y volátil."""
    manager = DBManager()
    assert manager.active_db_name == "Clipbase"
    assert manager.get_active_count() == 0
    
    # No debe existir archivo físico tras guardar (es volátil)
    manager.save_clipbase()
    assert not os.path.exists(temp_env["clipbase"])

def test_load_and_filter_parquet(temp_env, sample_parquet):
    """Prueba la carga de una base externa y la aplicación de filtros."""
    manager = DBManager()
    name = manager.load_parquet(sample_parquet)
    
    assert name == os.path.basename(sample_parquet)
    assert manager.get_active_count() == 2
    
    # Probar filtro por jugador
    manager.filter_db({"white": "Carlsen"})
    assert manager.get_view_count() == 1
    
    # Probar filtro por resultado
    manager.filter_db({"result": "1/2-1/2"})
    assert manager.get_view_count() == 1
    assert manager.current_filter_df.row(0, named=True)["black"] == "Karpov"

def test_edit_game_logic(temp_env, sample_parquet):
    """Verifica que se pueden editar partidas en una base (vía Lazy logic)."""
    manager = DBManager()
    name = manager.load_parquet(sample_parquet)
    
    # Cambiamos el nombre del blanco en la partida con ID 1
    success = manager.update_game(name, 1, {"white": "Magnus"})
    assert success is True
    
    # Verificamos el cambio
    updated_game = manager.get_game_by_id(name, 1)
    assert updated_game["white"] == "Magnus"
    assert updated_game["black"] == "Nepo" # Se mantiene el resto

def test_delete_database_logic(temp_env, sample_parquet):
    """Verifica la eliminación física de una base de datos."""
    manager = DBManager()
    name = manager.load_parquet(sample_parquet)
    assert os.path.exists(sample_parquet)
    
    success = manager.delete_database_from_disk(name)
    assert success is True
    assert not os.path.exists(sample_parquet)
    assert name not in manager.dbs
    # Debería haber vuelto a la Clipbase
    assert manager.active_db_name == "Clipbase"
