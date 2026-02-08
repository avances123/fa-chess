import pytest
import polars as pl
from core.db_manager import DBManager
from PySide6.QtTest import QSignalSpy
from PySide6.QtCore import Qt

def test_db_manager_signal_logic():
    """Prueba la lógica de señales del DBManager sin levantar ninguna UI"""
    db = DBManager()
    
    # Espías para las señales
    spy_active = QSignalSpy(db.active_db_changed)
    spy_filter = QSignalSpy(db.filter_updated)
    
    # 1. Probar cambio de base
    db.set_active_db("Clipbase")
    assert spy_active.count() >= 1
    assert spy_filter.count() >= 1
    assert db.current_filter_df is None # Filtro reseteado
    
    # 2. Probar aplicación de filtro
    # Mockeamos una base de datos con una partida para que el filtro no devuelva None
    db.dbs["Clipbase"] = pl.DataFrame({
        "id": [1], "white": ["Carlsen"], "black": ["Caruana"],
        "w_elo": [2850], "b_elo": [2830], "result": ["1-0"],
        "date": ["2024.01.01"], "event": ["Test"], "line": [""],
        "full_line": [""], "fens": [[123]]
    })
    
    db.filter_db({"white": "Carlsen"})
    assert spy_filter.count() >= 2
    assert db.current_filter_df is not None
    assert db.current_filter_df.height == 1
    
    # 3. Probar limpieza (set_active_db de nuevo)
    db.set_active_db("Clipbase")
    assert db.current_filter_df is None
    # La señal filter_updated debe emitir None al limpiar
    last_signal_data = spy_filter.at(spy_filter.count() - 1)[0]
    assert last_signal_data is None
