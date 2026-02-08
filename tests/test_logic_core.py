import pytest
import polars as pl
from src.core.db_manager import DBManager
from PySide6.QtTest import QSignalSpy

@pytest.fixture
def db():
    manager = DBManager()
    # Inyectar datos de prueba en modo Lazy para simular una base real
    df = pl.DataFrame({
        "id": [1, 2, 3],
        "white": ["Carlsen", "Kasparov", "Carlsen"],
        "black": ["Caruana", "Deep Blue", "Anand"],
        "w_elo": [2850, 2800, 2840],
        "b_elo": [2830, 2000, 2750],
        "result": ["1-0", "0-1", "1/2-1/2"],
        "date": ["2024.01.01", "1997.02.01", "2013.01.01"],
        "event": ["Tournament A", "IBM Match", "Tournament A"],
        "site": ["https://lichess.org/1", "https://lichess.org/2", ""],
        "line": ["", "", ""],
        "full_line": ["e2e4 e7e5", "d2d4 d5", "e2e4 c5"],
        "fens": [[100, 101], [200, 201], [100, 102]]
    })
    manager.dbs["Clipbase"] = df.lazy()
    manager.active_db_name = "Clipbase"
    manager.reset_to_full_base()
    return manager

def test_db_lazy_filtering(db):
    """Verifica que el filtrado produce una consulta Lazy y una muestra Eager"""
    criteria = {"white": "Carlsen"}
    db.filter_db(criteria)
    
    # La consulta completa debe ser Lazy
    assert isinstance(db.current_filter_query, pl.LazyFrame)
    # La vista previa debe ser Eager
    assert isinstance(db.current_filter_df, pl.DataFrame)
    assert db.current_filter_df.height == 2
    assert db.get_view_count() == 2

def test_db_position_search_logic(db):
    """Verifica que la búsqueda por posición (hash) funciona sobre LazyFrames"""
    # Hash 100 está en partidas 1 y 3
    db.filter_db({"position_hash": 100})
    assert db.get_view_count() == 2
    
    # Hash 200 solo en partida 2
    db.filter_db({"position_hash": 200})
    assert db.get_view_count() == 1

def test_db_signals_on_filter(db):
    """Verifica que se emiten las señales correctas al filtrar"""
    spy = QSignalSpy(db.filter_updated)
    db.filter_db({"white": "Carlsen"})
    assert spy.count() == 1
    # El primer argumento de la señal debe ser el DataFrame de previsualización
    assert isinstance(spy.at(0)[0], pl.DataFrame)
