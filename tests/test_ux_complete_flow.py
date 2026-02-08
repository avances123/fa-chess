import pytest
import polars as pl
from core.db_manager import DBManager
from PySide6.QtTest import QSignalSpy

@pytest.fixture
def db_with_data():
    db = DBManager()
    # Creamos un dataset de prueba con 3 partidas
    db.dbs["Clipbase"] = pl.DataFrame({
        "id": [1, 2, 3],
        "white": ["Carlsen", "Kasparov", "Carlsen"],
        "black": ["Caruana", "Deep Blue", "Anand"],
        "w_elo": [2850, 2800, 2840],
        "b_elo": [2830, 2000, 2750],
        "result": ["1-0", "0-1", "1/2-1/2"],
        "date": ["2024.01.01", "1997.02.01", "2013.01.01"],
        "event": ["Test", "IBM", "Test"],
        "line": ["", "", ""],
        "full_line": ["", "", ""],
        "fens": [[100, 101], [200, 201], [100, 300]] # La pos 100 está en la partida 1 y 3
    })
    return db

def test_flow_combined_filters(db_with_data):
    """Prueba filtros de nombre, elo y resultado combinados"""
    db = db_with_data
    
    # Filtrar por Carlsen y ELO > 2845
    criteria = {"white": "Carlsen", "min_elo": "2845", "result": "1-0"}
    db.filter_db(criteria)
    
    assert db.current_filter_df.height == 1
    assert db.current_filter_df["white"][0] == "Carlsen"
    assert db.current_filter_df["w_elo"][0] == 2850

def test_flow_invert_filter(db_with_data):
    """Prueba que la inversión de filtro funciona matemáticamente"""
    db = db_with_data
    
    # Primero filtramos 1 partida (Carlsen vs Caruana)
    db.filter_db({"white": "Carlsen", "black": "Caruana"})
    assert db.current_filter_df.height == 1
    
    # Invertimos: deberían quedar las otras 2 partidas
    db.invert_filter()
    assert db.current_filter_df.height == 2
    assert 1 not in db.current_filter_df["id"].to_list() # El ID 1 ya no debe estar

def test_flow_position_search(db_with_data):
    """Prueba la búsqueda por hash de posición (Diana)"""
    db = db_with_data
    
    # Buscamos la posición con hash 100 (está en partidas 1 y 3)
    db.filter_db({"position_hash": 100})
    
    assert db.current_filter_df.height == 2
    ids = db.current_filter_df["id"].to_list()
    assert 1 in ids
    assert 3 in ids
    assert 2 not in ids

def test_signal_consistency(db_with_data):
    """Verifica que las señales emiten los estados de color correctos"""
    db = db_with_data
    spy_filter = QSignalSpy(db.filter_updated)
    
    # 1. Aplicar filtro -> Debe emitir un DataFrame (UI se pondrá VERDE)
    db.filter_db({"white": "Carlsen"})
    assert isinstance(spy_filter.at(0)[0], pl.DataFrame)
    
    # 2. Resetear base -> Debe emitir None (UI se pondrá GRIS)
    db.set_active_db("Clipbase")
    assert spy_filter.at(1)[0] is None

def test_empty_results_flow(db_with_data):
    """Prueba que pasa cuando un filtro no encuentra nada"""
    db = db_with_data
    db.filter_db({"white": "Inexistente"})
    
    assert db.current_filter_df is not None
    assert db.current_filter_df.height == 0
    # La UI debería mostrar [0/3] en verde
