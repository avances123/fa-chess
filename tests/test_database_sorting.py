import pytest
import polars as pl
from src.ui.main_window import MainWindow
from PySide6.QtCore import Qt

@pytest.fixture
def app(qtbot):
    """Ventana con datos diseñados para probar ordenación"""
    window = MainWindow()
    qtbot.addWidget(window)
    
    df = pl.DataFrame({
        "id": [1, 2, 3],
        "white": ["Z", "A", "M"], # Orden alfabético: A, M, Z
        "w_elo": [2000, 2800, 2400], # Orden numérico: 2000, 2400, 2800
        "black": ["B1", "B2", "B3"],
        "b_elo": [2100, 2100, 2100],
        "result": ["1-0", "0-1", "1/2-1/2"],
        "date": ["2020.01.01", "2024.01.01", "2010.01.01"], # Orden crono: 2010, 2020, 2024
        "event": ["E", "E", "E"], "site": ["", "", ""],
        "line": ["", "", ""], "full_line": ["", "", ""], "fens": [[], [], []]
    })
    window.db.dbs["Clipbase"] = df.lazy()
    window.db.set_active_db("Clipbase")
    return window

def test_sort_by_white_player(app, qtbot):
    """Verifica la ordenación alfabética por el nombre del jugador blanco"""
    # 1. Simular clic en la cabecera "Blancas" (columna 2)
    # Primera vez suele ser Ascendente o Descendente según lógica de UI
    app.sort_database(2) # Columna Blancas
    
    # 2. Verificar que el primer elemento es "Z" (Si la UI empieza por Descendente por defecto)
    # o "A" (Si es Ascendente). Nuestra MainWindow pone sort_desc = True en el primer clic.
    assert app.db_table.item(0, 2).text() == "Z"
    
    # 3. Segundo clic -> Invertir
    app.sort_database(2)
    assert app.db_table.item(0, 2).text() == "A"

def test_sort_by_elo(app, qtbot):
    """Verifica la ordenación numérica por ELO"""
    # 1. Clic en Elo B (columna 3)
    app.sort_database(3) # Elo Blanco
    
    # Debería ser el más alto primero (Descendente)
    assert app.db_table.item(0, 3).text() == "2800"
    
    # 2. Invertir -> El más bajo primero
    app.sort_database(3)
    assert app.db_table.item(0, 3).text() == "2000"

def test_sorting_with_filter(app, qtbot):
    """Verifica que la ordenación respeta el filtro activo"""
    # 1. Filtrar para que solo queden 'A' y 'M' (quitamos 'Z' que tiene ELO 2000)
    app.db.filter_db({"min_elo": "2300"})
    app.refresh_db_list()
    assert app.db_table.rowCount() == 2
    
    # 2. Ordenar por blancas
    app.sort_database(2)
    # Entre 'A' y 'M', el primero en descendente es 'M'
    assert app.db_table.item(0, 2).text() == "M"
