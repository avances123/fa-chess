import pytest
import polars as pl
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

def test_database_sorting_logic(app, qtbot):
    # 1. Crear una base con datos variados para ordenar
    df = pl.DataFrame({
        "id": [1, 2, 3],
        "white": ["A", "B", "C"],
        "black": ["A", "B", "C"],
        "w_elo": [2500, 2800, 2300], # Desordenados
        "b_elo": [2500, 2800, 2300],
        "result": ["*", "*", "*"],
        "date": ["2023", "2023", "2023"],
        "event": ["Test", "Test", "Test"],
        "line": ["", "", ""], 
        "full_line": ["", "", ""], 
        "fens": [[], [], []]
    }, schema_overrides={"id": pl.Int64, "fens": pl.List(pl.UInt64)})
    
    app.db.dbs["Clipbase"] = df
    app.db.set_active_db("Clipbase")
    app.refresh_db_list()
    
    # Estado inicial: Orden de inserción (2500, 2800, 2300)
    assert app.db_table.item(0, 2).text() == "2500"
    
    # 2. Ordenar por Elo Blanco (Columna 2) -> Debería ser Descendente por defecto (definido en sort_database)
    # Simulamos clic en cabecera
    app.sort_database(2)
    
    # Verificar orden Descendente (2800 primero)
    assert app.db_table.item(0, 2).text() == "2800"
    assert app.db_table.item(1, 2).text() == "2500"
    assert app.db_table.item(2, 2).text() == "2300"
    
    # 3. Invertir Orden -> Ascendente (2300 primero)
    app.sort_database(2)
    assert app.db_table.item(0, 2).text() == "2300"
    assert app.db_table.item(2, 2).text() == "2800"

def test_sorting_with_filter(app, qtbot):
    # 1. Base con datos
    df = pl.DataFrame({
        "id": [1, 2, 3, 4],
        "white": ["Carlsen", "Carlsen", "Nepo", "Caruana"],
        "w_elo": [2800, 2850, 2750, 2780],
        "black": ["A", "B", "C", "D"], "b_elo": [0]*4,
        "result": ["*"]*4, "date": [""]*4, "event": [""]*4,
        "line": [""]*4, "full_line": [""]*4, "fens": [[]]*4
    }, schema_overrides={"id": pl.Int64, "fens": pl.List(pl.UInt64)})
    
    app.db.dbs["Clipbase"] = df
    app.db.set_active_db("Clipbase")
    
    # 2. Filtrar por "Carlsen"
    app.search_criteria = {"white": "Carlsen", "black": "", "min_elo": "", "result": "Cualquiera"}
    filtered = app.db.filter_db(app.search_criteria)
    app.refresh_db_list(filtered)
    
    assert app.db_table.rowCount() == 2
    # Orden original del filtro (2800, 2850)
    
    # 3. Ordenar el FILTRO por Elo -> Descendente (2850 primero)
    app.sort_database(2)
    
    assert app.db_table.rowCount() == 2 # Siguen siendo 2
    assert app.db_table.item(0, 2).text() == "2850"
    assert app.db_table.item(1, 2).text() == "2800"
    
    # Verificar que NO ha traído a Nepo o Caruana
    items = [app.db_table.item(i, 1).text() for i in range(2)]
    assert "Nepo" not in items
    assert all("Carlsen" in x for x in items)
