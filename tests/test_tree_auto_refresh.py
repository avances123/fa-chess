import pytest
import polars as pl
from PySide6.QtCore import Qt
from ui.main_window import MainWindow
import ui.main_window

@pytest.fixture
def app(qtbot, tmp_path, monkeypatch):
    # Mock de la ruta de configuración
    test_config = tmp_path / "test_config.json"
    monkeypatch.setattr(ui.main_window, "CONFIG_FILE", str(test_config))
    
    window = MainWindow()
    qtbot.addWidget(window)
    return window

def test_tree_loads_on_startup(app, qtbot):
    # 1. Preparar una base de datos con algunas partidas
    df = pl.DataFrame({
        "id": [1], "white": ["A"], "black": ["B"],
        "w_elo": [2000], "b_elo": [2000], "result": ["1-0"],
        "date": ["2023"], "event": ["?"],
        "line": ["e2e4"], "full_line": ["e2e4"], "fens": [[0, 1]]
    })
    
    # Inyectamos los datos y forzamos el cambio de base
    app.db.dbs["Clipbase"] = df
    app.db.active_db_changed.emit("Clipbase")
    
    # 2. Esperar a que el worker de estadísticas termine (asíncrono)
    # El worker se dispara tras 150ms (debounce)
    qtbot.wait(300) 
    
    # 3. Verificar que la tabla no está vacía al arrancar/cambiar base
    # La primera jugada (e2e4) debería aparecer en la primera fila
    assert app.tree_ana.rowCount() > 0
    assert app.tree_ana.item(0, 0).text() == "e4"

def test_tree_refreshes_on_db_switch(app, qtbot):
    # 1. Crear dos bases con diferentes jugadas iniciales
    df1 = pl.DataFrame({"id":[1], "white":["A"], "black":["B"], "w_elo":[2000], "b_elo":[2000], "result":["1-0"], "date":["2023"], "event":["?"], "line":["e2e4"], "full_line":["e2e4"], "fens":[[0,1]]})
    df2 = pl.DataFrame({"id":[2], "white":["C"], "black":["D"], "w_elo":[2000], "b_elo":[2000], "result":["0-1"], "date":["2023"], "event":["?"], "line":["d2d4"], "full_line":["d2d4"], "fens":[[0,2]]})
    
    app.db.dbs["Base1"] = df1
    app.db.dbs["Base2"] = df2
    
    # 2. Cambiar a Base1 y verificar
    app.db.set_active_db("Base1")
    qtbot.wait(300)
    assert app.tree_ana.item(0, 0).text() == "e4"
    
    # 3. Cambiar a Base2 y verificar que el árbol CAMBIA
    app.db.set_active_db("Base2")
    qtbot.wait(300)
    assert app.tree_ana.item(0, 0).text() == "d4"
