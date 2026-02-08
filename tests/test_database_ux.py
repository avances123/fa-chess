import pytest
import polars as pl
from src.ui.main_window import MainWindow
from PySide6.QtCore import Qt
from PySide6.QtTest import QTest

@pytest.fixture
def app(qtbot):
    """Fixture que inicializa la ventana principal para tests de integración"""
    window = MainWindow()
    qtbot.addWidget(window)
    
    # Inyectar datos mock en Clipbase
    df = pl.DataFrame({
        "id": [1, 2],
        "white": ["Magnus", "Fabiano"],
        "black": ["Nepo", "Ding"],
        "w_elo": [2850, 2820],
        "b_elo": [2800, 2810],
        "result": ["1-0", "1/2-1/2"],
        "date": ["2024.01.01", "2024.01.02"],
        "event": ["Test Event", "Test Event"],
        "site": ["", ""],
        "line": ["e2e4", "d2d4"],
        "full_line": ["e2e4 e7e5", "d2d4 d5"],
        "fens": [[1, 2], [3, 4]]
    })
    window.db.dbs["Clipbase"] = df.lazy()
    window.db.set_active_db("Clipbase")
    return window

def test_filter_integration(app, qtbot):
    """Verifica que aplicar un filtro actualiza la tabla y los badges"""
    # 1. Simular búsqueda de "Magnus"
    app.search_criteria = {"white": "Magnus", "black": "", "min_elo": "", "result": "Cualquiera"}
    app.db.filter_db(app.search_criteria)
    
    # 2. Verificar que la tabla solo tiene 1 fila
    assert app.db_table.rowCount() == 1
    assert app.db_table.item(0, 2).text() == "Magnus"
    
    # 3. Verificar que el badge de DBSidebar marca [1/2] y color verde (success)
    stats_text = app.db_sidebar.label_stats.text()
    assert "1/2" in stats_text
    # El estilo de Polars Lazy aplica STYLE_BADGE_SUCCESS para filtros
    assert "color: #2e7d32" in app.db_sidebar.label_stats.styleSheet().lower()

def test_reset_filter_integration(app, qtbot):
    """Verifica que el botón de limpiar filtros en el sidebar funciona"""
    # 1. Aplicar filtro
    app.db.filter_db({"white": "Magnus"})
    assert app.db_table.rowCount() == 1
    
    # 2. Simular clic en el botón de borrar filtros del sidebar
    # Buscamos la acción de 'eraser' en el toolbar del sidebar
    clear_action = None
    for action in app.db_sidebar.toolbar.actions():
        if "Quitar Filtros" in action.statusTip():
            clear_action = action
            break
    
    assert clear_action is not None
    clear_action.trigger()
    
    # 3. Verificar que volvemos a ver las 2 partidas
    assert app.db_table.rowCount() == 2
    assert "2/2" in app.db_sidebar.label_stats.text()

def test_load_game_from_table(app, qtbot):
    """Verifica que doble clic en la tabla carga la partida y actualiza la cabecera"""
    # 1. Forzar carga de la primera partida (Magnus)
    item = app.db_table.item(0, 0)
    app.load_game_from_list(item)
    
    # 2. Verificar cabecera
    header_text = app.game_header.label.text()
    assert "Magnus" in header_text
    assert "Nepo" in header_text
    
    # 3. Verificar que el controlador de juego está en la posición inicial (sin jugadas en el stack todavía)
    assert app.game.current_line_uci == ""
    assert len(app.game.board.move_stack) == 0
