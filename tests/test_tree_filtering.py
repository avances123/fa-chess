import pytest
import chess
import polars as pl
from ui.main_window import MainWindow
import ui.main_window

@pytest.fixture
def app(qtbot, tmp_path, monkeypatch):
    test_config = tmp_path / "test_config.json"
    monkeypatch.setattr(ui.main_window, "CONFIG_FILE", str(test_config))
    window = MainWindow()
    qtbot.addWidget(window)
    return window

def test_tree_respects_filter(app):
    # 1. Crear datos de prueba: 2 partidas, una con e4 y otra con d4
    df = pl.DataFrame({
        "id": [1, 2],
        "white": ["Jugador A", "Jugador B"],
        "black": ["X", "Y"],
        "w_elo": [2000, 2000], "b_elo": [2000, 2000],
        "result": ["1-0", "0-1"],
        "date": ["2023", "2023"], "event": ["?", "?"],
        "line": ["e2e4", "d2d4"],
        "full_line": ["e2e4", "d2d4"],
        "fens": [[0], [0]] # Hashes simplificados
    })
    
    # Inyectar datos en la base activa (Clipbase)
    app.db.dbs["Clipbase"] = df
    
    # 2. Sin filtro: el árbol debería ver 2 movimientos posibles (e4 y d4)
    # Forzamos ejecución síncrona del worker para el test
    res_all = app.db.get_stats_for_position("", is_white=True)
    assert res_all.height == 2
    
    # 3. Aplicar filtro: solo partidas de "Jugador A"
    app.db.filter_db({"white": "Jugador A", "black": "", "min_elo": "", "result": "Cualquiera"})
    
    # 4. Con filtro: el árbol debería ver solo 1 movimiento (e4)
    res_filtered = app.db.get_stats_for_position("", is_white=True)
    assert res_filtered.height == 1
    assert res_filtered.row(0, named=True)["uci"] == "e2e4"
    
    # 5. Quitar filtro: volver a ver todo
    app.db.set_active_db("Clipbase") # Esto resetea current_filter_df en nuestro DBManager
    res_reset = app.db.get_stats_for_position("", is_white=True)
    assert res_reset.height == 2
