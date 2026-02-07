import pytest
import chess
from ui.main_window import MainWindow
from tests.utils.mock_engine import mock_popen_uci
import chess.engine
import ui.main_window

@pytest.fixture
def app(qtbot, tmp_path, monkeypatch):
    test_config = tmp_path / "test_config.json"
    test_clipbase = tmp_path / "test_clipbase.parquet"
    monkeypatch.setattr(ui.main_window, "CONFIG_FILE", str(test_config))
    # Mockear el motor UCI globalmente
    monkeypatch.setattr(chess.engine.SimpleEngine, "popen_uci", mock_popen_uci)
    
    window = MainWindow()
    qtbot.addWidget(window)
    return window

def test_full_analysis_integration(app, qtbot):
    # 1. Preparar una partida pequeña
    app.game.load_uci_line("e2e4 e7e5 g1f3")
    assert len(app.game.full_mainline) == 3
    
    # 2. Iniciar Análisis Completo
    with qtbot.waitSignal(app.analysis_worker.finished if hasattr(app, 'analysis_worker') else app.start_full_analysis() or app.analysis_worker.finished, timeout=2000):
        pass
        
    # 3. Verificar que se han generado evaluaciones
    # El worker debería haber rellenado game_evals
    # +1 por la posición inicial -> total 4 evaluaciones
    assert len(app.game_evals) == 4
    # El mock devuelve 50 o -50, así que no deben ser 0 todos
    assert any(e != 0 for e in app.game_evals)
    
    # 4. Verificar que la UI se desbloqueó
    assert app.board_ana.isEnabled()
    assert app.tree_ana.isEnabled()

def test_engine_toggle_mocked(app, qtbot):
    # Probar el botón de motor normal con el mock
    app.toggle_engine(True)
    
    # Esperar a que el worker emita info (simulada)
    def check_eval():
        return app.label_eval.text() != ""
        
    qtbot.waitUntil(check_eval, timeout=5000)
    assert "+" in app.label_eval.text() or "-" in app.label_eval.text() or "0.00" in app.label_eval.text()
    
    app.toggle_engine(False)
