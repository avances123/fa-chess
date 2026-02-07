import pytest
import chess
from PySide6.QtCore import Qt
from ui.main_window import MainWindow
from unittest.mock import MagicMock, patch

@pytest.fixture
def main_window(qtbot, monkeypatch):
    # 1. Evitar carga de archivos reales
    monkeypatch.setattr("os.path.exists", lambda path: False)
    
    # 2. Parchear run_stats_worker a nivel de CLASE antes de instanciar
    with patch.object(MainWindow, 'run_stats_worker', return_value=None):
        window = MainWindow()
        # Desactivar el timer para que no interfiera
        window.stats_timer.stop()
        
        qtbot.addWidget(window)
        return window

def test_signal_flow_filter_updates_ui(main_window, qtbot):
    """Verifica que el flujo de señales entre DBManager y MainWindow funciona"""
    
    # Mockeamos refresh_db_list para ver si la señal lo llama
    main_window.refresh_db_list = MagicMock()
    
    # Disparamos la señal de filtro desde el manager
    main_window.db.filter_updated.emit(None)
    
    # Verificamos que MainWindow reaccionó a la señal
    assert main_window.refresh_db_list.called
    assert main_window.run_stats_worker.called

def test_search_dialog_presets(main_window, qtbot):
    """Verifica que los nuevos botones de preset funcionan en el diálogo"""
    from ui.search_dialog import SearchDialog
    
    dialog = SearchDialog(main_window)
    qtbot.addWidget(dialog)
    
    # Simular clic en el preset de Élite
    btn_elite = None
    from PySide6.QtWidgets import QPushButton
    for btn in dialog.findChildren(QPushButton):
        if "Élite" in btn.text():
            btn_elite = btn
            break
    
    assert btn_elite is not None
    qtbot.mouseClick(btn_elite, Qt.LeftButton)
    
    # Verificar que el ELO se ha autorellenado
    assert dialog.min_elo_input.text() == "2700"

def test_game_toolbar_presence(main_window):
    """Verifica que la nueva barra de herramientas de partida existe"""
    from PySide6.QtWidgets import QToolBar
    
    found_save = False
    for toolbar in main_window.findChildren(QToolBar):
        for action in toolbar.actions():
            if "Guardar" in action.text():
                found_save = True
                break
    
    assert found_save is True