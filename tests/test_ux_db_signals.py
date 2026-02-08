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
    """Verifica que el flujo de señales actualiza la UI y los colores de los Badges"""
    from ui.styles import STYLE_BADGE_SUCCESS, STYLE_BADGE_NORMAL
    
    # 1. Estado inicial: Debería estar en gris (Normal)
    assert main_window.label_eco.styleSheet() == STYLE_BADGE_NORMAL
    
    # 2. Mockeamos refresh_db_list
    main_window.refresh_db_list = MagicMock()
    
    # 3. Simulamos la aplicación de un filtro (emitiendo la señal que emitiría el DBManager)
    # Creamos un mock de DataFrame para que refresh_db_list no falle
    mock_df = MagicMock()
    main_window.db.current_filter_df = mock_df
    main_window.db.filter_updated.emit(mock_df)
    
    # 4. Verificamos que MainWindow reaccionó
    assert main_window.refresh_db_list.called
    assert main_window.run_stats_worker.called
    
    # 5. Verificamos que el color cambió a verde (Success)
    assert main_window.label_eco.styleSheet() == STYLE_BADGE_SUCCESS
    assert main_window.label_db_stats.styleSheet() == STYLE_BADGE_SUCCESS

def test_reset_filters_restores_colors(main_window, qtbot):
    """Verifica que al resetear los filtros los colores vuelven a gris"""
    from ui.styles import STYLE_BADGE_SUCCESS, STYLE_BADGE_NORMAL
    
    # 1. Forzamos un estado filtrado (verde)
    main_window.db.current_filter_df = MagicMock()
    main_window.update_stats()
    assert main_window.label_eco.styleSheet() == STYLE_BADGE_SUCCESS
    
    # 2. Ejecutamos el reset
    main_window.reset_filters()
    
    # 3. Verificamos que vuelve a gris
    assert main_window.db.current_filter_df is None
    assert main_window.label_eco.styleSheet() == STYLE_BADGE_NORMAL
    assert main_window.label_db_stats.styleSheet() == STYLE_BADGE_NORMAL

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