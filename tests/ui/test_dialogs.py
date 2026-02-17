import pytest
from PySide6.QtCore import Qt
from src.ui.settings_dialog import SettingsDialog
from src.ui.search_dialog import SearchDialog

def test_settings_dialog(qapp):
    initial_cfg = {
        "color_light": "#eeeed2",
        "color_dark": "#8ca2ad",
        "engine_path": "/usr/bin/sf", 
        "engine_threads": 2
    }
    dialog = SettingsDialog(initial_cfg)
    
    # Verificar que los campos se cargaron
    assert dialog.edit_engine_path.text() == "/usr/bin/sf"
    
    # Cambiar un valor
    dialog.edit_engine_path.setText("/usr/bin/stockfish")
    dialog.accept()
    
    new_cfg = dialog.get_config()
    assert new_cfg["engine_path"] == "/usr/bin/stockfish"

def test_search_dialog(qapp):
    dialog = SearchDialog()
    dialog.white_input.setText("Kasparov")
    dialog.accept()
    
    criteria = dialog.get_criteria()
    assert criteria["white"] == "Kasparov"
