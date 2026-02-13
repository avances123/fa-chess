import pytest
from unittest.mock import MagicMock
from PySide6.QtCore import Qt
from PySide6.QtGui import QAction

def test_navigation_connections(main_window):
    """Verifica que las funciones de navegación están conectadas."""
    main_window.game.step_forward = MagicMock()
    # Ejecutamos el trigger de las acciones de la toolbar
    for action in main_window.toolbar_ana.actions():
        if action.statusTip() == "Siguiente":
            action.trigger()
            break
    assert main_window.game.step_forward.called

def test_menu_structure(main_window):
    """Verifica que los menús principales existen."""
    menubar = main_window.menuBar()
    titles = [a.text().replace("&", "") for a in menubar.actions()]
    assert any("Archivo" in t for t in titles)
    assert any("Editar" in t for t in titles)
    assert any("Jugador" in t for t in titles)

def test_action_triggering(main_window):
    """Verifica que disparar las acciones del menú llama a los métodos correctos."""
    main_window.open_search = MagicMock()
    main_window.prompt_player_report = MagicMock()
    main_window.save_to_active_db = MagicMock()

    all_actions = main_window.findChildren(QAction)
    
    # Mapeo de texto a mock
    tests = [
        ("Filtrar Partidas", main_window.open_search),
        ("Dossier de Inteligencia", main_window.prompt_player_report),
        ("Guardar Base", main_window.save_to_active_db)
    ]

    for text, mock in tests:
        found = False
        for act in all_actions:
            if text in act.text().replace("&", ""):
                act.trigger()
                found = True
                break
        assert found, f"No se encontró la acción con texto: {text}"
        assert mock.called, f"La acción {text} no disparó su método."

def test_ui_elements_exist(main_window):
    """Verifica que los elementos clave de la UI han sido instanciados."""
    assert main_window.btn_save is not None
    assert main_window.btn_clip is not None
    assert main_window.btn_new is not None
    assert main_window.opening_tree is not None
    assert main_window.db_table is not None
