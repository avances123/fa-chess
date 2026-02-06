import pytest
from PySide6.QtCore import Qt
from ui.search_dialog import SearchDialog

def test_search_dialog_clear(qtbot):
    # Crear una instancia del diálogo
    dialog = SearchDialog()
    qtbot.addWidget(dialog)

    # Rellenar campos
    dialog.white_input.setText("Carlsen")
    dialog.black_input.setText("Caruana")
    dialog.min_elo_input.setText("2800")
    dialog.result_combo.setCurrentText("1-0")

    # Verificar que no están vacíos
    assert dialog.white_input.text() == "Carlsen"
    assert dialog.black_input.text() == "Caruana"

    # Simular clic en el botón Limpiar
    # Buscamos el botón por su texto
    from PySide6.QtWidgets import QPushButton
    btn_clear = None
    for btn in dialog.findChildren(QPushButton):
        if btn.text() == "Limpiar":
            btn_clear = btn
            break
    
    assert btn_clear is not None
    qtbot.mouseClick(btn_clear, Qt.LeftButton)

    # Verificar que se han limpiado
    assert dialog.white_input.text() == ""
    assert dialog.black_input.text() == ""
    assert dialog.min_elo_input.text() == ""
    assert dialog.result_combo.currentText() == "Cualquiera"

def test_search_dialog_get_criteria(qtbot):
    dialog = SearchDialog()
    qtbot.addWidget(dialog)

    dialog.white_input.setText("Kasparov")
    dialog.result_combo.setCurrentText("0-1")

    criteria = dialog.get_criteria()
    assert criteria["white"] == "Kasparov"
    assert criteria["result"] == "0-1"
    assert criteria["black"] == ""
