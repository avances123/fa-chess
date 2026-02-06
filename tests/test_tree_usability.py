import pytest
import chess
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

def test_tree_selection_draws_arrow(app, qtbot):
    # 1. Simular que hay datos en el árbol (mockeamos la tabla)
    # Columna 0 es el movimiento, usamos setData para el UCI
    from PySide6.QtWidgets import QTableWidgetItem
    it = QTableWidgetItem("e4")
    it.setData(Qt.UserRole, "e2e4")
    app.tree_ana.setRowCount(1)
    app.tree_ana.setItem(0, 0, it)
    
    # 2. Simular un clic en la fila
    app.on_tree_cell_click(app.tree_ana.item(0, 0))
    
    # 3. Verificar que se ha activado la flecha de hover (azul)
    assert app.board_ana.hover_move == "e2e4"

def test_move_clears_selection_and_arrow(app, qtbot):
    # 1. Establecer una flecha y selección previa
    app.board_ana.hover_move = "e2e4"
    app.tree_ana.setRowCount(1)
    app.tree_ana.setItem(0, 0, pytest.importorskip("PySide6.QtWidgets").QTableWidgetItem("e4"))
    app.tree_ana.selectRow(0)
    
    # 2. Realizar un movimiento
    app.make_move(chess.Move.from_uci("g1f3"))
    
    # 3. Verificar que la flecha y la selección han desaparecido
    assert app.board_ana.hover_move is None
    assert len(app.tree_ana.selectedItems()) == 0

def test_results_bar_tooltip(app):
    # Probar que el widget de resultados calcula bien el éxito para el bando que mueve
    # Blancas: 10, Tablas: 10, Negras: 0 (Total 20) -> Éxito blancas: (10 + 5)/20 = 75%
    res_w = app.ResultsWidget(w=10, d=10, b=0, total=20, is_white=True)
    assert "Éxito: 75.0%" in res_w.toolTip()
    
    # Mismo caso para negras -> Éxito negras: (0 + 5)/20 = 25%
    res_b = app.ResultsWidget(w=10, d=10, b=0, total=20, is_white=False)
    assert "Éxito: 25.0%" in res_b.toolTip()

def test_tree_double_click_makes_move(app):
    # 1. Preparar jugada en el árbol
    from PySide6.QtWidgets import QTableWidgetItem
    it = QTableWidgetItem("d4")
    it.setData(Qt.UserRole, "d2d4")
    app.tree_ana.setRowCount(1)
    app.tree_ana.setItem(0, 0, it)
    
    # 2. Simular doble clic
    app.on_tree_cell_double_click(app.tree_ana.item(0, 0))
    
    # 3. Verificar que la jugada se ha realizado en el tablero real
    assert len(app.board.move_stack) == 1
    assert app.board.move_stack[0].uci() == "d2d4"
