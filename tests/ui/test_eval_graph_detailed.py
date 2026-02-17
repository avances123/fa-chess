import pytest
from PySide6.QtWidgets import QApplication
from PySide6.QtCore import Qt, QPoint, QEvent, QPointF
from PySide6.QtGui import QMouseEvent, QWheelEvent
from src.ui.widgets.eval_graph import EvaluationGraph

def test_eval_graph_full_interaction(qtbot):
    widget = EvaluationGraph()
    qtbot.addWidget(widget)
    widget.show()
    widget.resize(400, 200)

    # Set evaluations including mate scores (if handled)
    evals = [0, 50, -50, 1000, -1000, 200, -200]
    widget.set_evaluations(evals)
    
    # Trigger paint event
    widget.repaint()
    QApplication.processEvents()
    
    # Mouse press to select move
    # With 7 evals and 400 width, each bar is ~57px. 
    # Clicking at x=100 should hit the 2nd bar (idx 1)
    with qtbot.waitSignal(widget.move_selected):
        qtbot.mouseClick(widget, Qt.LeftButton, pos=QPoint(100, 100))
        
    # Test Wheel Event (zoom/scroll simulation if any)
    from PySide6.QtGui import QWheelEvent
    wheel = QWheelEvent(QPointF(100, 100), QPointF(100, 100), QPoint(0, 0), QPoint(0, 120), Qt.NoButton, Qt.NoModifier, Qt.NoScrollPhase, False)
    QApplication.sendEvent(widget, wheel)

    # Resize event
    widget.resize(500, 300)
    
    # Empty evals
    widget.set_evaluations([])
    widget.update()

def test_eval_graph_mouse_move(qtbot):
    widget = EvaluationGraph()
    qtbot.addWidget(widget)
    widget.set_evaluations([0, 10, 20, 30, 40, 50])
    widget.resize(600, 200)
    
    # Mouse move to trigger hover effects if any
    qtbot.mouseMove(widget, QPoint(100, 100))
    qtbot.mouseMove(widget, QPoint(300, 100))
