import pytest
import chess
from src.ui.widgets.eval_graph import EvaluationGraph
from src.ui.widgets.analysis_report import AnalysisReport
from src.ui.widgets.results_bar import ResultsWidget

def test_eval_graph(qapp):
    widget = EvaluationGraph()
    widget.set_evaluations([0, 50, -100, 200])
    assert widget.evals == [0, 50, -100, 200]
    
    widget.set_current_move(2)
    assert widget.current_idx == 2

def test_analysis_report(qapp):
    widget = AnalysisReport()
    # Para 4 movimientos:
    # 0. Inicial: 0
    # 1. Blancas: +50 (OK)
    # 2. Negras: +60 (OK)
    # 3. Blancas: +40 (Inaccuracy blancas)
    # 4. Negras: +500 (Blunder negras: la ventaja blanca sube mucho)
    evals = [0, 50, 60, 40, 500] 
    moves = ["e2e4", "e7e5", "g1f3", "f7f6"]
    widget.update_stats(evals, moves, "White", "Black")
    
    # Blunder de negras (f7f6): 500 - 40 = 460 de pérdida
    assert widget.labels["black_blunders"].text() == "1"

def test_results_widget(qapp):
    widget = ResultsWidget(10, 5, 5, 20, is_white=True)
    assert widget.total == 20
