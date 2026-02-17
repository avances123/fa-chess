import pytest
from src.ui.widgets.results_bar import ResultsWidget
from src.ui.widgets.game_info_header import GameInfoHeader
from PySide6.QtWidgets import QApplication
import chess

@pytest.fixture(scope="session")
def qapp():
    app = QApplication.instance()
    if app is None: app = QApplication([])
    yield app

def test_results_widget(qapp):
    # W: 10, D: 5, B: 5, Total: 20
    # Score = (10 + 0.5 * 5) / 20 = 12.5 / 20 = 62.5%
    rw = ResultsWidget(10, 5, 5, 20, True)
    assert "62.5%" in rw.toolTip()

def test_game_info_header(qapp):
    gh = GameInfoHeader()
    data = {"white": "P1", "black": "P2", "w_elo": 2000, "b_elo": 1900, "result": "1-0", "event": "E", "date": "D", "site": "S"}
    gh.update_info(data)
    assert "P1" in gh.label.text()
