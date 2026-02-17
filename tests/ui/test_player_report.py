import pytest
from src.ui.player_report_widget import PlayerReportWidget

def test_player_report_widget(qapp):
    stats = {
        "name": "Carlsen",
        "as_white": {"w": 10, "d": 5, "b": 2, "total": 17, "avg_opp_elo": 2800, "perf": 2850},
        "as_black": {"w": 8, "d": 6, "b": 3, "total": 17, "avg_opp_elo": 2810, "perf": 2840},
        "top_white": [], "top_black": [], "elo_history": [], "best_wins": [], "worst_losses": [], "max_elo": 2882
    }
    widget = PlayerReportWidget(stats)
    # No verificamos windowTitle ya que no lo setea en el init
    assert widget.stats["name"] == "Carlsen"
    widget.show()
