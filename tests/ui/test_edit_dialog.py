import pytest
from src.ui.edit_game_dialog import EditGameDialog

def test_edit_game_dialog(qapp):
    game_data = {
        "white": "Player1", "black": "Player2", "w_elo": 2000, "b_elo": 2000,
        "result": "1-0", "date": "2024.01.01", "event": "Event", "site": "Site"
    }
    dialog = EditGameDialog(game_data)
    # Corregido: usa self.inputs
    assert dialog.inputs["white"].text() == "Player1"
    
    dialog.inputs["white"].setText("NewName")
    dialog.accept()
    
    new_data = dialog.get_data()
    assert new_data["white"] == "NewName"
