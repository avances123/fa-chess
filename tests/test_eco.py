import pytest
import os
from core.eco import ECOManager

@pytest.fixture
def eco_manager(tmp_path):
    eco_file = tmp_path / "test.eco"
    # El ECOManager espera formato Scid: Cód "Nombre" 1. e4 e5 ...
    content = 'B00 "King\'s Pawn Game" 1. e4\nC20 "Open Game" 1. e4 e5\nB20 "Sicilian Defense" 1. e4 c5\n'
    eco_file.write_text(content)
    return ECOManager(str(eco_file))

def test_eco_get_opening_name(eco_manager):
    assert eco_manager.get_opening_name("e2e4") == "King's Pawn Game"
    assert eco_manager.get_opening_name("e2e4 c7c5") == "Sicilian Defense"
    assert eco_manager.get_opening_name("") == "Posición Inicial"

def test_eco_exact_match(eco_manager):
    # Probar que devuelve la posición más profunda encontrada (Open Game en vez de King's Pawn)
    assert eco_manager.get_opening_name("e2e4 e7e5") == "Open Game"

def test_eco_unknown(eco_manager):
    assert eco_manager.get_opening_name("d2d4") == "Variante Desconocida"
