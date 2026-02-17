import pytest
import os
from src.core.eco import ECOManager

@pytest.fixture
def eco_manager():
    eco_content = 'A00 "Anderssen\'s Opening" 1. a3\nA01 "Nimzo-Larsen Attack" 1. b3\n'
    eco_path = "tests/data/test.eco"
    with open(eco_path, "w") as f:
        f.write(eco_content)
    
    manager = ECOManager(eco_path)
    yield manager
    if os.path.exists(eco_path): os.remove(eco_path)

def test_eco_load_and_recognition(eco_manager):
    name, depth = eco_manager.get_opening_name("a2a3")
    assert "Anderssen's Opening" in name
    assert depth == 1

def test_eco_unknown_opening(eco_manager):
    # Probar posición desconocida (el código actual devuelve 'Variante Desconocida')
    name, depth = eco_manager.get_opening_name("e2e4 e7e5")
    assert name == "Variante Desconocida"
    assert depth == 0

def test_eco_partial_match(eco_manager):
    name, depth = eco_manager.get_opening_name("a2a3 e7e5")
    assert "Anderssen's Opening" in name
    assert depth == 1
