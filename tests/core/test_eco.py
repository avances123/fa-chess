import pytest
import os
import chess
from src.core.eco import ECOManager

@pytest.fixture
def eco_file(tmp_path):
    p = tmp_path / "test.eco"
    p.write_text('A00 "Apertura Anderssen" 1. a3 e5\nB00 "Defensa Nimzowitsch" 1. e4 Nc6\n', encoding="utf-8")
    return str(p)

def test_eco_load_and_lookup(eco_file):
    manager = ECOManager(eco_file)
    
    # 1. e4 Nc6 -> e2e4 b8c6
    name, depth = manager.get_opening_name("e2e4 b8c6")
    assert "Defensa Nimzowitsch" in name
    assert depth == 2

    # Test prefijo (más profundo)
    name, depth = manager.get_opening_name("e2e4 b8c6 d2d4")
    assert "Defensa Nimzowitsch" in name
    assert depth == 2

    # Test posición inicial
    name, depth = manager.get_opening_name("")
    assert name == "Posición Inicial"

    # Test desconocida
    name, depth = manager.get_opening_name("h2h4")
    assert name == "Variante Desconocida"

def test_eco_invalid_file():
    manager = ECOManager("non_existent.eco")
    assert manager.openings == []
    name, depth = manager.get_opening_name("e2e4")
    assert name == "Variante Desconocida"
