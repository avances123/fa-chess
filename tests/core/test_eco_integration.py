import pytest
import os
from src.core.eco import ECOManager

import chess

def test_eco_detection_logic(tmp_path):
    # Crear un archivo .eco de prueba con formato SCID
    eco_content = """
A00 "Apertura Polaca" 1. b4
B20 "Defensa Siciliana" 1. e4 c5
B21 "Siciliana: Gran Premio" 1. e4 c5 2. f4
"""
    eco_file = tmp_path / "test.eco"
    eco_file.write_text(eco_content)
    
    manager = ECOManager(str(eco_file))
    board = chess.Board()
    
    # 1. Test posición inicial
    name, depth = manager.get_opening_name(board)
    assert "Posición Inicial" in name
    
    # 2. Test detección exacta
    board.push_san("e4")
    board.push_san("c5")
    name, depth = manager.get_opening_name(board)
    assert "Defensa Siciliana" in name
    assert depth == 2
    
    # 3. Test detección incremental (más específica primero)
    board.push_san("f4")
    name, depth = manager.get_opening_name(board)
    assert "Gran Premio" in name
    assert depth == 3
    
    # 4. Test fallback a la anterior
    board.pop() # Quitar f4
    board.push_san("Nf3") # e4 c5 Nf3 (no está en ECO de prueba)
    name, depth = manager.get_opening_name(board)
    assert "Defensa Siciliana" in name
    assert depth == 2
