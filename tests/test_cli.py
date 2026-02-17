import pytest
from unittest.mock import patch, MagicMock
from src.cli import main
import os

def test_cli_import(tmp_path):
    # Crear un archivo de entrada falso
    input_file = tmp_path / "test.pgn"
    input_file.touch()
    output_file = tmp_path / "test.parquet"
    
    with patch('src.cli.convert_pgn_to_parquet') as mock_conv:
        with patch('sys.argv', ['fa-chess-cli', str(input_file), str(output_file)]):
            main()
            mock_conv.assert_called_once()

def test_cli_puzzles(tmp_path):
    input_file = tmp_path / "test.csv"
    input_file.touch()
    output_file = tmp_path / "test.parquet"
    
    with patch('src.cli.convert_lichess_puzzles') as mock_conv:
        with patch('sys.argv', ['fa-chess-cli', str(input_file), str(output_file), '--puzzles']):
            main()
            mock_conv.assert_called_once()

def test_cli_not_found():
    with patch('sys.argv', ['fa-chess-cli', 'missing.pgn', 'out.p']):
        with pytest.raises(SystemExit) as e:
            main()
        assert e.value.code == 1
