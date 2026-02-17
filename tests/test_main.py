import pytest
from unittest.mock import patch, MagicMock
import sys

def test_main_entry_point():
    # Mockear QApplication y MainWindow para no lanzar la UI real
    with patch('PySide6.QtWidgets.QApplication'), patch('src.ui.main_window.MainWindow'), patch('sys.exit'):
        from src.main import main
        main()
