import pytest
from src.main import main
import sys
from PySide6.QtWidgets import QApplication

def test_main_app_start(monkeypatch):

    # Mockear MainWindow

    class MockWin:

        def __init__(self): pass

        def show(self): pass



    monkeypatch.setattr("src.main.MainWindow", MockWin)

    

    # Mockear QApplication para que no intente crear una real

    class MockApp:

        def __init__(self, argv): pass

        def exec(self): return 0

    

    monkeypatch.setattr("src.main.QApplication", MockApp)



    from src.main import main

    main()


