import pytest
import os
import polars as pl
from src.core.db_manager import GAME_SCHEMA

@pytest.fixture
def temp_env(tmp_path, monkeypatch):
    config_dir = tmp_path / ".config"
    config_dir.mkdir()
    config_file = config_dir / "fa-chess-test.json"
    clipbase_file = config_dir / "test-clipbase.parquet"
    
    monkeypatch.setattr("src.config.CONFIG_FILE", str(config_file))
    monkeypatch.setattr("src.core.db_manager.CLIPBASE_FILE", str(clipbase_file))
    
    return {
        "config": str(config_file),
        "clipbase": str(clipbase_file),
        "tmp_dir": str(tmp_path)
    }

@pytest.fixture
def main_window(qtbot, temp_env):
    from src.ui.main_window import MainWindow
    window = MainWindow()
    qtbot.addWidget(window)
    return window
