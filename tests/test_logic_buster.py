import pytest
import polars as pl
from src.core.db_manager import DBManager
from src.core.workers import StatsWorker, PGNWorker
from src.config import GAME_SCHEMA

def test_db_manager_exceptions(monkeypatch):
    db = DBManager()
    # Cubrir rama de error en sort
    monkeypatch.setattr(pl.LazyFrame, "sort", lambda *args, **kwargs: Exception("Fail"))
    db.dbs["test"] = pl.DataFrame(schema=GAME_SCHEMA).lazy()
    db.active_db_name = "test"
    db.sort_active_db("date", True) # No debería explotar

def test_stats_worker_logic_branches(qapp, tmp_path):
    # Probar StatsWorker con diferentes estados de caché
    db_path = tmp_path / "stats.parquet"
    pl.DataFrame(schema=GAME_SCHEMA).write_parquet(db_path)
    db = DBManager()
    db.load_parquet(str(db_path))
    
    worker = StatsWorker(db, "", True, current_hash=123)
    # Mockear el cache para forzar lectura
    db.stats_cache[(db.filter_id, 123)] = (pl.DataFrame({"uci": ["e4"], "c": [1]}), None)
    worker.run() # Debería usar el caché y terminar rápido

def test_pgn_worker_error(qtbot, tmp_path):
    # Archivo PGN corrupto o inexistente
    worker = PGNWorker("/non/existent.pgn")
    with qtbot.waitSignal(worker.finished):
        worker.start()
    # Debería haber emitido un error silencioso o vacío
