import pytest
import polars as pl
from src.core.db_manager import DBManager
from src.core.eco import ECOManager

def test_player_report_accuracy(temp_env, sample_parquet):
    """Verifica que las estadísticas del dossier de jugador sean matemáticamente correctas."""
    manager = DBManager()
    manager.load_parquet(sample_parquet)
    
    # El sample_parquet tiene 1 partida de Carlsen ganando (1-0)
    stats = manager.get_player_report("Carlsen")
    
    assert stats["name"] == "Carlsen"
    assert stats["as_white"]["total"] == 1
    assert stats["as_white"]["w"] == 1
    assert stats["max_elo"] == 2850
    
    # Verificar cálculo de Performance (Elo Opp 2780 + Score 1.0 -> 2780 + 400 = 3180)
    assert stats["as_white"]["perf"] == 3180

def test_theoretical_depth_detection(temp_env, sample_parquet):
    """Verifica que el sistema detecta correctamente la profundidad teórica."""
    # Necesitamos un ECOManager real o mockeado para este test
    from src.config import ECO_FILE
    eco = ECOManager(ECO_FILE)
    
    manager = DBManager()
    manager.load_parquet(sample_parquet)
    
    # Carlsen jugó e4 e5 Nf3 Nc6 Bb5
    stats = manager.get_player_report("Carlsen", eco_manager=eco)
    
    top_opening = stats["top_white"][0]
    # El nombre puede variar según el archivo ECO (Open Game, Ruy Lopez, etc)
    # Lo importante es que haya detectado una apertura y su profundidad
    assert top_opening["opening_name"] != "Variante Desconocida"
    assert top_opening["avg_depth"] > 0
