import pytest
from unittest.mock import MagicMock, patch
import chess
import polars as pl
import os
from src.core.workers import StatsWorker, RefutationWorker, PGNWorker, PGNAppendWorker, CachePopulatorWorker
from src.core.engine_worker import EngineWorker, TreeScannerWorker, FullAnalysisWorker

def test_pgn_worker():
    with patch('src.core.workers.convert_pgn_to_parquet') as mock_conv:
        worker = PGNWorker("test.pgn")
        worker.run()
        mock_conv.assert_called_once()

def test_pgn_append_worker():
    # Parchear polars a nivel global del sistema para los tests
    with patch('polars.scan_parquet') as mock_scan, \
         patch('src.core.workers.convert_pgn_to_parquet'), \
         patch('polars.concat') as mock_concat, \
         patch('os.remove'), patch('os.rename'), patch('os.path.exists', return_value=True):
        
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.cast.return_value = mock_lf
        mock_lf.select.return_value.collect.return_value.item.return_value = 10
        mock_lf.with_columns.return_value = mock_lf
        
        mock_res = MagicMock()
        mock_concat.return_value.collect.return_value = mock_res
        
        worker = PGNAppendWorker("new.pgn", "target.parquet")
        worker.run()
        assert mock_concat.called

def test_cache_populator_worker():
    mock_db = MagicMock()
    mock_app_db = MagicMock()
    mock_db.get_reference_path.return_value = "ref.p"
    mock_view = MagicMock()
    mock_view.select.return_value.collect.return_value.item.return_value = 10
    mock_db.get_reference_view.return_value = mock_view
    mock_app_db.get_opening_stats.return_value = (None, None)
    
    worker = CachePopulatorWorker(mock_db, mock_app_db, min_games=100)
    with patch.object(CachePopulatorWorker, '_calculate_stats_sync') as mock_calc:
        mock_calc.return_value = pl.DataFrame(schema={"uci": pl.String, "c": pl.Int64})
        worker.running = True
        
        # Parchear deque en collections ya que el import es interno en el worker
        with patch('collections.deque') as mock_deque:
            mock_q = MagicMock()
            mock_q.__bool__.side_effect = [True, False]
            mock_q.popleft.return_value = chess.Board()
            mock_deque.return_value = mock_q
            
            worker.run()
            assert mock_calc.called

def test_tree_scanner_worker():
    with patch('chess.engine.SimpleEngine.popen_uci') as mock_popen:
        mock_engine = MagicMock()
        mock_popen.return_value = mock_engine
        mock_score = MagicMock()
        mock_score.white.return_value.is_mate.return_value = False
        mock_score.white.return_value.score.return_value = 100
        mock_engine.analyse.return_value = {"score": mock_score}
        worker = TreeScannerWorker("sf", chess.STARTING_FEN, ["e2e4"])
        worker.run()
        assert mock_engine.quit.called

def test_full_analysis_worker():
    with patch('chess.engine.SimpleEngine.popen_uci') as mock_popen:
        mock_engine = MagicMock()
        mock_popen.return_value = mock_engine
        mock_engine.analyse.return_value = {"score": MagicMock()}
        worker = FullAnalysisWorker([chess.Move.from_uci("e2e4")], depth=10)
        worker.run()
        assert mock_engine.analyse.called
