import pytest
import chess
from src.core.engine_worker import EngineWorker, FullAnalysisWorker
import subprocess

def test_engine_worker_full(monkeypatch, qtbot):
    class MockProcess:
        def __init__(self):
            self.stdin = type('obj', (object,), {'write': lambda s, x: None, 'flush': lambda s: None})()
            self.stdout = type('obj', (object,), {'readline': lambda s: b"bestmove e2e4\n"})()
        def poll(self): return None
        def terminate(self): pass
        def wait(self, t=None): pass

    monkeypatch.setattr(subprocess, "Popen", lambda *args, **kwargs: MockProcess())
    
    worker = EngineWorker("dummy")
    worker.update_position(chess.STARTING_FEN)
    worker.stop()
    assert worker.engine_path == "dummy"

def test_full_analysis_worker(monkeypatch, qtbot):
    worker = FullAnalysisWorker([chess.Move.from_uci("e2e4")], engine_path="dummy")
    assert worker.depth == 10
