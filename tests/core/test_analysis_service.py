import pytest
import chess
import os
import subprocess
from src.core.analysis_service import AnalysisService

def test_analysis_service_flow(qtbot, monkeypatch):
    # Mock de un proceso de motor UCI mínimo
    class MockProcess:
        def __init__(self):
            self.stdin = type('obj', (object,), {'write': lambda s, x: None, 'flush': lambda s: None})()
            self.stdout = type('obj', (object,), {'readline': self.readline})()
            self._counter = 0
        def readline(self):
            self._counter += 1
            if self._counter % 2 == 0:
                return b"info depth 10 score cp 50 pv e2e4\n"
            return b"bestmove e2e4\n"
        def poll(self): return None
        def terminate(self): pass
        def wait(self, t=None): pass

    # Forzar que el motor parezca que existe
    monkeypatch.setattr(subprocess, "Popen", lambda *args, **kwargs: MockProcess())
    monkeypatch.setattr(os.path, "exists", lambda p: True)

    service = AnalysisService()
    service.set_engine_params("fake_path", 10)
    
    moves = ["e2e4", "e7e5"]
    
    # 1. Iniciar análisis
    with qtbot.waitSignal(service.analysis_started) as blocker:
        service.start_full_analysis(moves)
    assert blocker.args == [2]

    # 2. Verificar que el worker está corriendo
    assert service.is_running()

    # 3. Parar el análisis
    service.stop_analysis()
    qtbot.wait(200) # Pequeña espera para que el hilo pare
    assert not service.is_running()
