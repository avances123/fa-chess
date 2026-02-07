import chess.engine
from unittest.mock import MagicMock

class MockScore:
    def __init__(self, cp=None, mate=None):
        self._cp = cp
        self._mate = mate
        
    def white(self):
        return self
        
    def score(self, mate_score=10000):
        if self.is_mate():
            return 2000 if self._mate > 0 else -2000
        return self._cp if self._cp is not None else 0
        
    def is_mate(self):
        return self._mate is not None
        
    def mate(self):
        return self._mate

class MockInfo:
    def __init__(self, cp=50):
        self.score = MockScore(cp=cp)
    
    def get(self, key):
        if key == "score": return self.score
        return None
    
    def __getitem__(self, key):
        return self.get(key)

class MockEngine:
    def __init__(self, path=None):
        self.return_value = {"score": MockScore(cp=35)}
        
    def analyse(self, board, limit):
        # Simula un análisis devolviendo un diccionario con score
        # Podemos variar el score según el turno para hacerlo más realista si queremos
        val = 50 if board.turn == chess.WHITE else -50
        return {"score": MockScore(cp=val)}
        
    def analysis(self, board, limit=None):
        # Simula un generador de análisis
        # Devuelve una lista con un solo resultado
        val = 100
        mock_info = {"score": MockScore(cp=val), "pv": [chess.Move.from_uci("e2e4")]}
        yield mock_info
        
    def quit(self):
        pass
    
    def close(self):
        pass

def mock_popen_uci(path):
    return MockEngine(path)
