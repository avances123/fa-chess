import chess
import chess.engine
import os
from PySide6.QtCore import QThread, Signal

class EngineWorker(QThread):
    # Señal que envía: (evaluación_str, mejor_movimiento, línea_principal)
    info_updated = Signal(str, str, list)

    def __init__(self, engine_path="/usr/bin/stockfish"):
        super().__init__()
        self.engine_path = engine_path
        self.engine = None
        self.board = None
        self._is_running = True
        self.current_fen = None

    def stop(self):
        self._is_running = False
        if self.engine:
            try:
                self.engine.quit()
            except:
                pass
            self.engine = None

    def update_position(self, fen):
        self.current_fen = fen

    def run(self):
        try:
            # En python-chess síncrono, popen_uci devuelve directamente (transport, engine)
            # No es una corrutina.
            self.engine = chess.engine.SimpleEngine.popen_uci(self.engine_path)
            
            while self._is_running:
                if self.current_fen:
                    board = chess.Board(self.current_fen)
                    # Usamos la API síncrona de SimpleEngine
                    info = self.engine.analyse(board, chess.engine.Limit(time=0.1))
                    
                    if not self._is_running or self.current_fen != board.fen():
                        continue
                    
                    score = info.get("score")
                    pv = info.get("pv")
                    
                    if score and pv:
                        score_str = self._format_score(score, board.turn)
                        best_move_uci = pv[0].uci() if pv else ""
                        
                        mainline = []
                        temp_board = board.copy()
                        for m in pv[:5]:
                            if m in temp_board.legal_moves:
                                mainline.append(temp_board.san(m))
                                temp_board.push(m)
                            else:
                                break
                        self.info_updated.emit(score_str, best_move_uci, mainline)
                
                self.msleep(100)
                    
        except Exception as e:
            print(f"Error en el motor: {e}")
        finally:
            if self.engine:
                self.engine.quit()

    def _format_score(self, score, turn):
        if score.is_mate():
            mate = score.relative.mate()
            return f"M{abs(mate)}" if mate > 0 else f"-M{abs(mate)}"
        
        cp = score.relative.score(mate_score=10000)
        # Ajustar para que siempre sea desde el punto de vista de las blancas
        if turn == chess.BLACK:
            cp = -cp
        return f"{cp/100:+.2f}"
