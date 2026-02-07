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

class FullAnalysisWorker(QThread):
    progress = Signal(int, int) # current_move, total_moves
    analysis_result = Signal(int, int) # move_idx, cp_score
    finished = Signal()
    error_occurred = Signal(str) # Nueva señal de error

    def __init__(self, moves, depth=10):
        super().__init__()
        self.moves = moves
        self.depth = depth
        self.running = True

    def run(self):
        try:
            import shutil
            # Búsqueda robusta del motor
            engine_path = shutil.which("stockfish")
            if not engine_path:
                for p in ["/usr/games/stockfish", "/usr/bin/stockfish", "/usr/local/bin/stockfish"]:
                    if os.path.exists(p):
                        engine_path = p
                        break
            
            if not engine_path:
                self.error_occurred.emit("No se encontró el ejecutable de Stockfish. Asegúrate de tenerlo instalado.")
                return

            engine = chess.engine.SimpleEngine.popen_uci(engine_path)
            
            board = chess.Board()
            # Analizar posición inicial (índice 0)
            self.analyze_position(engine, board, 0)
            
            for i, move in enumerate(self.moves):
                if not self.running: break
                board.push(move)
                # Analizar tras el movimiento (índice i+1)
                self.analyze_position(engine, board, i + 1)
                self.progress.emit(i + 1, len(self.moves))
            
            engine.quit()
        except Exception as e:
            self.error_occurred.emit(f"Error crítico durante el análisis: {str(e)}")
        finally:
            self.finished.emit()

    def analyze_position(self, engine, board, idx):
        try:
            # Análisis rápido por profundidad
            info = engine.analyse(board, chess.engine.Limit(depth=self.depth))
            score_obj = info.get("score")
            
            if score_obj:
                score = score_obj.white()
                if score.is_mate():
                    val = 2000 if score.mate() > 0 else -2000
                else:
                    val = score.score()
                self.analysis_result.emit(idx, val)
        except:
            self.analysis_result.emit(idx, 0)

    def stop(self):
        self.running = False
