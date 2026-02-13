import chess
import chess.engine
import os
from PySide6.QtCore import QThread, Signal

class EngineWorker(QThread):
    # Señal que envía: (evaluación_str, mejor_movimiento, línea_principal)
    info_updated = Signal(str, str, list)

    def __init__(self, engine_path="/usr/bin/stockfish", threads=1, hash_mb=64):
        super().__init__()
        self.engine_path = engine_path
        self.threads = threads
        self.hash_mb = hash_mb
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
            # Iniciar motor con parámetros configurados
            self.engine = chess.engine.SimpleEngine.popen_uci(self.engine_path)
            self.engine.configure({"Threads": self.threads, "Hash": self.hash_mb})
            
            last_fen = None
            
            while self._is_running:
                if self.current_fen and self.current_fen != last_fen:
                    last_fen = self.current_fen
                    board = chess.Board(self.current_fen)
                    
                    # Modo de análisis infinito (sin límite de tiempo ni profundidad)
                    with self.engine.analysis(board) as analysis:
                        for info in analysis:
                            # Si la posición cambia globalmente o cerramos, salir del bucle interno
                            if not self._is_running or self.current_fen != board.fen():
                                break
                            
                            score = info.get("score")
                            pv = info.get("pv")
                            depth = info.get("depth")
                            nps = info.get("nps") # Nodos por segundo
                            
                            if score and pv and depth:
                                score_str = self._format_score(score, board.turn)
                                # Añadir información de profundidad y velocidad al string
                                if nps:
                                    speed = f"{int(nps/1000)}k nps"
                                    full_info = f"d:{depth} | {speed} | {score_str}"
                                else:
                                    full_info = f"d:{depth} | {score_str}"
                                    
                                best_move_uci = pv[0].uci() if pv else ""
                                
                                mainline = []
                                temp_board = board.copy()
                                for m in pv[:5]:
                                    if m in temp_board.legal_moves:
                                        mainline.append(temp_board.san(m))
                                        temp_board.push(m)
                                    else:
                                        break
                                self.info_updated.emit(full_info, best_move_uci, mainline)
                
                self.msleep(50)
                    
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

    def __init__(self, moves, depth=10, engine_path=None):
        super().__init__()
        self.moves = moves
        self.depth = depth
        self.engine_path = engine_path
        # Parámetros hardcodeados para análisis rápido/ligero
        self.threads = 1
        self.hash_mb = 16
        self.running = True

    def run(self):
        try:
            import shutil
            # Búsqueda robusta del motor si no se proporciona
            if not self.engine_path:
                self.engine_path = shutil.which("stockfish")
                if not self.engine_path:
                    for p in ["/usr/games/stockfish", "/usr/bin/stockfish", "/usr/local/bin/stockfish"]:
                        if os.path.exists(p):
                            self.engine_path = p
                            break
            
            if not self.engine_path or not os.path.exists(self.engine_path):
                self.error_occurred.emit(f"No se encontró Stockfish en: {self.engine_path}")
                return

            engine = chess.engine.SimpleEngine.popen_uci(self.engine_path)
            engine.configure({"Threads": self.threads, "Hash": self.hash_mb})
            
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
            # Análisis ultra-rápido para el gráfico: 0.01 segundos o profundidad, lo que ocurra antes
            info = engine.analyse(board, chess.engine.Limit(time=0.01, depth=self.depth))
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
