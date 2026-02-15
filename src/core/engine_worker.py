import chess
import chess.engine
import os
from PySide6.QtCore import QThread, Signal

class EngineWorker(QThread):
    # Señal que envía: (evaluación_str, mejor_movimiento_uci, línea_principal_san)
    info_updated = Signal(str, str, list)

    def __init__(self, engine_path="/usr/bin/stockfish", threads=1, hash_mb=64, depth_limit=0):
        super().__init__()
        self.engine_path = engine_path
        self.threads = threads
        self.hash_mb = hash_mb
        self.depth_limit = depth_limit
        self.engine = None
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
            self.engine = chess.engine.SimpleEngine.popen_uci(self.engine_path)
            self.engine.configure({"Threads": self.threads, "Hash": self.hash_mb})
            
            last_analysed_fen = None
            
            while self._is_running:
                if self.current_fen and self.current_fen != last_analysed_fen:
                    current_board_fen = self.current_fen
                    last_analysed_fen = current_board_fen
                    board = chess.Board(current_board_fen)
                    
                    limit = chess.engine.Limit(depth=self.depth_limit) if self.depth_limit > 0 else None
                    with self.engine.analysis(board, limit) as analysis:
                        for info in analysis:
                            if not self._is_running or self.current_fen != current_board_fen:
                                break
                            
                            score = info.get("score")
                            pv = info.get("pv")
                            depth = info.get("depth")
                            nps = info.get("nps")
                            
                            if score and pv and depth:
                                score_str = self._format_score(score, board.turn)
                                speed = f"{int(nps/1000)}k nps" if nps else ""
                                full_info = f"d:{depth} | {speed} | {score_str}"
                                
                                best_move_uci = pv[0].uci()
                                mainline = []
                                temp_board = board.copy()
                                for m in pv[:5]:
                                    if m in temp_board.legal_moves:
                                        mainline.append(temp_board.san(m))
                                        temp_board.push(m)
                                    else: break
                                self.info_updated.emit(full_info, best_move_uci, mainline)
                
                self.msleep(50)
        except Exception as e:
            print(f"Error crítico en EngineWorker: {e}")
        finally:
            if self.engine:
                try: self.engine.quit()
                except: pass

    def _format_score(self, score, turn):
        if score.is_mate():
            mate = score.relative.mate()
            return f"M{abs(mate)}" if mate > 0 else f"-M{abs(mate)}"
        cp = score.relative.score(mate_score=10000)
        if turn == chess.BLACK: cp = -cp
        return f"{cp/100:+.2f}"

class TreeScannerWorker(QThread):
    """Analizador secundario que recorre el árbol posición a posición rápidamente"""
    eval_ready = Signal(str, str) # uci, score_str (ej. +0.45 o M2)

    def __init__(self, engine_path, fen, moves_uci, depth=12):
        super().__init__()
        self.engine_path = engine_path
        self.fen = fen
        self.moves = moves_uci
        self.depth = depth
        self._is_running = True

    def run(self):
        engine = None
        try:
            engine = chess.engine.SimpleEngine.popen_uci(self.engine_path)
            engine.configure({"Threads": 1, "Hash": 16})
            
            for move_uci in self.moves:
                if not self._is_running: break
                
                try:
                    board = chess.Board(self.fen)
                    move = chess.Move.from_uci(move_uci)
                    if move in board.legal_moves:
                        board.push(move)
                        # Análisis ultra-rápido para el árbol (usando profundidad configurada)
                        info = engine.analyse(board, chess.engine.Limit(time=0.05, depth=self.depth))
                        score_obj = info.get("score")
                        if score_obj:
                            # Devolvemos siempre la evaluación desde el punto de vista del blanco (formateada)
                            score_white = score_obj.white()
                            if score_white.is_mate():
                                mate_val = score_white.mate()
                                res = f"M{abs(mate_val)}" if mate_val > 0 else f"-M{abs(mate_val)}"
                            else:
                                res = f"{score_white.score()/100:+.2f}"
                            self.eval_ready.emit(move_uci, res)
                except: continue
        except Exception as e:
            print(f"Error en TreeScanner: {e}")
        finally:
            if engine:
                try: engine.quit()
                except: pass

    def stop(self):
        self._is_running = False

class FullAnalysisWorker(QThread):
    progress = Signal(int, int)
    analysis_result = Signal(int, int)
    finished = Signal()
    error_occurred = Signal(str)

    def __init__(self, moves, depth=10, engine_path=None):
        super().__init__()
        self.moves = moves
        self.depth = depth
        self.engine_path = engine_path
        self.threads = 1
        self.hash_mb = 16
        self.running = True

    def run(self):
        try:
            import shutil
            engine_path = self.engine_path or shutil.which("stockfish") or "/usr/bin/stockfish"
            engine = chess.engine.SimpleEngine.popen_uci(engine_path)
            engine.configure({"Threads": self.threads, "Hash": self.hash_mb})
            
            board = chess.Board()
            self.analyze_position(engine, board, 0)
            
            for i, move in enumerate(self.moves):
                if not self.running: break
                board.push(move)
                self.analyze_position(engine, board, i + 1)
                self.progress.emit(i + 1, len(self.moves))
            
            engine.quit()
        except Exception as e:
            self.error_occurred.emit(str(e))
        finally:
            self.finished.emit()

    def analyze_position(self, engine, board, idx):
        try:
            info = engine.analyse(board, chess.engine.Limit(time=0.01, depth=self.depth))
            score_obj = info.get("score")
            if score_obj:
                score = score_obj.white()
                val = 2000 if score.is_mate() and score.mate() > 0 else (-2000 if score.is_mate() else score.score())
                self.analysis_result.emit(idx, val)
        except: self.analysis_result.emit(idx, 0)

    def stop(self): self.running = False
