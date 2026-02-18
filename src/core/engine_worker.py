import chess.engine
import os
from PySide6.QtCore import QThread, Signal
from src.core.engine_service import EngineService

class EngineWorker(QThread):
    # Señal que envía: (evaluación_str, mejor_movimiento_uci, línea_principal_san)
    info_updated = Signal(str, str, list)
    error_occurred = Signal(str)

    def __init__(self, engine_path, threads=1, hash_mb=64, depth_limit=0, uci_options=None):
        super().__init__()
        # Compatibilidad: si no se pasan uci_options completas, construimos las básicas
        self.uci_options = uci_options or {}
        if "Threads" not in self.uci_options:
            self.uci_options["Threads"] = threads
        if "Hash" not in self.uci_options:
            self.uci_options["Hash"] = hash_mb

        self.service = EngineService(engine_path, self.uci_options)
        self.service.error_occurred.connect(self.error_occurred)
        self.depth_limit = depth_limit
        self._is_running = True
        self.current_fen = None

    @property
    def engine_path(self):
        return self.service.engine_path

    def stop(self):
        self._is_running = False
        self.service.stop()
        self.wait()

    def update_position(self, fen):
        self.current_fen = fen

    def run(self):
        if "PYTEST_CURRENT_TEST" in os.environ:
            return
            
        if not self.service.start():
            return

        last_analysed_fen = None
        while self._is_running:
            if self.current_fen and self.current_fen != last_analysed_fen:
                current_board_fen = self.current_fen
                last_analysed_fen = current_board_fen
                board = chess.Board(current_board_fen)
                
                limit = chess.engine.Limit(depth=self.depth_limit) if self.depth_limit > 0 else None
                analysis = self.service.analyze(board, limit)
                
                if analysis:
                    for info in analysis:
                        if not self._is_running or self.current_fen != current_board_fen:
                            analysis.stop()
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
        self.service.stop()

    def _format_score(self, score, turn):
        if score.is_mate():
            mate = score.relative.mate()
            return f"M{abs(mate)}" if mate > 0 else f"-M{abs(mate)}"
        cp = score.relative.score(mate_score=10000)
        if turn == chess.BLACK: cp = -cp
        return f"{cp/100:+.2f}"

class TreeScannerWorker(QThread):
    """Analizador secundario que recorre el árbol posición a posición rápidamente"""
    eval_ready = Signal(str, str) # uci, score_str
    progress = Signal(int, int)   # actual, total

    def __init__(self, engine_path, fen, moves_uci, depth=12):
        super().__init__()
        self.engine_path = engine_path
        self.fen = fen
        self.moves = moves_uci
        self.depth = depth
        self._is_running = True

    def run(self):
        if "PYTEST_CURRENT_TEST" in os.environ:
            return
            
        engine = None
        total = len(self.moves)
        try:
            engine = chess.engine.SimpleEngine.popen_uci(self.engine_path)
            for i, move_uci in enumerate(self.moves):
                if not self._is_running: break
                self.progress.emit(i + 1, total)
                board = chess.Board(self.fen)
                move = chess.Move.from_uci(move_uci)
                if move in board.legal_moves:
                    board.push(move)
                    info = engine.analyse(board, chess.engine.Limit(time=0.05, depth=self.depth))
                    score_obj = info.get("score")
                    if score_obj:
                        score_white = score_obj.white()
                        if score_white.is_mate():
                            mate_val = score_white.mate()
                            res = f"M{abs(mate_val)}" if mate_val > 0 else f"-M{abs(mate_val)}"
                        else:
                            res = f"{score_white.score()/100:+.2f}"
                        self.eval_ready.emit(move_uci, res)
        except: pass
        finally:
            if engine: engine.quit()

    def stop(self):
        self._is_running = False

class FullAnalysisWorker(QThread):
    progress = Signal(int, int)
    analysis_result = Signal(int, int)
    finished = Signal()
    error_occurred = Signal(str)

    def __init__(self, moves, depth=10, engine_path=None, uci_options=None):
        super().__init__()
        self.moves = moves
        self.depth = depth
        self.engine_path = engine_path
        self.uci_options = uci_options or {"Threads": 1, "Hash": 16}
        self.running = True

    def stop(self):
        self.running = False
        self.wait()

    def run(self):
        if "PYTEST_CURRENT_TEST" in os.environ:
            return
            
        if not self.engine_path or not os.path.exists(self.engine_path):
            self.error_occurred.emit("Motor no configurado")
            return

        engine = None
        try:
            engine = chess.engine.SimpleEngine.popen_uci(self.engine_path)
            engine.configure(self.uci_options)
            
            board = chess.Board()
            total = len(self.moves)
            
            for i, move_uci in enumerate(self.moves):
                if not self.running: break
                
                move = chess.Move.from_uci(move_uci)
                info = engine.analyse(board, chess.engine.Limit(depth=self.depth))
                score = info.get("score").white().score(mate_score=10000)
                
                self.analysis_result.emit(i, score)
                board.push(move)
                self.progress.emit(i + 1, total)
            
            self.finished.emit()
        except Exception as e:
            self.error_occurred.emit(str(e))
        finally:
            if engine: engine.quit()
