import polars as pl
import chess
import chess.polyglot
from PySide6.QtCore import QObject, Signal, QTimer
from src.core.workers import StatsWorker
from src.core.eco import ECOManager
from src.config import ECO_FILE, logger

def format_chess_score(score):
    """Utilidad de formateo unificada para todo el sistema."""
    if score is None: return ""
    if isinstance(score, str): return score
    
    if abs(score) > 50: 
        # Cálculo de distancia al mate (asumiendo 10000 como base)
        dist = int(10000 - abs(score))
        dist = max(1, dist)
        return f"M{dist}" if score > 0 else f"-M{dist}"
    return f"{score:+.2f}"

class OpeningService(QObject):
    """
    Servicio que coordina la identificación de aperturas, estadísticas de BD
    y análisis de variantes en el árbol mediante motor en vivo.
    """
    stats_ready = Signal(pl.DataFrame, str, object) 
    tree_eval_ready = Signal(str, str) 
    progress_updated = Signal(int)
    tree_progress = Signal(int, int) # (actual, total)
    tree_finished = Signal()
    status_message = Signal(str)

    def __init__(self, db_manager, app_db=None):
        super().__init__()
        self.db = db_manager
        self.app_db = app_db
        self.eco = ECOManager(ECO_FILE)
        self.stats_worker = None
        self.tree_worker = None
        self.engine_path = None
        self.tree_depth = 12
        
        self.timer = QTimer()
        self.timer.setSingleShot(True)
        self.timer.timeout.connect(self._run_stats_worker)
        self.last_request = None

    def set_engine_params(self, path, depth):
        self.engine_path = path
        self.tree_depth = depth

    def request_stats(self, board):
        if self.tree_worker and self.tree_worker.isRunning():
            self.tree_worker.stop()

        self.last_request = {
            "hash": chess.polyglot.zobrist_hash(board),
            "line_uci": " ".join([m.uci() for m in board.move_stack]),
            "is_white": board.turn == chess.WHITE,
            "board_copy": board.copy(),
            "fen": board.fen()
        }
        self.timer.start(150)

    def _run_stats_worker(self):
        if not self.last_request or not self.db.active_db_name: return

        if self.stats_worker and self.stats_worker.isRunning():
            try: self.stats_worker.disconnect()
            except: pass

        self.status_message.emit("Calculando estadísticas de base de datos...")
        self.stats_worker = StatsWorker(
            self.db, self.last_request["line_uci"], self.last_request["is_white"],
            current_hash=self.last_request["hash"], app_db=self.app_db
        )
        
        opening_name, _ = self.eco.get_opening_name(self.last_request["board_copy"])
        self.stats_worker.finished.connect(lambda df, ev: self._on_stats_worker_finished(df, ev, opening_name))
        self.stats_worker.progress.connect(self.progress_updated.emit)
        self.stats_worker.start()

    def _on_stats_worker_finished(self, df, current_eval, opening_name):
        # Si no hay datos en la BD, inyectamos movimientos legales para que el árbol no esté vacío
        # y el TreeScanner pueda empezar a trabajar.
        if (df is None or df.is_empty()) and self.last_request:
            board = self.last_request["board_copy"]
            # Tomamos hasta 15 movimientos legales (prioriza capturas y jaques por orden de python-chess)
            legal_moves = [m.uci() for m in board.legal_moves][:15]
            if legal_moves:
                df = pl.DataFrame({
                    "uci": legal_moves,
                    "c": [0] * len(legal_moves),
                    "w": [0] * len(legal_moves),
                    "d": [0] * len(legal_moves),
                    "b": [0] * len(legal_moves),
                    "avg_w_elo": [0] * len(legal_moves),
                    "avg_b_elo": [0] * len(legal_moves)
                })

        formatted_eval = format_chess_score(current_eval)
        self.stats_ready.emit(df, opening_name, formatted_eval)

    def start_tree_analysis(self, moves_uci):
        if not self.engine_path or not moves_uci: return
        if self.tree_worker and self.tree_worker.isRunning():
            self.tree_worker.stop()
            self.tree_worker.wait()
            
        from src.core.engine_worker import TreeScannerWorker
        self.status_message.emit(f"Analizando árbol ({len(moves_uci)} jugadas)...")
        self.tree_worker = TreeScannerWorker(
            self.engine_path, self.last_request["fen"], moves_uci, depth=self.tree_depth
        )
        self.tree_worker.eval_ready.connect(self.tree_eval_ready.emit)
        self.tree_worker.progress.connect(self.tree_progress.emit)
        self.tree_worker.finished.connect(self.tree_finished.emit)
        self.tree_worker.start()

    @property
    def current_opening_name(self):
        if not self.last_request: return "Posición Inicial"
        return self.eco.get_opening_name(self.last_request["board_copy"])[0]
