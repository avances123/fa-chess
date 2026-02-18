import os
import chess
from PySide6.QtCore import QObject, Signal
from src.core.engine_worker import FullAnalysisWorker
from src.config import logger

class AnalysisService(QObject):
    """
    Servicio encargado del análisis profundo de partidas completas.
    Gestiona el ciclo de vida del FullAnalysisWorker y reporta progreso y resultados.
    """
    analysis_started = Signal(int) # total_moves
    progress_updated = Signal(int, int) # current, total
    move_analysed = Signal(int, int) # index, score
    analysis_finished = Signal()
    analysis_error = Signal(str)

    def __init__(self):
        super().__init__()
        self.worker = None
        self.engine_path = None
        self.depth = 10

    def set_engine_params(self, engine_path, depth):
        self.engine_path = engine_path
        self.depth = depth

    def start_full_analysis(self, moves_uci):
        """Inicia el análisis de una lista de movimientos UCI."""
        if not self.engine_path or not os.path.exists(self.engine_path):
            self.analysis_error.emit("Motor de ajedrez no configurado correctamente")
            return False

        if self.worker and self.worker.isRunning():
            return False

        self.worker = FullAnalysisWorker(moves_uci, depth=self.depth, engine_path=self.engine_path)
        
        # Conectar señales del worker a las del servicio
        self.worker.progress.connect(self.progress_updated.emit)
        self.worker.analysis_result.connect(self.move_analysed.emit)
        self.worker.finished.connect(self.analysis_finished.emit)
        self.worker.error_occurred.connect(self.analysis_error.emit)
        
        self.analysis_started.emit(len(moves_uci))
        self.worker.start()
        return True

    def stop_analysis(self):
        """Detiene el análisis en curso."""
        if self.worker and self.worker.isRunning():
            self.worker.stop()
            return True
        return False

    def is_running(self):
        return self.worker is not None and self.worker.isRunning()
