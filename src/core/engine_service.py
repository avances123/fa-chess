import chess.engine
import os
from PySide6.QtCore import QObject, Signal, QTimer
from src.config import logger

class EngineService(QObject):
    """
    Servicio de alto nivel para gestionar motores de ajedrez.
    Mantiene un proceso vivo y gestiona el flujo de análisis.
    """
    info_updated = Signal(dict) # d, score, cp, pv, nps, speed
    error_occurred = Signal(str)

    def __init__(self, engine_path, threads=1, hash_mb=64):
        super().__init__()
        self.engine_path = engine_path
        self.threads = threads
        self.hash_mb = hash_mb
        self.engine = None
        self._is_active = False
        self._current_analysis = None
        
        # Auto-reinicio si el motor muere
        self.reconnect_timer = QTimer()
        self.reconnect_timer.setSingleShot(True)
        self.reconnect_timer.timeout.connect(self.start)

    def start(self):
        if not self.engine_path or not os.path.exists(self.engine_path):
            self.error_occurred.emit(f"Motor no encontrado: {self.engine_path}")
            return False
        
        try:
            self.engine = chess.engine.SimpleEngine.popen_uci(self.engine_path)
            self.engine.configure({"Threads": self.threads, "Hash": self.hash_mb})
            self._is_active = True
            logger.info(f"EngineService: Motor iniciado ({self.engine_path})")
            return True
        except Exception as e:
            logger.error(f"EngineService: Fallo al iniciar motor: {e}")
            self.error_occurred.emit(str(e))
            return False

    def stop(self):
        self._is_active = False
        if self.engine:
            try:
                self.engine.quit()
            except:
                pass
            self.engine = None
            logger.info("EngineService: Motor detenido")

    def analyze(self, board, limit=None):
        """Inicia un análisis asíncrono sobre un tablero."""
        if not self.engine or not self._is_active:
            return
        
        # Detener análisis previo si existe
        if self._current_analysis:
            self._current_analysis.stop()

        try:
            self._current_analysis = self.engine.analysis(board, limit)
            # En un entorno real, procesaríamos el iterador en un hilo aparte
            # Para este servicio, expondremos una API que el Worker pueda usar.
            return self._current_analysis
        except Exception as e:
            logger.error(f"EngineService: Error en análisis: {e}")
            self.reconnect_timer.start(2000)
            return None

    def get_quick_eval(self, board, time_limit=0.1):
        """Evaluación rápida síncrona."""
        if not self.engine: return None
        try:
            info = self.engine.analyse(board, chess.engine.Limit(time=time_limit))
            return info.get("score")
        except:
            return None
