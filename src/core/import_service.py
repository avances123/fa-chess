from PySide6.QtCore import QObject, Signal
from src.core.workers import PGNWorker, PGNAppendWorker
from src.config import logger

class ImportService(QObject):
    """
    Servicio encargado de la importación de archivos PGN a formato Parquet.
    Maneja los workers y centraliza las señales de progreso.
    """
    import_started = Signal(str) # mensaje
    import_progress = Signal(int)
    import_finished = Signal(bool, str) # success, message
    database_ready = Signal(str) # path de la nueva base

    def __init__(self, db_manager):
        super().__init__()
        self.db = db_manager
        self.worker = None

    def import_new_db(self, pgn_path, parquet_path):
        """Inicia la importación de un PGN a un nuevo archivo Parquet."""
        if self.worker and self.worker.isRunning():
            return False, "Ya hay una importación en curso"
        
        self.import_started.emit(f"Importando {pgn_path}...")
        self.worker = PGNWorker(pgn_path, parquet_path)
        
        # Conexiones
        self.worker.progress.connect(self.import_progress.emit)
        self.worker.finished.connect(lambda p: self._on_import_finished(p, True))
        self.worker.error.connect(lambda e: self.import_finished.emit(False, e))
        
        self.worker.start()
        return True, "Importación iniciada"

    def append_to_db(self, pgn_path, db_name):
        """Añade partidas de un PGN a una base de datos existente."""
        if self.worker and self.worker.isRunning():
            return False, "Ya hay una importación en curso"
        
        if db_name not in self.db.db_metadata:
            return False, "Base de datos no encontrada"
            
        parquet_path = self.db.db_metadata[db_name]["path"]
        self.import_started.emit(f"Añadiendo partidas a {db_name}...")
        
        self.worker = PGNAppendWorker(pgn_path, parquet_path)
        
        # Conexiones
        self.worker.progress.connect(self.import_progress.emit)
        self.worker.finished.connect(lambda p: self._on_import_finished(p, False))
        
        self.worker.start()
        return True, "Anexado iniciado"

    def _on_import_finished(self, path, is_new):
        if not path:
            self.import_finished.emit(False, "Error en la conversión")
            return
            
        if is_new:
            # Si es nueva, la cargamos en el manager
            name = self.db.load_parquet(path)
            self.database_ready.emit(path)
            self.import_finished.emit(True, f"Nueva base '{name}' creada con éxito")
        else:
            # Si es un append, recargamos la base para ver los cambios
            name = self.db.active_db_name
            self.db.reload_db(name)
            self.import_finished.emit(True, f"Partidas añadidas a '{name}'")
