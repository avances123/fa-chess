from src.core.app_db import AppDBManager
from src.config import APP_DB_FILE

class ConfigService:
    """
    Servicio centralizado para la gestión de configuración y preferencias.
    Desacopla la persistencia de la lógica de UI.
    """
    def __init__(self, db_path=APP_DB_FILE):
        self.app_db = AppDBManager(db_path)
        self._defaults = {
            "perf_threshold": 25,
            "engine_path": "/usr/bin/stockfish",
            "engine_threads": 1,
            "engine_hash": 64,
            "engine_depth": 10,
            "tree_depth": 12,
            "min_games": 20,
            "venom_eval": 0.5,
            "venom_win": 52,
            "practical_win": 60,
            "colors": {"light": "#eeeed2", "dark": "#8ca2ad"},
            "open_dbs": [],
            "active_db": None
        }

    def get(self, key):
        """Obtiene un valor de configuración con fallback al valor por defecto."""
        default = self._defaults.get(key)
        return self.app_db.get_config(key, default)

    def set(self, key, value):
        """Guarda un valor de configuración."""
        self.app_db.set_config(key, value)

    def get_all(self):
        """Devuelve un diccionario con toda la configuración actual."""
        return {key: self.get(key) for key in self._defaults}

    def save_bulk(self, config_dict):
        """Guarda múltiples valores a la vez."""
        for k, v in config_dict.items():
            self.set(k, v)
