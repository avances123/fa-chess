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
            "engine_path": "/usr/bin/stockfish", # Legacy, kept for fallback
            "engine_threads": 1, # Legacy
            "engine_hash": 64,   # Legacy
            "engine_depth": 10,
            "tree_depth": 12,
            "min_games": 20,
            "venom_eval": 0.5,
            "venom_win": 52,
            "practical_win": 60,
            "colors": {"light": "#eeeed2", "dark": "#8ca2ad"},
            "open_dbs": [],
            "active_db": None,
            "engines": [], # List of dicts: {name, path, options}
            "active_engine": "Default"
        }
        self._migrate_legacy_engine_config()

    def _migrate_legacy_engine_config(self):
        """Migra la configuración antigua de un solo motor a la nueva lista si es necesario."""
        engines = self.get("engines")
        if not engines:
            # Si no hay lista de motores, intentamos crear uno con la config legacy
            path = self.get("engine_path")
            if path:
                threads = self.get("engine_threads")
                hash_mb = self.get("engine_hash")
                
                default_engine = {
                    "name": "Default",
                    "path": path,
                    "options": {
                        "Threads": threads,
                        "Hash": hash_mb
                    }
                }
                self.set("engines", [default_engine])
                self.set("active_engine", "Default")

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

    # --- ENGINE MANAGEMENT ---

    def get_engines(self):
        return self.get("engines") or []

    def save_engine(self, engine_data):
        """Añade o actualiza un motor en la lista (por nombre)."""
        engines = self.get_engines()
        # Buscar si ya existe y actualizar
        for i, eng in enumerate(engines):
            if eng["name"] == engine_data["name"]:
                engines[i] = engine_data
                self.set("engines", engines)
                return
        # Si no existe, añadir
        engines.append(engine_data)
        self.set("engines", engines)

    def remove_engine(self, engine_name):
        engines = self.get_engines()
        engines = [e for e in engines if e["name"] != engine_name]
        self.set("engines", engines)

    def set_active_engine(self, engine_name):
        self.set("active_engine", engine_name)

    def get_active_engine_config(self):
        name = self.get("active_engine")
        engines = self.get_engines()
        for e in engines:
            if e["name"] == name:
                return e
        return None
