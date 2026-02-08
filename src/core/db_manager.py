import os
import json
import time
import polars as pl
from datetime import datetime
from config import CONFIG_FILE
from PySide6.QtCore import QObject, Signal

# Rutas del Proyecto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLIPBASE_FILE = os.path.expanduser("~/.config/fa-chess-clipbase.parquet")

class DBManager(QObject):
    # Señales para que la UI reaccione a cambios en los datos
    database_loaded = Signal(str)  # nombre de la base
    active_db_changed = Signal(str) # nombre de la base
    filter_updated = Signal(object) # DataFrame filtrado (Eager para la UI)

    def __init__(self):
        super().__init__()
        self.dbs = {} # Guardará LazyFrames
        self.db_metadata = {}
        self.active_db_name = "Clipbase"
        self.current_filter_query = None # Consulta Lazy completa
        self.current_filter_df = None    # Vista previa Eager para la UI
        
        # Versión del filtro para la caché
        self.filter_id = 0
        self.current_view_count = 0 # Caché del total de partidas filtradas
        
        # Caché de estadísticas: {(filter_id, pos_hash): stats_df}
        self.stats_cache = {} 
        self.MAX_CACHE_SIZE = 5000
        
        self.current_tree = None
        self.init_clipbase()

    def init_clipbase(self):
        schema = {
            "id": pl.Int64, "white": pl.String, "black": pl.String, 
            "w_elo": pl.Int64, "b_elo": pl.Int64, "result": pl.String, 
            "date": pl.String, "event": pl.String, "line": pl.String, 
            "full_line": pl.String, "fens": pl.List(pl.UInt64)
        }
        if os.path.exists(CLIPBASE_FILE):
            try:
                df = pl.read_parquet(CLIPBASE_FILE)
                self.dbs["Clipbase"] = df.lazy()
            except:
                self.dbs["Clipbase"] = pl.DataFrame(schema=schema).lazy()
        else:
            self.dbs["Clipbase"] = pl.DataFrame(schema=schema).lazy()
        self.db_metadata["Clipbase"] = {"read_only": False, "path": CLIPBASE_FILE}
        self.current_filter_query = None
        self.current_filter_df = None

    def save_clipbase(self):
        if "Clipbase" in self.dbs:
            self.dbs["Clipbase"].collect().write_parquet(CLIPBASE_FILE)

    def load_parquet(self, path):
        name = os.path.basename(path)
        self.dbs[name] = pl.scan_parquet(path)
        self.db_metadata[name] = {"read_only": True, "path": path}
        self.active_db_name = name
        self.current_filter_query = None
        self.current_filter_df = None
        self.load_tree(path)
        self.database_loaded.emit(name)
        self.active_db_changed.emit(name)
        return name

    def load_tree(self, db_path):
        self.current_tree = None
        self.stats_cache.clear()
        if not db_path: return
        tree_path = db_path.replace(".parquet", ".tree.parquet")
        if os.path.exists(tree_path):
            try: self.current_tree = pl.scan_parquet(tree_path)
            except: pass

    def get_stats_from_tree(self, pos_hash):
        if self.current_tree is None or self.current_filter_query is not None:
            return None
        try:
            target = int(pos_hash)
            res = self.current_tree.filter(pl.col("hash") == pl.lit(target, dtype=pl.UInt64)).collect()
            return res if res.height > 0 else None
        except: return None

    def get_current_view(self):
        if self.current_filter_query is not None: return self.current_filter_query
        return self.dbs.get(self.active_db_name)

    def set_active_db(self, name):
        if name in self.dbs:
            self.active_db_name = name
            self.current_filter_query = None
            self.current_filter_df = None
            self.filter_id += 1 # Nueva versión de filtro (reseteo)
            self.stats_cache.clear()
            self.current_view_count = self.get_active_count() # Cachear total base
            self.load_tree(self.db_metadata.get(name, {}).get("path"))
            self.active_db_changed.emit(name)
            self.filter_updated.emit(None)

    def filter_db(self, criteria):
        lazy_df = self.dbs.get(self.active_db_name)
        if lazy_df is None: return
        q = lazy_df
        if criteria.get("white"): q = q.filter(pl.col("white").str.contains(criteria["white"]))
        if criteria.get("black"): q = q.filter(pl.col("black").str.contains(criteria["black"]))
        if criteria.get("position_hash"):
            target = int(criteria["position_hash"])
            q = q.filter(pl.col("fens").list.contains(pl.lit(target, dtype=pl.UInt64)))
        min_elo = criteria.get("min_elo")
        if min_elo and str(min_elo).isdigit():
            m = int(min_elo); q = q.filter((pl.col("w_elo") >= m) | (pl.col("b_elo") >= m))
        result = criteria.get("result")
        if result and result != "Cualquiera": q = q.filter(pl.col("result") == result)
        
        self.current_filter_query = q
        self.current_filter_df = q.head(1000).collect()
        self.current_view_count = q.select(pl.len()).collect().item() # Calcular UNA VEZ
        
        self.filter_id += 1 # Incrementar versión para invalidar caché vieja
        self.stats_cache.clear()
        self.filter_updated.emit(self.current_filter_df)
        return self.current_filter_df

    def invert_filter(self):
        lazy_active = self.dbs.get(self.active_db_name)
        if lazy_active is None: return None
        if self.current_filter_query is None:
            self.current_filter_query = lazy_active.head(0)
            self.current_filter_df = self.current_filter_query.collect()
        else:
            current_ids = self.current_filter_query.select("id")
            self.current_filter_query = lazy_active.filter(~pl.col("id").is_in(current_ids))
            self.current_filter_df = self.current_filter_query.head(1000).collect()
        self.filter_id += 1
        self.stats_cache.clear()
        self.filter_updated.emit(self.current_filter_df)
        return self.current_filter_df

    def get_cached_stats(self, pos_hash):
        key = (self.filter_id, int(pos_hash))
        res = self.stats_cache.get(key)
        if res is not None:
            print(f"[CACHE] HIT: {key}")
        else:
            print(f"[CACHE] MISS: {key}")
        return res

    def cache_stats(self, pos_hash, stats_df):
        key = (self.filter_id, int(pos_hash))
        if len(self.stats_cache) >= self.MAX_CACHE_SIZE:
            self.stats_cache.pop(next(iter(self.stats_cache)))
        self.stats_cache[key] = stats_df
        print(f"[CACHE] Guardado: {key} (Total: {len(self.stats_cache)})")

    def get_active_df(self):
        lazy = self.dbs.get(self.active_db_name)
        return lazy.head(1000).collect() if lazy is not None else None
    
    def add_to_clipbase(self, game_data): 
        df = self.dbs["Clipbase"].collect()
        df = pl.concat([df, pl.DataFrame([game_data], schema=df.schema)])
        self.dbs["Clipbase"] = df.lazy()
        if self.active_db_name == "Clipbase": self.current_filter_query = None; self.current_filter_df = None
        
    def delete_game(self, db_name, game_id):
        if db_name in self.dbs: 
            self.dbs[db_name] = self.dbs[db_name].filter(pl.col("id") != game_id)
            if self.active_db_name == db_name: self.current_filter_query = None; self.current_filter_df = None
            return True
        return False
        
    def update_game(self, db_name, game_id, new_data):
        if db_name in self.dbs:
            self.dbs[db_name] = self.dbs[db_name].with_columns([pl.when(pl.col("id") == game_id).then(pl.lit(new_data[k])).otherwise(pl.col(k)).alias(k) for k in new_data.keys()])
            if self.active_db_name == db_name: self.current_filter_query = None; self.current_filter_df = None
            return True
        return False
        
    def get_game_by_id(self, db_name, game_id):
        lazy = self.dbs.get(db_name)
        if lazy is not None:
            res = lazy.filter(pl.col("id") == game_id).collect()
            if not res.is_empty(): return res.row(0, named=True)
        return None

    def get_active_count(self):
        lazy = self.dbs.get(self.active_db_name)
        return lazy.select(pl.len()).collect().item() if lazy is not None else 0

    def get_view_count(self):
        """Devuelve el conteo cacheado (Instantáneo)"""
        return self.current_view_count
