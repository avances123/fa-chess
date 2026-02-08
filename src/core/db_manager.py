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
    database_loaded = Signal(str)
    active_db_changed = Signal(str)
    filter_updated = Signal(object)

    def __init__(self):
        super().__init__()
        self.dbs = {} # LazyFrames
        self.db_metadata = {}
        self.active_db_name = "Clipbase"
        self.current_filter_query = None # Consulta Lazy completa
        self.current_filter_df = None    # Vista previa Eager
        
        self.filter_id = 0
        self.current_view_count = 0 
        self.stats_cache = {} 
        self.MAX_CACHE_SIZE = 5000
        
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
        self.reset_to_full_base()
        self.database_loaded.emit(name)
        self.active_db_changed.emit(name)
        return name

    def reset_to_full_base(self):
        """Resetea el estado para mostrar la base completa (Lazy)"""
        self.current_filter_query = None
        self.current_filter_df = None
        self.current_view_count = self.get_active_count()
        self.filter_id += 1
        self.stats_cache.clear()

    def set_active_db(self, name):
        if name in self.dbs:
            self.active_db_name = name
            self.reset_to_full_base()
            self.active_db_changed.emit(name)
            self.filter_updated.emit(None)

    def get_current_view(self):
        if self.current_filter_query is not None: return self.current_filter_query
        return self.dbs.get(self.active_db_name)

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
        self.current_view_count = q.select(pl.len()).collect().item()
        
        self.filter_id += 1 
        self.stats_cache.clear()
        self.filter_updated.emit(self.current_filter_df)
        return self.current_filter_df

    def invert_filter(self):
        """Inversión de filtro optimizada mediante Anti-Join"""
        lazy_active = self.dbs.get(self.active_db_name)
        if lazy_active is None: return None
        
        if self.current_filter_query is None:
            self.current_filter_query = lazy_active.head(0)
        else:
            # OPTIMIZACIÓN: Anti-join es más rápido que is_in para millones de registros
            self.current_filter_query = lazy_active.join(
                self.current_filter_query.select("id"), on="id", how="anti"
            )
            
        self.current_filter_df = self.current_filter_query.head(1000).collect()
        self.current_view_count = self.current_filter_query.select(pl.len()).collect().item()
        self.filter_id += 1
        self.stats_cache.clear()
        self.filter_updated.emit(self.current_filter_df)
        return self.current_filter_df

    def sort_active_db(self, col_name, descending):
        """Ordenación Lazy sobre el conjunto completo de datos"""
        q = self.get_current_view()
        if q is not None:
            q = q.sort(col_name, descending=descending)
            if self.current_filter_query is not None:
                self.current_filter_query = q
            else:
                self.dbs[self.active_db_name] = q
            
            self.current_filter_df = q.head(1000).collect()
            self.filter_updated.emit(self.current_filter_df)

    def get_cached_stats(self, pos_hash):
        return self.stats_cache.get((self.filter_id, int(pos_hash)))

    def cache_stats(self, pos_hash, stats_df):
        key = (self.filter_id, int(pos_hash))
        if len(self.stats_cache) >= self.MAX_CACHE_SIZE:
            self.stats_cache.pop(next(iter(self.stats_cache)))
        self.stats_cache[key] = stats_df

    def get_active_df(self):
        lazy = self.dbs.get(self.active_db_name)
        return lazy.head(1000).collect() if lazy is not None else None
    
    def add_to_clipbase(self, game_data): 
        # MEJORA: Concatenación Lazy pura
        new_row = pl.DataFrame([game_data], schema=self.dbs["Clipbase"].schema).lazy()
        self.dbs["Clipbase"] = pl.concat([self.dbs["Clipbase"], new_row])
        if self.active_db_name == "Clipbase": self.reset_to_full_base()
        
    def delete_game(self, db_name, game_id):
        if db_name in self.dbs: 
            self.dbs[db_name] = self.dbs[db_name].filter(pl.col("id") != game_id)
            if self.active_db_name == db_name: self.reset_to_full_base()
            return True
        return False
        
    def update_game(self, db_name, game_id, new_data):
        if db_name in self.dbs:
            self.dbs[db_name] = self.dbs[db_name].with_columns([
                pl.when(pl.col("id") == game_id).then(pl.lit(new_data[k])).otherwise(pl.col(k)).alias(k) 
                for k in new_data.keys()
            ])
            if self.active_db_name == db_name: self.reset_to_full_base()
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
        return self.current_view_count
