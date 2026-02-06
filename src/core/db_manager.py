import os
import json
import time
import polars as pl
from datetime import datetime
from config import CONFIG_FILE
from PySide6.QtCore import QObject, Signal

class DBManager(QObject):
    # Señales para que la UI reaccione a cambios en los datos
    database_loaded = Signal(str)  # nombre de la base
    active_db_changed = Signal(str) # nombre de la base
    filter_updated = Signal(object) # DataFrame filtrado
    stats_ready = Signal(object)    # DataFrame de estadísticas

    def __init__(self):
        super().__init__()
        self.dbs = {}
        self.db_metadata = {}
        self.active_db_name = "Clipbase"
        self.current_filter_df = None # Estado del filtro actual
        self.init_clipbase()

    def init_clipbase(self):
        self.dbs["Clipbase"] = pl.DataFrame(schema={
            "id": pl.Int64, "white": pl.String, "black": pl.String, 
            "w_elo": pl.Int64, "b_elo": pl.Int64, "result": pl.String, 
            "date": pl.String, "event": pl.String, "line": pl.String, 
            "full_line": pl.String, "fens": pl.List(pl.UInt64)
        })
        self.db_metadata["Clipbase"] = {"read_only": False, "path": None}
        self.current_filter_df = None

    def load_parquet(self, path):
        name = os.path.basename(path)
        self.dbs[name] = pl.read_parquet(path)
        self.db_metadata[name] = {"read_only": True, "path": path}
        self.active_db_name = name
        self.current_filter_df = None # Reset filtro al cargar base
        self.database_loaded.emit(name)
        self.active_db_changed.emit(name)
        return name

    def set_active_db(self, name):
        if name in self.dbs:
            self.active_db_name = name
            self.current_filter_df = None # Reset filtro al cambiar base
            self.active_db_changed.emit(name)

    def remove_database(self, name):
        if name == "Clipbase": return False
        if name in self.dbs: del self.dbs[name]
        if name in self.db_metadata: del self.db_metadata[name]
        if self.active_db_name == name:
            self.active_db_name = "Clipbase"
            self.current_filter_df = None
            self.active_db_changed.emit("Clipbase")
        return True

    def filter_db(self, criteria):
        df = self.dbs.get(self.active_db_name)
        if df is None: return
        
        q = df.lazy()
        if criteria.get("white"): q = q.filter(pl.col("white").str.contains(criteria["white"]))
        if criteria.get("black"): q = q.filter(pl.col("black").str.contains(criteria["black"]))
        
        if criteria.get("position_hash"):
            if "fens" in df.columns:
                q = q.filter(pl.col("fens").list.contains(criteria["position_hash"]))

        min_elo = criteria.get("min_elo")
        if min_elo and str(min_elo).isdigit():
            m = int(min_elo)
            q = q.filter((pl.col("w_elo") >= m) | (pl.col("b_elo") >= m))
            
        result = criteria.get("result")
        if result and result != "Cualquiera":
            q = q.filter(pl.col("result") == result)
            
        self.current_filter_df = q.collect()
        self.filter_updated.emit(self.current_filter_df)
        return self.current_filter_df

    def get_stats_for_position(self, line_uci, is_white):
        # USAR FILTRO SI EXISTE, SI NO TODA LA BASE
        df = self.current_filter_df if self.current_filter_df is not None else self.get_active_df()
        
        if df is None: return None
        try:
            res = df.lazy().filter(pl.col("line").str.starts_with(line_uci)).select([
                pl.col("line").str.slice(len(line_uci)).str.strip_chars().str.split(" ").list.get(0).alias("uci"),
                pl.col("result"), pl.col("w_elo"), pl.col("b_elo")
            ]).filter(pl.col("uci").is_not_null() & (pl.col("uci") != "")).group_by("uci").agg([
                pl.len().alias("c"),
                pl.col("result").filter(pl.col("result") == "1-0").count().alias("w"),
                pl.col("result").filter(pl.col("result") == "0-1").count().alias("b"),
                pl.col("result").filter(pl.col("result") == "1/2-1/2").count().alias("d"),
                pl.col("w_elo").mean().alias("avg_w_elo"),
                pl.col("b_elo").mean().alias("avg_b_elo")
            ]).sort("c", descending=True).limit(15).collect()
            return res
        except: return None

    def get_active_df(self): return self.dbs.get(self.active_db_name)
    def add_to_clipbase(self, game_data): 
        self.dbs["Clipbase"] = pl.concat([self.dbs["Clipbase"], pl.DataFrame([game_data])])
        if self.active_db_name == "Clipbase": self.current_filter_df = None
    def delete_game(self, db_name, game_id):
        if db_name in self.dbs: 
            self.dbs[db_name] = self.dbs[db_name].filter(pl.col("id") != game_id)
            if self.active_db_name == db_name: self.current_filter_df = None
            return True
        return False
    def update_game(self, db_name, game_id, new_data):
        if db_name in self.dbs:
            self.dbs[db_name] = self.dbs[db_name].with_columns([pl.when(pl.col("id") == game_id).then(pl.lit(new_data[k])).otherwise(pl.col(k)).alias(k) for k in new_data.keys()])
            if self.active_db_name == db_name: self.current_filter_df = None
            return True
        return False
    def get_game_by_id(self, db_name, game_id):
        df = self.dbs.get(db_name)
        if df is not None:
            res = df.filter(pl.col("id") == game_id)
            if not res.is_empty(): return res.row(0, named=True)
        return None