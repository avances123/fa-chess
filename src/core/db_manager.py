import os
import json
import time
import polars as pl
from datetime import datetime
from config import CONFIG_FILE

class DBManager:
    def __init__(self):
        self.dbs = {}
        self.db_metadata = {}
        self.active_db_name = "Clipbase"
        self.init_clipbase()

    def init_clipbase(self):
        self.dbs["Clipbase"] = pl.DataFrame(schema={
            "id": pl.Int64, 
            "white": pl.String, 
            "black": pl.String, 
            "w_elo": pl.Int64, 
            "b_elo": pl.Int64, 
            "result": pl.String, 
            "date": pl.String, 
            "event": pl.String, 
            "line": pl.String, 
            "full_line": pl.String
        })
        self.db_metadata["Clipbase"] = {"read_only": False, "path": None}

    def load_parquet(self, path):
        name = os.path.basename(path)
        self.dbs[name] = pl.read_parquet(path)
        # Por defecto, bases cargadas son de solo lectura
        self.db_metadata[name] = {"read_only": True, "path": path}
        self.active_db_name = name
        return name

    def remove_database(self, name):
        if name == "Clipbase":
            return False
        if name in self.dbs:
            del self.dbs[name]
        if name in self.db_metadata:
            del self.db_metadata[name]
        if self.active_db_name == name:
            self.active_db_name = "Clipbase"
        return True

    def get_active_df(self):
        return self.dbs.get(self.active_db_name)

    def filter_db(self, name, criteria):
        df = self.dbs.get(name)
        if df is None:
            return None
        
        q = df.lazy()
        if criteria.get("white"):
            q = q.filter(pl.col("white").str.contains(criteria["white"]))
        if criteria.get("black"):
            q = q.filter(pl.col("black").str.contains(criteria["black"]))
        
        # Búsqueda por posición (Feature 4)
        if criteria.get("position_epd"):
            if "fens" in df.columns:
                # Buscamos el EPD actual dentro de la lista de FENs de la partida
                q = q.filter(pl.col("fens").str.contains(criteria["position_epd"]))
            else:
                # Si la base es antigua y no tiene FENs, no podemos filtrar por posición
                # Podríamos lanzar una excepción capturable o simplemente ignorar el filtro
                pass

        min_elo = criteria.get("min_elo")
        if min_elo and str(min_elo).isdigit():
            m = int(min_elo)
            q = q.filter((pl.col("w_elo") >= m) | (pl.col("b_elo") >= m))
            
        result = criteria.get("result")
        if result and result != "Cualquiera":
            q = q.filter(pl.col("result") == result)
            
        return q.collect()

    def add_to_clipbase(self, game_data):
        self.dbs["Clipbase"] = pl.concat([self.dbs["Clipbase"], pl.DataFrame([game_data])])

    def delete_game(self, db_name, game_id):
        if db_name in self.dbs:
            self.dbs[db_name] = self.dbs[db_name].filter(pl.col("id") != game_id)
            return True
        return False

    def update_game(self, db_name, game_id, new_data):
        if db_name in self.dbs:
            self.dbs[db_name] = self.dbs[db_name].with_columns([
                pl.when(pl.col("id") == game_id).then(pl.lit(new_data[k])).otherwise(pl.col(k)).alias(k)
                for k in new_data.keys()
            ])
            return True
        return False

    def get_game_by_id(self, db_name, game_id):
        df = self.dbs.get(db_name)
        if df is not None:
            res = df.filter(pl.col("id") == game_id)
            if not res.is_empty():
                return res.row(0, named=True)
        return None

    def get_stats_for_position(self, line_uci, is_white):
        df = self.get_active_df()
        if df is None:
            return None

        try:
            res = df.lazy().filter(pl.col("line").str.starts_with(line_uci)).select([
                pl.col("line").str.slice(len(line_uci)).str.strip_chars().str.split(" ").list.get(0).alias("uci"),
                pl.col("result"),
                pl.col("w_elo"),
                pl.col("b_elo")
            ]).filter(pl.col("uci").is_not_null() & (pl.col("uci") != "")).group_by("uci").agg([
                pl.len().alias("c"),
                pl.col("result").filter(pl.col("result") == "1-0").count().alias("w"),
                pl.col("result").filter(pl.col("result") == "0-1").count().alias("b"),
                pl.col("result").filter(pl.col("result") == "1/2-1/2").count().alias("d"),
                pl.col("w_elo").mean().alias("avg_w_elo"),
                pl.col("b_elo").mean().alias("avg_b_elo")
            ]).sort("c", descending=True).limit(15).collect()
            return res
        except:
            return None
