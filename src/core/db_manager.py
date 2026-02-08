import os
import json
import time
import polars as pl
from datetime import datetime
from src.config import CONFIG_FILE
from PySide6.QtCore import QObject, Signal

# Rutas del Proyecto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLIPBASE_FILE = os.path.expanduser("~/.config/fa-chess-clipbase.parquet")

GAME_SCHEMA = {
    "id": pl.Int64, 
    "white": pl.String, 
    "black": pl.String, 
    "w_elo": pl.Int64, 
    "b_elo": pl.Int64, 
    "result": pl.String, 
    "date": pl.String, 
    "event": pl.String, 
    "site": pl.String,
    "line": pl.String, 
    "full_line": pl.String, 
    "fens": pl.List(pl.UInt64)
}

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
        if os.path.exists(CLIPBASE_FILE):
            try:
                # Forzamos el cast al esquema oficial para evitar errores de tipo con archivos antiguos
                self.dbs["Clipbase"] = pl.scan_parquet(CLIPBASE_FILE).cast(GAME_SCHEMA)
            except Exception:
                self.dbs["Clipbase"] = pl.DataFrame(schema=GAME_SCHEMA).lazy()
        else:
            self.dbs["Clipbase"] = pl.DataFrame(schema=GAME_SCHEMA).lazy()
        
        self.db_metadata["Clipbase"] = {"read_only": False, "path": CLIPBASE_FILE}
        self.reset_to_full_base()

    def save_clipbase(self):
        if "Clipbase" in self.dbs:
            # Aseguramos el esquema antes de escribir
            self.dbs["Clipbase"].cast(GAME_SCHEMA).collect().write_parquet(CLIPBASE_FILE)

    def load_parquet(self, path):
        name = os.path.basename(path)
        # Aplicamos cast al cargar para normalizar bases de datos externas
        self.dbs[name] = pl.scan_parquet(path).cast(GAME_SCHEMA)
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
        if criteria.get("white"): 
            q = q.filter(pl.col("white").str.contains(criteria["white"]))
        if criteria.get("black"): 
            q = q.filter(pl.col("black").str.contains(criteria["black"]))
        
        if criteria.get("position_hash"):
            target = int(criteria["position_hash"])
            q = q.filter(pl.col("fens").list.contains(pl.lit(target, dtype=pl.UInt64)))
        
        min_elo = criteria.get("min_elo")
        if min_elo and str(min_elo).isdigit():
            m = int(min_elo)
            q = q.filter((pl.col("w_elo") >= m) | (pl.col("b_elo") >= m))
            
        result = criteria.get("result")
        if result and result != "Cualquiera": 
            q = q.filter(pl.col("result") == result)
        
        # Guardamos la consulta completa
        self.current_filter_query = q
        
        # Ejecutamos una única vez para obtener vista previa y conteo (usando sink si fuera posible, 
        # pero aquí collect es necesario para la UI)
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
            # Anti-join: registros en lazy_active que NO están en current_filter_query
            self.current_filter_query = lazy_active.join(
                self.current_filter_query.select("id"), on="id", how="anti"
            )
            
        # Actualizamos vista y conteo
        res = pl.collect_all([
            self.current_filter_query.head(1000),
            self.current_filter_query.select(pl.len())
        ])
        
        self.current_filter_df = res[0]
        self.current_view_count = res[1].item()
        
        self.filter_id += 1
        self.stats_cache.clear()
        self.filter_updated.emit(self.current_filter_df)
        return self.current_filter_df

    def sort_active_db(self, col_name, descending):
        """Ordenación Lazy sobre el conjunto completo de datos"""
        q = self.get_current_view()
        if q is None: return

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
            # Eliminar la entrada más antigua (FIFO)
            self.stats_cache.pop(next(iter(self.stats_cache)))
        self.stats_cache[key] = stats_df

    def get_active_df(self):
        lazy = self.dbs.get(self.active_db_name)
        return lazy.head(1000).collect() if lazy is not None else None
    
    def add_to_clipbase(self, game_data): 
        """Añade una partida a la base de datos de clips asegurando integridad del ID"""
        if "id" not in game_data or game_data["id"] is None:
            max_id = self.get_active_count()
            game_data["id"] = max_id + 1

        new_row = pl.DataFrame([game_data], schema=GAME_SCHEMA).lazy()
        self.dbs["Clipbase"] = pl.concat([self.dbs["Clipbase"], new_row])
        
        if self.active_db_name == "Clipbase": 
            self.reset_to_full_base()
        
    def delete_game(self, db_name, game_id):
        if db_name in self.dbs: 
            self.dbs[db_name] = self.dbs[db_name].filter(pl.col("id") != game_id)
            if self.active_db_name == db_name: 
                self.reset_to_full_base()
            return True
        return False
        
    def update_game(self, db_name, game_id, new_data):
        if db_name in self.dbs:
            self.dbs[db_name] = self.dbs[db_name].with_columns([
                pl.when(pl.col("id") == game_id).then(pl.lit(new_data[k])).otherwise(pl.col(k)).alias(k) 
                for k in new_data.keys() if k in GAME_SCHEMA
            ])
            if self.active_db_name == db_name: 
                self.reset_to_full_base()
            return True
        return False
        
    def get_game_by_id(self, db_name, game_id):
        lazy = self.dbs.get(db_name)
        if lazy is not None:
            res = lazy.filter(pl.col("id") == game_id).collect()
            if not res.is_empty(): 
                return res.row(0, named=True)
        return None

    def get_active_count(self):
        lazy = self.dbs.get(self.active_db_name)
        if lazy is None: return 0
        return lazy.select(pl.len()).collect().item()

    def get_view_count(self):
        return self.current_view_count

    def create_new_database(self, path):
        """Crea un archivo parquet vacío con el esquema de ajedrez estándar"""
        pl.DataFrame(schema=GAME_SCHEMA).write_parquet(path)
        return self.load_parquet(path)

    def get_player_report(self, player_name, eco_manager=None):
        """Genera un dossier detallado analizando el repertorio por nombres de apertura"""
        lazy_df = self.get_current_view()
        if lazy_df is None: return None

        p_df = lazy_df.filter(
            (pl.col("white") == player_name) | (pl.col("black") == player_name)
        ).collect()

        if p_df.is_empty(): return None

        # --- ASIGNAR NOMBRES DE APERTURA (Crucial para el resumen) ---
        if eco_manager:
            # Asignamos el nombre ECO y la profundidad a cada partida individual
            opening_data = [eco_manager.get_opening_name(line) for line in p_df["full_line"]]
            names = [d[0] for d in opening_data]
            depths = [d[1] for d in opening_data]
            p_df = p_df.with_columns([
                pl.Series("opening_name", names),
                pl.Series("theory_depth", depths)
            ])
        else:
            # Fallback si no hay ECO
            p_df = p_df.with_columns([
                pl.col("line").str.slice(0, 20).alias("opening_name"),
                pl.lit(0).alias("theory_depth")
            ])

        white_games = p_df.filter(pl.col("white") == player_name)
        black_games = p_df.filter(pl.col("black") == player_name)

        def get_detailed_stats(df, is_white):
            if df.is_empty(): return {"w": 0, "d": 0, "b": 0, "total": 0, "avg_opp_elo": 0, "perf": 0}
            w = (df["result"] == ("1-0" if is_white else "0-1")).sum()
            d = (df["result"] == "1/2-1/2").sum()
            b = (df["result"] == ("0-1" if is_white else "1-0")).sum()
            total = df.height
            opp_elo_col = "b_elo" if is_white else "w_elo"
            avg_opp_elo = df.select(pl.col(opp_elo_col)).filter(pl.col(opp_elo_col) > 0).mean().item() or 0
            score = (w + 0.5 * d) / total if total > 0 else 0.5
            perf = avg_opp_elo + (score - 0.5) * 800
            return {"w": int(w), "d": int(d), "b": int(b), "total": total, "avg_opp_elo": int(avg_opp_elo), "perf": int(perf)}

        def get_repertoire(df, is_white):
            if df.is_empty(): return []
            
            return (df.group_by("opening_name")
                    .agg([
                        pl.len().alias("count"),
                        ((pl.col("result") == ("1-0" if is_white else "0-1")).sum() + 
                         (pl.col("result") == "1/2-1/2").sum() * 0.5).alias("points"),
                        pl.col("theory_depth").mean().alias("avg_depth"),
                        pl.col("full_line").first().alias("sample_line")
                    ])
                    .with_columns((pl.col("points") / pl.col("count") * 100).alias("win_rate"))
                    .sort("count", descending=True)
                    .head(15)
                    .to_dicts())

        stats = {
            "name": player_name,
            "as_white": get_detailed_stats(white_games, True),
            "as_black": get_detailed_stats(black_games, False),
            "top_white": get_repertoire(white_games, True),
            "top_black": get_repertoire(black_games, False),
            "elo_history": (p_df.select(["date", pl.when(pl.col("white") == player_name).then(pl.col("w_elo")).otherwise(pl.col("b_elo")).alias("elo")])
                           .filter(pl.col("elo") > 0).sort("date").to_dicts()),
        }

        # 5. MEJORES VICTORIAS (Por ELO del oponente)
        wins = p_df.filter(
            ((pl.col("white") == player_name) & (pl.col("result") == "1-0")) |
            ((pl.col("black") == player_name) & (pl.col("result") == "0-1"))
        )
        stats["best_wins"] = (wins.with_columns(
            pl.when(pl.col("white") == player_name).then(pl.col("b_elo")).otherwise(pl.col("w_elo")).alias("opp_elo"),
            pl.when(pl.col("white") == player_name).then(pl.col("black")).otherwise(pl.col("white")).alias("opp_name")
        ).sort("opp_elo", descending=True).head(5).to_dicts())

        # 6. PEORES DERROTAS (Derrotas contra ELO más bajo)
        losses = p_df.filter(
            ((pl.col("white") == player_name) & (pl.col("result") == "0-1")) |
            ((pl.col("black") == player_name) & (pl.col("result") == "1-0"))
        )
        stats["worst_losses"] = (losses.with_columns(
            pl.when(pl.col("white") == player_name).then(pl.col("b_elo")).otherwise(pl.col("w_elo")).alias("opp_elo"),
            pl.when(pl.col("white") == player_name).then(pl.col("black")).otherwise(pl.col("white")).alias("opp_name")
        ).filter(pl.col("opp_elo") > 0).sort("opp_elo", descending=False).head(5).to_dicts())

        stats["max_elo"] = max([x["elo"] for x in stats["elo_history"]] or [0])
        return stats

    def delete_database_from_disk(self, name):
        """Elimina físicamente el archivo de base de datos y lo quita de la memoria"""
        if name in self.db_metadata:
            path = self.db_metadata[name]["path"]
            try:
                if os.path.exists(path): os.remove(path)
                del self.dbs[name]
                del self.db_metadata[name]
                if self.active_db_name == name: self.set_active_db("Clipbase")
                return True
            except: return False
        return False
