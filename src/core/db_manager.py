import os
import json
import time
import os
import chess
import chess.pgn
import chess.polyglot
import polars as pl
from datetime import datetime
from src.config import logger, GAME_SCHEMA
from PySide6.QtCore import QObject, Signal

class DBManager(QObject):
    database_loaded = Signal(str)
    active_db_changed = Signal(str)
    filter_updated = Signal(object)

    def __init__(self):
        super().__init__()
        self.dbs = {} 
        self.db_metadata = {}
        self.active_db_name = None
        self.current_filter_query = None
        self.current_filter_df = None
        self.filter_id = 0
        self.current_view_count = 0 
        self.stats_cache = {} 
        self.MAX_CACHE_SIZE = 5000
        self.reference_db_name = None
        logger.info("DBManager: Inicializando sistema de bases de datos Parquet")

    def save_active_db(self):
        name = self.active_db_name
        if not name or name not in self.dbs: return False
        
        path = self.db_metadata[name]["path"]
        if path:
            temp_path = path + ".tmp_save"
            try:
                self.dbs[name].collect().write_parquet(temp_path)
                if os.path.exists(path): os.remove(path)
                os.rename(temp_path, path)
                self.set_dirty(name, False)
                self.reload_db(name)
                return True
            except Exception as e:
                logger.error(f"DBManager: Error al persistir: {e}")
                if os.path.exists(temp_path): os.remove(temp_path)
                raise e
        return False

    def set_readonly(self, name, status):
        if name in self.db_metadata:
            self.db_metadata[name]["read_only"] = status
            return True
        return False

    def delete_db_from_disk(self, name):
        """Elimina físicamente el archivo de la base de datos."""
        if name not in self.db_metadata:
            return False, "Base de datos no encontrada"
        
        path = self.db_metadata[name]["path"]
        try:
            if name in self.dbs:
                del self.dbs[name]
            if name in self.db_metadata:
                del self.db_metadata[name]
            
            if os.path.exists(path):
                os.remove(path)
                logger.info(f"DBManager: Archivo eliminado: {path}")
                return True, "Archivo eliminado del disco"
            return False, "El archivo no existe en la ruta especificada"
        except Exception as e:
            logger.error(f"DBManager: Error al borrar archivo {path}: {e}")
            return False, str(e)

    def set_dirty(self, name, status=True):
        if name in self.db_metadata:
            self.db_metadata[name]["dirty"] = status
            return True
        return False

    def is_dirty(self, name):
        return self.db_metadata.get(name, {}).get("dirty", False)

    def load_parquet(self, path):
        name = os.path.basename(path)
        try:
            test_scan = pl.scan_parquet(path)
            test_scan.head(1).collect() 
            self.dbs[name] = test_scan.cast(GAME_SCHEMA)
            self.db_metadata[name] = {"read_only": True, "path": path, "dirty": False}
            self.set_active_db(name)
            self.database_loaded.emit(name)
            return name
        except Exception as e:
            logger.error(f"DBManager: Error cargando Parquet {path}: {e}")
            return None

    def reload_db(self, name):
        if name in self.db_metadata:
            path = self.db_metadata[name]["path"]
            if path and os.path.exists(path):
                if name in self.dbs: del self.dbs[name]
                self.dbs[name] = pl.scan_parquet(path).cast(GAME_SCHEMA)
                if self.active_db_name == name:
                    self.reset_to_full_base()
                    self.filter_updated.emit(None)
                return True
        return False

    def reset_to_full_base(self):
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

    def close(self):
        """Libera recursos y detiene workers."""
        self.dbs = {}
        self.current_filter_df = None
        self.stats_cache = {}

    def save_game(self, game: chess.pgn.Game):
        """Añade una partida a la base de datos activa."""
        if not self.active_db_name:
            return False, "No hay base de datos activa"

        try:
            # 1. Extraer datos para el esquema
            full_line = []
            fens = []
            board = game.board()
            fens.append(chess.polyglot.zobrist_hash(board))
            
            node = game
            while node.variations:
                next_node = node.variation(0)
                move = next_node.move
                full_line.append(move.uci())
                board.push(move)
                fens.append(chess.polyglot.zobrist_hash(board))
                node = next_node

            new_data = {
                "id": [int(time.time() * 1000)], # ID temporal basado en timestamp
                "date": [game.headers.get("Date", "????.??.??")],
                "event": [game.headers.get("Event", "?")],
                "site": [game.headers.get("Site", "?")],
                "white": [game.headers.get("White", "Unknown")],
                "w_elo": [int(game.headers.get("WhiteElo", 0)) if game.headers.get("WhiteElo", "0").isdigit() else 0],
                "black": [game.headers.get("Black", "Unknown")],
                "b_elo": [int(game.headers.get("BlackElo", 0)) if game.headers.get("BlackElo", "0").isdigit() else 0],
                "result": [game.headers.get("Result", "*")],
                "line": [game.headers.get("ECO", "")], # Podríamos usar ECOManager aquí después
                "full_line": [" ".join(full_line)],
                "fens": [fens]
            }

            new_df = pl.DataFrame(new_data, schema=GAME_SCHEMA).lazy()
            
            # 2. Concatenar a la base actual
            name = self.active_db_name
            self.dbs[name] = pl.concat([self.dbs[name], new_df])
            self.set_dirty(name, True)
            
            # 3. Notificar cambios
            self.reset_to_full_base()
            self.filter_updated.emit(None)
            return True, "Partida guardada correctamente"
        except Exception as e:
            logger.error(f"DBManager: Error al guardar partida: {e}")
            return False, str(e)

    def filter_by_position(self, pos_hash):
        """Filtra la base de datos por una posición específica."""
        criteria = {
            "white": "", "black": "", "min_elo": "", 
            "result": "Cualquiera", "position_hash": pos_hash, 
            "use_position": True
        }
        return self.filter_db(criteria)

    def get_reference_path(self):
        target = self.reference_db_name if self.reference_db_name else self.active_db_name
        return self.db_metadata.get(target, {}).get("path") if target else None

    def set_reference_db(self, name):
        self.reference_db_name = name if name != "Base Activa" else None
        self.stats_cache.clear()
        return True

    def get_reference_view(self):
        target = self.reference_db_name if self.reference_db_name else self.active_db_name
        if not target: return None
        if target == self.active_db_name and self.current_filter_query is not None:
            return self.current_filter_query
        return self.dbs.get(target)

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
        
        date_from = criteria.get("date_from")
        if date_from: q = q.filter(pl.col("date") >= date_from)
        date_to = criteria.get("date_to")
        if date_to: q = q.filter(pl.col("date") <= date_to)
        
        result = criteria.get("result")
        if result and result != "Cualquiera": q = q.filter(pl.col("result") == result)
        
        self.current_filter_query = q
        self.current_filter_df = q.head(1000).collect()
        self.current_view_count = q.select(pl.len()).collect().item()
        self.filter_id += 1 
        self.stats_cache.clear()
        self.filter_updated.emit(self.current_filter_df)
        return self.current_filter_df

    def sort_active_db(self, col_name, descending):
        q = self.get_current_view()
        if q is None: return
        try:
            q = q.sort(col_name, descending=descending)
            if self.current_filter_query is not None: self.current_filter_query = q
            else: self.dbs[self.active_db_name] = q
            self.current_filter_df = q.head(1000).collect(engine="streaming")
            self.filter_updated.emit(self.current_filter_df)
        except Exception as e:
            logger.error(f"DBManager: Error al ordenar: {e}")

    def get_cached_stats(self, pos_hash):
        res = self.stats_cache.get((self.filter_id, int(pos_hash)))
        return res if res else (None, None)

    def cache_stats(self, pos_hash, stats_df, engine_eval):
        key = (self.filter_id, int(pos_hash))
        if len(self.stats_cache) >= self.MAX_CACHE_SIZE: 
            self.stats_cache.pop(next(iter(self.stats_cache)))
        self.stats_cache[key] = (stats_df, engine_eval)

    def get_active_df(self):
        lazy = self.dbs.get(self.active_db_name)
        return lazy.head(1000).collect() if lazy is not None else None
    
    def add_game(self, db_name, game_data):
        if db_name in self.dbs:
            if "id" not in game_data or game_data["id"] is None:
                game_data["id"] = int(time.time() * 1000)
            new_row = pl.DataFrame([game_data], schema=GAME_SCHEMA).lazy()
            self.dbs[db_name] = pl.concat([self.dbs[db_name], new_row])
            self.set_dirty(db_name, True)
            if self.active_db_name == db_name: self.reset_to_full_base()
            return True
        return False

    def delete_filtered_games(self):
        name = self.active_db_name
        if self.current_filter_query is None or name not in self.dbs: return False
        self.dbs[name] = self.dbs[name].join(self.current_filter_query.select("id"), on="id", how="anti")
        self.set_dirty(name)
        self.reset_to_full_base()
        self.filter_updated.emit(None)
        return True
        
    def delete_game(self, db_name, game_id):
        if db_name in self.dbs: 
            self.dbs[db_name] = self.dbs[db_name].filter(pl.col("id") != game_id)
            self.set_dirty(db_name)
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

    def create_new_database(self, path):
        pl.DataFrame(schema=GAME_SCHEMA).write_parquet(path)
        name = self.load_parquet(path)
        if name: self.set_readonly(name, False)
        return name

    def get_player_report(self, player_name, eco_manager=None):
        lazy_df = self.get_current_view()
        if lazy_df is None: return None
        p_df = lazy_df.filter((pl.col("white") == player_name) | (pl.col("black") == player_name)).collect()
        if p_df.is_empty(): return None
        
        # Generar nombres de apertura para el reporte
        if eco_manager:
            opening_data = [eco_manager.get_opening_name(line) for line in p_df["full_line"]]
            p_df = p_df.with_columns([
                pl.Series("opening_name", [d[0] for d in opening_data]), 
                pl.Series("theory_depth", [d[1] for d in opening_data])
            ])
        else:
            # Fallback si no hay ECO manager: usar los primeros movimientos
            p_df = p_df.with_columns([
                pl.col("line").alias("opening_name"),
                pl.lit(0).alias("theory_depth")
            ])
        
        white_games = p_df.filter(pl.col("white") == player_name)
        black_games = p_df.filter(pl.col("black") == player_name)
        def get_detailed_stats(df, is_white):
            if df.is_empty(): return {"w": 0, "d": 0, "b": 0, "total": 0, "avg_opp_elo": 0, "perf": 0}
            w = (df["result"] == ("1-0" if is_white else "0-1")).sum(); d = (df["result"] == "1/2-1/2").sum(); b = (df["result"] == ("0-1" if is_white else "1-0")).sum(); total = df.height; opp_elo_col = "b_elo" if is_white else "w_elo"; avg_opp_elo = df.select(pl.col(opp_elo_col)).filter(pl.col(opp_elo_col) > 0).mean().item() or 0; score = (w + 0.5 * d) / total if total > 0 else 0.5; perf = avg_opp_elo + (score - 0.5) * 800; return {"w": int(w), "d": int(d), "b": int(b), "total": total, "avg_opp_elo": int(avg_opp_elo), "perf": int(perf)}
        def get_repertoire(df, is_white):
            if df.is_empty(): return []; 
            return (df.group_by("opening_name").agg([pl.len().alias("count"), ((pl.col("result") == ("1-0" if is_white else "0-1")).sum() + (pl.col("result") == "1/2-1/2").sum() * 0.5).alias("points"), pl.col("theory_depth").mean().alias("avg_depth"), pl.col("full_line").first().alias("sample_line")]).with_columns((pl.col("points") / pl.col("count") * 100).alias("win_rate")).sort("count", descending=True).head(15).to_dicts())
        stats = {"name": player_name, "as_white": get_detailed_stats(white_games, True), "as_black": get_detailed_stats(black_games, False), "top_white": get_repertoire(white_games, True), "top_black": get_repertoire(black_games, False), "elo_history": (p_df.select(["date", pl.when(pl.col("white") == player_name).then(pl.col("w_elo")).otherwise(pl.col("b_elo")).alias("elo")]).filter(pl.col("elo") > 0).sort("date").to_dicts())}
        wins = p_df.filter(((pl.col("white") == player_name) & (pl.col("result") == "1-0")) | ((pl.col("black") == player_name) & (pl.col("result") == "0-1")))
        stats["best_wins"] = (wins.with_columns(pl.when(pl.col("white") == player_name).then(pl.col("b_elo")).otherwise(pl.col("w_elo")).alias("opp_elo"), pl.when(pl.col("white") == player_name).then(pl.col("black")).otherwise(pl.col("white")).alias("opp_name")).sort("opp_elo", descending=True).head(5).to_dicts())
        losses = p_df.filter(((pl.col("white") == player_name) & (pl.col("result") == "0-1")) | ((pl.col("black") == player_name) & (pl.col("result") == "1-0")))
        stats["worst_losses"] = (losses.with_columns(pl.when(pl.col("white") == player_name).then(pl.col("b_elo")).otherwise(pl.col("w_elo")).alias("opp_elo"), pl.when(pl.col("white") == player_name).then(pl.col("black")).otherwise(pl.col("white")).alias("opp_name")).filter(pl.col("opp_elo") > 0).sort("opp_elo", descending=False).head(5).to_dicts())
        stats["max_elo"] = max([x["elo"] for x in stats["elo_history"]] or [0])
        return stats

    def delete_database_from_disk(self, name):
        if name in self.db_metadata:
            path = self.db_metadata[name]["path"]
            try:
                if os.path.exists(path): os.remove(path)
                del self.dbs[name]; del self.db_metadata[name]
                if self.active_db_name == name: 
                    self.active_db_name = list(self.dbs.keys())[0] if self.dbs else None
                return True
            except: return False
        return False
