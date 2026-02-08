import os
import chess
import chess.pgn
import chess.polyglot
import polars as pl
from PySide6.QtCore import QThread, Signal, QObject
from src.converter import extract_game_data, convert_pgn_to_parquet

class PGNWorker(QThread):
    progress = Signal(int)
    finished = Signal(str)
    status = Signal(str)

    def __init__(self, path):
        super().__init__()
        self.path = path

    def run(self):
        try:
            out = self.path.replace(".pgn", ".parquet")
            convert_pgn_to_parquet(self.path, out, progress_callback=self.progress.emit)
            self.finished.emit(out)
        except Exception as e:
            self.status.emit(f"Error: {e}")
            self.finished.emit("")

class PGNAppendWorker(QThread):
    progress = Signal(int)
    finished = Signal() 
    status = Signal(str)

    def __init__(self, pgn_path, target_parquet_path):
        super().__init__()
        self.pgn_path = pgn_path
        self.target_path = target_parquet_path

    def run(self):
        import tempfile
        import os
        temp_parquet = None
        try:
            # 1. Convertir PGN nuevo a un Parquet temporal
            self.status.emit("Convirtiendo PGN nuevo...")
            temp_dir = tempfile.gettempdir()
            temp_parquet = os.path.join(temp_dir, f"new_games_{os.getpid()}.parquet")
            
            from src.converter import convert_pgn_to_parquet
            convert_pgn_to_parquet(self.pgn_path, temp_parquet, progress_callback=self.progress.emit)

            # 2. Fusionar con la base existente
            self.status.emit("Preparando fusión de bases...")
            
            from src.core.db_manager import GAME_SCHEMA
            
            # Escaneo con esquema estricto
            old_lf = pl.scan_parquet(self.target_path).cast(GAME_SCHEMA)
            new_lf = pl.scan_parquet(temp_parquet).cast(GAME_SCHEMA)
            
            # Ajuste de IDs
            max_id_res = old_lf.select(pl.col("id").max()).collect()
            max_id = max_id_res.item() if not max_id_res.is_empty() and max_id_res.item() is not None else 0
            new_lf = new_lf.with_columns(pl.col("id") + max_id + 1)

            # CONCATENACIÓN DIRECTA CON STREAMING SEGURO
            final_tmp = self.target_path + ".tmp"
            self.status.emit("Escribiendo base de datos unificada (Streaming)...")
            
            # Forzamos collect(streaming=True) para que la escritura sea por bloques
            pl.concat([old_lf, new_lf]).collect(streaming=True).write_parquet(final_tmp)

            # Reemplazo seguro
            if os.path.exists(self.target_path):
                os.remove(self.target_path)
            os.rename(final_tmp, self.target_path)
            
            if temp_parquet and os.path.exists(temp_parquet): 
                os.remove(temp_parquet)

            self.status.emit("Fusión completada con éxito.")
            self.finished.emit()
            
        except Exception as e:
            print(f"DEBUG: Error en PGNAppendWorker: {e}")
            self.status.emit(f"Error en fusión: {e}")
            if temp_parquet and os.path.exists(temp_parquet):
                try: os.remove(temp_parquet)
                except: pass
            self.finished.emit()

class StatsWorker(QThread):
    finished = Signal(object)

    def __init__(self, db, current_line_uci, is_white_turn, current_hash=None):
        super().__init__()
        self.db = db
        self.current_line = current_line_uci
        self.current_hash = current_hash
        self.is_white = is_white_turn

    def run(self):
        try:
            if not self.current_hash:
                self.finished.emit(None)
                return

            cached = self.db.get_cached_stats(self.current_hash)
            if cached is not None:
                self.finished.emit(cached)
                return

            lazy_view = self.db.get_current_view()
            if lazy_view is None:
                self.finished.emit(None)
                return

            target = int(self.current_hash)
            
            q = (
                lazy_view
                .filter(pl.col("fens").list.contains(pl.lit(target, dtype=pl.UInt64)))
                .with_columns([
                    pl.col("full_line").str.split(" ").alias("_m"),
                    pl.col("fens").list.eval(pl.element() == target).list.arg_max().alias("_i")
                ])
                .filter(pl.col("_i") < pl.col("_m").list.len())
                .with_columns(
                    pl.col("_m").list.get(pl.col("_i")).alias("uci")
                )
                .filter(
                    (pl.col("uci").str.len_chars() >= 4) & 
                    (pl.col("uci").str.len_chars() <= 5)
                )
                .group_by("uci")
                .agg([
                    pl.len().alias("c"),
                    (pl.col("result") == "1-0").sum().alias("w"),
                    (pl.col("result") == "1/2-1/2").sum().alias("d"),
                    (pl.col("result") == "0-1").sum().alias("b"),
                    pl.col("w_elo").mean().fill_null(0).alias("avg_w_elo"),
                    pl.col("b_elo").mean().fill_null(0).alias("avg_b_elo")
                ])
                .sort("c", descending=True)
            )
            
            stats = q.collect(streaming=True)
            stats = stats.with_columns(pl.lit(False).alias("_is_partial"))
            
            self.db.cache_stats(self.current_hash, stats)
            self.finished.emit(stats)

        except Exception as e:
            print(f"Error en StatsWorker: {e}")
            self.finished.emit(None)

class PGNExportWorker(QThread):
    progress = Signal(int)
    finished = Signal(str)
    status = Signal(str)

    def __init__(self, df, output_path):
        super().__init__()
        self.df = df
        self.output_path = output_path

    def run(self):
        total = len(self.df)
        try:
            with open(self.output_path, "w", encoding="utf-8") as f:
                for idx, row in enumerate(self.df.iter_rows(named=True)):
                    game = chess.pgn.Game()
                    game.headers["White"] = row["white"]
                    game.headers["Black"] = row["black"]
                    game.headers["Result"] = row["result"]
                    game.headers["Date"] = row["date"]
                    game.headers["Event"] = row["event"]
                    game.headers["WhiteElo"] = str(row["w_elo"])
                    game.headers["BlackElo"] = str(row["b_elo"])
                    
                    game_board = chess.Board()
                    node = game
                    for uci in row["full_line"].split():
                        try:
                            move = chess.Move.from_uci(uci)
                            node = node.add_main_variation(move)
                            game_board.push(move)
                        except: break
                    
                    f.write(str(game) + "\n\n")
                    if idx % 100 == 0:
                        self.progress.emit(int((idx / total) * 100))
                        self.status.emit(f"Exportando partida {idx}/{total}...")
            self.finished.emit(self.output_path)
        except Exception as e: 
            self.status.emit(f"Error en exportación: {e}")
            self.finished.emit("")
