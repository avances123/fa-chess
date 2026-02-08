import os
import chess
import chess.pgn
import chess.polyglot
import polars as pl
import gc
from PySide6.QtCore import QThread, Signal
from src.converter import extract_game_data

class PGNWorker(QThread):
    progress = Signal(int)
    finished = Signal(str)
    status = Signal(str)

    def __init__(self, path):
        super().__init__()
        self.path = path

    def run(self):
        from src.converter import convert_pgn_to_parquet
        try:
            out = self.path.replace(".pgn", ".parquet")
            # Delegamos en la función optimizada de converter.py
            # Nota: Podríamos envolver la Progress de rich, pero por ahora 
            # lo mantenemos simple o refactorizamos convert_pgn_to_parquet para aceptar un callback
            convert_pgn_to_parquet(self.path, out)
            self.finished.emit(out)
        except Exception as e:
            self.status.emit(f"Error: {e}")

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

            # 1. Intentar obtener de la caché de RAM
            cached = self.db.get_cached_stats(self.current_hash)
            if cached is not None:
                self.finished.emit(cached)
                return

            # 2. Cálculo dinámico con Polars
            lazy_view = self.db.get_current_view()
            if lazy_view is None:
                self.finished.emit(None)
                return

            target = int(self.current_hash)
            
            # Expresión de Polars optimizada
            q = (
                lazy_view
                .filter(pl.col("fens").list.contains(pl.lit(target, dtype=pl.UInt64)))
                .with_columns([
                    pl.col("full_line").str.split(" ").alias("_m"),
                    pl.col("fens").list.eval(pl.element() == target).list.arg_max().alias("_i")
                ])
                # Solo procesar si hay una jugada después de la posición actual
                .filter(pl.col("_i") < pl.col("_m").list.len())
                .with_columns(
                    pl.col("_m").list.get(pl.col("_i")).alias("uci")
                )
                # Validar longitud de UCI (normalmente 4 o 5 caracteres)
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
            
            # Ejecución optimizada
            stats = q.collect(streaming=True)
            
            # Enriquecemos con metadatos
            stats = stats.with_columns(pl.lit(False).alias("_is_partial"))
            
            # Guardamos en caché
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
        import chess.pgn
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
                            move = chess.Move.from_uci(uci); node = node.add_main_variation(move); game_board.push(move)
                        except: break
                    f.write(str(game) + "\n\n")
                    if idx % 100 == 0:
                        self.progress.emit(int((idx / total) * 100)); self.status.emit(f"Exportando partida {idx}/{total}...")
            self.finished.emit(self.output_path)
        except Exception as e: self.status.emit(f"Error en exportación: {e}")

class MaskWorker(QThread):
    progress = Signal(int)
    finished = Signal(set)

    def __init__(self, df):
        super().__init__()
        self.df = df

    def run(self):
        positions = set()
        total = len(self.df)
        for idx, row in enumerate(self.df.iter_rows(named=True)):
            line = row["full_line"].split(); board = chess.Board(); positions.add(board.epd())
            for uci in line:
                try: board.push_uci(uci); positions.add(board.epd())
                except: break
            if idx % 500 == 0: self.progress.emit(int((idx/total)*100))
        self.finished.emit(positions)
