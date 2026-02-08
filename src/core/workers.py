import os
import chess
import chess.pgn
import chess.polyglot
import polars as pl
import gc
from PySide6.QtCore import QThread, Signal
from converter import extract_game_data

class PGNWorker(QThread):
    progress = Signal(int)
    finished = Signal(str)
    status = Signal(str)

    def __init__(self, path):
        super().__init__()
        self.path = path

    def run(self):
        games = []
        file_size = os.path.getsize(self.path)
        try:
            with open(self.path, encoding="utf-8", errors="ignore") as pgn:
                count = 0
                while True:
                    game = chess.pgn.read_game(pgn)
                    if not game: break
                    
                    games.append(extract_game_data(count, game))
                    count += 1
                    if count % 100 == 0:
                        self.progress.emit(int((pgn.tell() / file_size) * 100))
                        self.status.emit(f"Cargando partida {count}...")
            
            out = self.path.replace(".pgn", ".parquet")
            pl.DataFrame(games).write_parquet(out)
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

            # 1. Intentar obtener de la caché de RAM (Instantáneo)
            cached = self.db.get_cached_stats(self.current_hash)
            if cached is not None:
                self.finished.emit(cached)
                return

            # 2. Cálculo dinámico OPTIMIZADO con Polars Streaming
            lazy_view = self.db.get_current_view()
            if lazy_view is None:
                self.finished.emit(None)
                return

            target = int(self.current_hash)
            
            # Algoritmo de extracción nativa en C++
            q = (
                lazy_view
                .filter(pl.col("fens").list.contains(pl.lit(target, dtype=pl.UInt64)))
                .with_columns([
                    pl.col("full_line").str.split(" ").alias("_m"),
                    pl.col("fens").list.eval(pl.element() == target).list.arg_max().alias("_i")
                ])
                .with_columns(
                    pl.col("_m").list.get(pl.col("_i")).alias("uci")
                )
                .filter(pl.col("uci").is_not_null() & (pl.col("uci") != ""))
                .group_by("uci")
                .agg([
                    pl.len().alias("c"),
                    pl.col("result").filter(pl.col("result") == "1-0").len().alias("w"),
                    pl.col("result").filter(pl.col("result") == "1/2-1/2").len().alias("d"),
                    pl.col("result").filter(pl.col("result") == "0-1").len().alias("b"),
                    pl.mean("w_elo").fill_null(0).alias("avg_w_elo"),
                    pl.mean("b_elo").fill_null(0).alias("avg_b_elo")
                ])
                .sort("c", descending=True)
            )
            
            # Ejecución en streaming (mínimo consumo RAM)
            stats = q.collect(streaming=True)
            
            # Enriquecemos con metadatos
            stats = stats.with_columns(pl.lit(False).alias("_is_partial"))
            
            # Guardamos en caché para el resto de la sesión
            self.db.cache_stats(self.current_hash, stats)
            self.finished.emit(stats)

        except Exception as e:
            print(f"Error en StatsWorker: {e}")
            self.finished.emit(None)

class TreeBuilderWorker(QThread):
    # Se mantiene por compatibilidad si el usuario quiere generar un archivo estático
    # pero el programa ya no depende de él para el día a día.
    progress = Signal(int)
    finished = Signal(str)
    status = Signal(str)

    def __init__(self, db_path):
        super().__init__()
        self.db_path = db_path
        self.running = True

    def run(self):
        import tempfile, shutil, subprocess, sys
        temp_dir = tempfile.mkdtemp(prefix="fa_chess_tree_")
        bucket_results_dir = tempfile.mkdtemp(prefix="fa_chess_buckets_")
        try:
            self.status.emit("Calculando total...")
            lazy_source = pl.scan_parquet(self.db_path)
            total_rows = lazy_source.select(pl.len()).collect().item()
            chunk_size = 200000 
            total_chunks = (total_rows + chunk_size - 1) // chunk_size
            script_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "scripts", "process_chunk.py")
            for i in range(0, total_rows, chunk_size):
                if not self.running: break
                chunk_num = i // chunk_size
                self.status.emit(f"Fase 1: Mapeando bloque {chunk_num + 1}/{total_chunks}...")
                self.progress.emit(int((i / total_rows) * 70))
                cmd = [sys.executable, script_path, "--mode", "process", "--db", self.db_path, "--start", str(i), "--len", str(chunk_size), "--temp_dir", temp_dir, "--chunk_num", str(chunk_num)]
                subprocess.run(cmd, check=True)
                gc.collect()
            if not self.running: return
            for b in range(16):
                if not self.running: break
                self.status.emit(f"Fase 2: Consolidando cubo {b+1}/16...")
                self.progress.emit(70 + int((b / 16) * 25))
                bucket_out = os.path.join(bucket_results_dir, f"bucket_{b}.parquet")
                cmd = [sys.executable, script_path, "--mode", "merge_bucket", "--bucket", str(b), "--temp_dir", temp_dir, "--out", bucket_out]
                subprocess.run(cmd, check=True)
                gc.collect()
            if not self.running: return
            self.status.emit("Guardando árbol final...")
            out_path = self.db_path.replace(".parquet", ".tree.parquet")
            cmd_final = [sys.executable, script_path, "--mode", "final_merge", "--temp_dir", bucket_results_dir, "--out", out_path]
            subprocess.run(cmd_final, check=True)
            self.status.emit("Árbol generado correctamente.")
            self.finished.emit(out_path)
        except Exception as e:
            error_msg = f"Error generando árbol: {str(e)}"
            self.status.emit(error_msg); print(error_msg)
        finally:
            for d in [temp_dir, bucket_results_dir]:
                try:
                    if os.path.exists(d): shutil.rmtree(d)
                except: pass
            self.progress.emit(100)

    def stop(self): self.running = False

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
