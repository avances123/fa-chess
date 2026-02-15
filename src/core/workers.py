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

class PuzzleGeneratorWorker(QThread):
    progress = Signal(int)
    status = Signal(str)
    finished = Signal(str)

    def __init__(self, db_path, output_path):
        super().__init__()
        self.db_path = db_path
        self.output_path = output_path

    def run(self):
        try:
            self.status.emit("Escaneando base de datos para buscar táctica...")
            lf = pl.scan_parquet(self.db_path)
            df = lf.collect()
            
            puzzles = []
            total = df.height
            
            for i, row in enumerate(df.iter_rows(named=True)):
                if i % 100 == 0:
                    self.progress.emit(int((i / total) * 100))
                    self.status.emit(f"Analizando partida {i}/{total}...")

                board = chess.Board()
                moves = row["full_line"].split()
                
                for m_idx, m_uci in enumerate(moves):
                    try:
                        move = chess.Move.from_uci(m_uci)
                        is_tactical = False
                        puz_type = ""
                        
                        if board.gives_check(move):
                            board.push(move)
                            if board.is_checkmate():
                                is_tactical = True
                                puz_type = "mate"
                            board.pop()
                        
                        if not is_tactical and board.is_capture(move):
                            captured = board.piece_at(move.to_square)
                            if captured and captured.piece_type in [chess.QUEEN, chess.ROOK]:
                                is_tactical = True
                                puz_type = "material"

                        if is_tactical:
                            puzzles.append({
                                "PuzzleId": f"gen_{i}_{m_idx}", # ID generado
                                "FEN": board.fen(),
                                "Moves": " ".join(moves[m_idx:]),
                                "Rating": 1500, # Valor por defecto
                                "Themes": puz_type,
                                "OpeningTags": ""
                            })
                            if puz_type == "mate": break
                        
                        board.push(move)
                    except: break

            if puzzles:
                self.status.emit(f"Generando archivo con {len(puzzles)} ejercicios...")
                pl.DataFrame(puzzles).write_parquet(self.output_path)
                self.finished.emit(self.output_path)
            else:
                self.status.emit("No se encontraron ejercicios tácticos.")
                self.finished.emit("")

        except Exception as e:
            self.status.emit(f"Error en generador: {e}")
            self.finished.emit("")

class PuzzleSaveWorker(QThread):
    def __init__(self, app_db, puzzle_id, status):
        super().__init__()
        self.app_db = app_db
        self.puzzle_id = puzzle_id
        self.status = status

    def run(self):
        try:
            # Guardamos el estado en SQLite (Instantáneo y seguro)
            self.app_db.save_puzzle_status(self.puzzle_id, self.status)
        except Exception as e:
            print(f"Error en AppDB guardando puzzle: {e}")

class StatsWorker(QThread):
    finished = Signal(object, object) # stats_df, engine_eval
    progress = Signal(int) # Porcentaje 0-100

    def __init__(self, db, current_line_uci, is_white_turn, current_hash=None, app_db=None, min_games=0):
        super().__init__()
        self.db = db
        self.app_db = app_db
        self.current_line = current_line_uci
        self.current_hash = current_hash
        self.is_white = is_white_turn
        self.min_games = min_games

    def run(self):
        try:
            if not self.current_hash:
                self.finished.emit(None, None)
                return

            from src.config import logger
            import time

            # 1. Intentar obtener de la caché de RAM
            cached, cached_eval = self.db.get_cached_stats(self.current_hash)
            if cached is not None:
                logger.debug(f"StatsWorker: HIT Cache RAM para {self.current_hash}")
                self.finished.emit(cached, cached_eval)
                return

            # 2. Intentar obtener de la caché PERSISTENTE
            db_path = self.db.get_reference_path()
            is_full_base = self.db.reference_db_name is not None or self.db.current_filter_query is None
            
            if db_path and self.app_db:
                persistent_cached, persistent_eval = self.app_db.get_opening_stats(db_path, self.current_hash)
                if persistent_cached is not None:
                    logger.info(f"StatsWorker: HIT Cache SQLite para {self.current_hash}")
                    self.db.cache_stats(self.current_hash, persistent_cached, persistent_eval)
                    self.finished.emit(persistent_cached, persistent_eval)
                    return

            # 3. Cálculo con Polars
            lazy_view = self.db.get_reference_view()
            if lazy_view is None:
                self.finished.emit(None, None)
                return

            logger.info(f"StatsWorker: Calculando árbol con Polars para {self.current_hash}...")
            start = time.time()
            target = int(self.current_hash)
            
            # 1. Obtener el conteo total para dividir el trabajo
            total_count = lazy_view.select(pl.len()).collect().item()
            
            # Si la base es pequeña (< 200k), lo hacemos de un tirón para no perder rendimiento
            if total_count < 200000:
                q = self._build_stats_query(lazy_view, target)
                stats = q.collect(streaming=True)
                self.progress.emit(100)
            else:
                # Para bases grandes, dividimos en 10 bloques para dar feedback de progreso
                num_chunks = 10
                chunk_size = total_count // num_chunks
                all_chunks = []
                
                for i in range(num_chunks):
                    if self.isInterruptionRequested(): return
                    
                    offset = i * chunk_size
                    # El último bloque toma el resto
                    length = chunk_size if i < num_chunks - 1 else total_count - offset
                    
                    chunk_lazy = lazy_view.slice(offset, length)
                    q = self._build_stats_query(chunk_lazy, target)
                    chunk_res = q.collect(streaming=True)
                    all_chunks.append(chunk_res)
                    
                    self.progress.emit(int(((i + 1) / num_chunks) * 100))
                
                # Combinar resultados de los bloques
                combined = pl.concat(all_chunks)
                # Volver a agrupar porque el mismo movimiento UCI puede estar en varios bloques
                stats = (
                    combined.group_by("uci")
                    .agg([
                        pl.col("c").sum(),
                        pl.col("w").sum(),
                        pl.col("d").sum(),
                        pl.col("b").sum(),
                        # Promedio ponderado para los Elos
                        ((pl.col("avg_w_elo") * pl.col("c")).sum() / pl.col("c").sum()).alias("avg_w_elo"),
                        ((pl.col("avg_b_elo") * pl.col("c")).sum() / pl.col("c").sum()).alias("avg_b_elo")
                    ])
                    .sort("c", descending=True)
                )

            stats = stats.with_columns(pl.lit(False).alias("_is_partial"))
            elapsed = time.time() - start
            logger.debug(f"StatsWorker: Cálculo finalizado en {elapsed:.2f}s")
            
            # Guardamos en RAM
            self.db.cache_stats(self.current_hash, stats, None) # Eval es None al calcular nuevo
            
            # 4. Guardamos en PERSISTENTE si es base completa
            if is_full_base and db_path and self.app_db:
                logger.info(f"StatsWorker: Persistiendo resultado en SQLite...")
                self.app_db.save_opening_stats(db_path, self.current_hash, stats, None)
                
            self.finished.emit(stats, None)

        except Exception as e:
            print(f"Error en StatsWorker: {e}")
            self.finished.emit(None, None)

    def _build_stats_query(self, lazy_df, target):
        """Helper para construir la consulta base de estadísticas"""
        return (
            lazy_df
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
        )

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

class CachePopulatorWorker(QThread):
    progress = Signal(int) # posiciones cacheadas
    status = Signal(str)
    finished = Signal(int)

    def __init__(self, db_manager, app_db, min_games=50000):
        super().__init__()
        self.db = db_manager
        self.app_db = app_db
        self.min_games = min_games
        self.running = True

    def run(self):
        try:
            ref_path = self.db.get_reference_path()
            lazy_view = self.db.get_reference_view()
            if not ref_path or lazy_view is None:
                self.finished.emit(0)
                return

            from collections import deque
            import chess.polyglot
            
            queue = deque([chess.Board()])
            processed_hashes = set()
            count = 0

            self.status.emit("Analizando árbol teórico...")

            while queue and self.running:
                board = queue.popleft()
                
                # LÍMITE DE PROFUNDIDAD: Jugada 15 (30 medios movimientos)
                if len(board.move_stack) >= 30: continue

                pos_hash = chess.polyglot.zobrist_hash(board)
                
                if pos_hash in processed_hashes: continue
                processed_hashes.add(pos_hash)

                # 1. ¿Está ya en caché? (Con validación de formato nuevo)
                stats = self.app_db.get_opening_stats(ref_path, pos_hash)
                
                if stats is not None and "uci" not in stats.columns:
                    stats = None # Forzar recálculo de caché antigua

                if stats is None:
                    # 2. Calcular si no está
                    stats = self._calculate_stats_sync(lazy_view, pos_hash)
                    if stats is not None:
                        self.app_db.save_opening_stats(ref_path, pos_hash, stats)
                        count += 1
                        if count % 10 == 0:
                            self.progress.emit(count)
                            self.status.emit(f"Cacheando: {count} posiciones nuevas...")

                # 3. Decidir si profundizamos (si el movimiento tiene >= min_games)
                # IMPORTANTE: Incluso si ya estaba en caché, debemos explorar sus hijos
                if stats is not None and not stats.is_empty() and "uci" in stats.columns:
                    for row in stats.rows(named=True):
                        # Solo profundizamos si el movimiento tiene volumen suficiente
                        if "c" in row and row["c"] >= self.min_games:
                            nb = board.copy()
                            try:
                                nb.push_uci(row["uci"])
                                h = chess.polyglot.zobrist_hash(nb)
                                if h not in processed_hashes:
                                    queue.append(nb)
                            except: pass
                
                # Check de parada frecuente
                if count > 5000: break # Seguridad para no llenar el disco infinitamente
            
            self.finished.emit(count)
        except Exception as e:
            self.status.emit(f"Error: {e}")
            self.finished.emit(count)

    def _calculate_stats_sync(self, lazy_view, pos_hash):
        target = int(pos_hash)
        q = (
            lazy_view
            .filter(pl.col("fens").list.contains(pl.lit(target, dtype=pl.UInt64)))
            .with_columns([
                pl.col("full_line").str.split(" ").alias("_m"),
                pl.col("fens").list.eval(pl.element() == target).list.arg_max().alias("_i")
            ])
            .filter(pl.col("_i") < pl.col("_m").list.len())
            .with_columns(pl.col("_m").list.get(pl.col("_i")).alias("uci"))
            .filter((pl.col("uci").str.len_chars() >= 4) & (pl.col("uci").str.len_chars() <= 5))
            .group_by("uci")
            .agg([
                pl.len().alias("c"),
                (pl.col("result") == "1-0").sum().alias("w"),
                (pl.col("result") == "1/2-1/2").sum().alias("d"),
                (pl.col("result") == "0-1").sum().alias("b"),
                pl.col("w_elo").mean().fill_null(0).alias("avg_w_elo"),
                pl.col("b_elo").mean().fill_null(0).alias("avg_b_elo")
            ])
        )
        try: return q.collect(streaming=True)
        except: return None

    def stop(self):
        self.running = False
