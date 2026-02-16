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
            from src.converter import convert_pgn_to_parquet
            temp_dir = tempfile.gettempdir()
            temp_parquet = os.path.join(temp_dir, f"new_games_{os.getpid()}.parquet")
            convert_pgn_to_parquet(self.pgn_path, temp_parquet, progress_callback=self.progress.emit)
            
            from src.core.db_manager import GAME_SCHEMA
            old_lf = pl.scan_parquet(self.target_path).cast(GAME_SCHEMA)
            new_lf = pl.scan_parquet(temp_parquet).cast(GAME_SCHEMA)
            
            max_id_res = old_lf.select(pl.col("id").max()).collect()
            max_id = max_id_res.item() if not max_id_res.is_empty() and max_id_res.item() is not None else 0
            new_lf = new_lf.with_columns(pl.col("id") + max_id + 1)

            final_tmp = self.target_path + ".tmp"
            pl.concat([old_lf, new_lf]).collect(streaming=True).write_parquet(final_tmp)

            if os.path.exists(self.target_path): os.remove(self.target_path)
            os.rename(final_tmp, self.target_path)
            if temp_parquet and os.path.exists(temp_parquet): os.remove(temp_parquet)
            self.finished.emit()
        except: self.finished.emit()

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
            lf = pl.scan_parquet(self.db_path); df = lf.collect()
            puzzles = []; total = df.height
            for i, row in enumerate(df.iter_rows(named=True)):
                if i % 100 == 0: self.progress.emit(int((i / total) * 100))
                board = chess.Board(); moves = row["full_line"].split()
                for m_idx, m_uci in enumerate(moves):
                    try:
                        move = chess.Move.from_uci(m_uci); is_tactical = False; puz_type = ""
                        if board.gives_check(move):
                            board.push(move)
                            if board.is_checkmate(): is_tactical = True; puz_type = "mate"
                            board.pop()
                        if not is_tactical and board.is_capture(move):
                            captured = board.piece_at(move.to_square)
                            if captured and captured.piece_type in [chess.QUEEN, chess.ROOK]: is_tactical = True; puz_type = "material"
                        if is_tactical:
                            puzzles.append({"PuzzleId": f"gen_{i}_{m_idx}", "FEN": board.fen(), "Moves": " ".join(moves[m_idx:]), "Rating": 1500, "Themes": puz_type, "OpeningTags": ""})
                            if puz_type == "mate": break
                        board.push(move)
                    except: break
            if puzzles: pl.DataFrame(puzzles).write_parquet(self.output_path); self.finished.emit(self.output_path)
            else: self.finished.emit("")
        except: self.finished.emit("")

class PuzzleSaveWorker(QThread):
    def __init__(self, app_db, puzzle_id, status):
        super().__init__(); self.app_db = app_db; self.puzzle_id = puzzle_id; self.status = status
    def run(self):
        try: self.app_db.save_puzzle_status(self.puzzle_id, self.status)
        except: pass

class StatsWorker(QThread):
    finished = Signal(object, object)
    progress = Signal(int)

    def __init__(self, db, current_line_uci, is_white_turn, current_hash=None, app_db=None, min_games=0):
        super().__init__(); self.db = db; self.app_db = app_db; self.current_line = current_line_uci; self.current_hash = current_hash; self.is_white = is_white_turn; self.min_games = min_games

    def run(self):
        try:
            if not self.current_hash: self.finished.emit(None, None); return
            cached, cached_eval = self.db.get_cached_stats(self.current_hash)
            if cached is not None: self.finished.emit(cached, cached_eval); return
            db_path = self.db.get_reference_path(); is_full_base = self.db.reference_db_name is not None or self.db.current_filter_query is None
            if db_path and self.app_db:
                persistent_cached, persistent_eval = self.app_db.get_opening_stats(db_path, self.current_hash)
                if persistent_cached is not None: self.db.cache_stats(self.current_hash, persistent_cached, persistent_eval); self.finished.emit(persistent_cached, persistent_eval); return
            lazy_view = self.db.get_reference_view()
            if lazy_view is None: self.finished.emit(None, None); return
            target = int(self.current_hash); total_count = lazy_view.select(pl.len()).collect().item()
            if total_count < 200000:
                stats = self._build_stats_query(lazy_view, target).collect(streaming=True); self.progress.emit(100)
            else:
                num_chunks = 10; chunk_size = total_count // num_chunks; all_chunks = []
                for i in range(num_chunks):
                    if self.isInterruptionRequested(): return
                    chunk_lazy = lazy_view.slice(i * chunk_size, chunk_size if i < num_chunks - 1 else total_count - (i * chunk_size))
                    all_chunks.append(self._build_stats_query(chunk_lazy, target).collect(streaming=True)); self.progress.emit(int(((i + 1) / num_chunks) * 100))
                combined = pl.concat(all_chunks)
                stats = combined.group_by("uci").agg([pl.col("c").sum(), pl.col("w").sum(), pl.col("d").sum(), pl.col("b").sum(), ((pl.col("avg_w_elo") * pl.col("c")).sum() / pl.col("c").sum()).alias("avg_w_elo"), ((pl.col("avg_b_elo") * pl.col("c")).sum() / pl.col("c").sum()).alias("avg_b_elo")]).sort("c", descending=True)
            stats = stats.with_columns(pl.lit(False).alias("_is_partial"))
            self.db.cache_stats(self.current_hash, stats, None)
            if is_full_base and db_path and self.app_db: self.app_db.save_opening_stats(db_path, self.current_hash, stats, None)
            self.finished.emit(stats, None)
        except: self.finished.emit(None, None)

    def _build_stats_query(self, lazy_df, target):
        return (lazy_df.filter(pl.col("fens").list.contains(pl.lit(target, dtype=pl.UInt64))).with_columns([pl.col("full_line").str.split(" ").alias("_m"), pl.col("fens").list.eval(pl.element() == target).list.arg_max().alias("_i")]).filter(pl.col("_i") < pl.col("_m").list.len()).with_columns(pl.col("_m").list.get(pl.col("_i")).alias("uci")).filter((pl.col("uci").str.len_chars() >= 4) & (pl.col("uci").str.len_chars() <= 5)).group_by("uci").agg([pl.len().alias("c"), (pl.col("result") == "1-0").sum().alias("w"), (pl.col("result") == "1/2-1/2").sum().alias("d"), (pl.col("result") == "0-1").sum().alias("b"), pl.col("w_elo").mean().fill_null(0).alias("avg_w_elo"), pl.col("b_elo").mean().fill_null(0).alias("avg_b_elo")]))

class PGNExportWorker(QThread):
    progress = Signal(int); finished = Signal(str); status = Signal(str)
    def __init__(self, df, output_path): super().__init__(); self.df = df; self.output_path = output_path
    def run(self):
        total = len(self.df)
        try:
            with open(self.output_path, "w", encoding="utf-8") as f:
                for idx, row in enumerate(self.df.iter_rows(named=True)):
                    game = chess.pgn.Game(); game.headers["White"] = row["white"]; game.headers["Black"] = row["black"]; game.headers["Result"] = row["result"]; game.headers["Date"] = row["date"]; game.headers["Event"] = row["event"]; game.headers["WhiteElo"] = str(row["w_elo"]); game.headers["BlackElo"] = str(row["b_elo"])
                    game_board = chess.Board(); node = game
                    for uci in row["full_line"].split():
                        try: move = chess.Move.from_uci(uci); node = node.add_main_variation(move); game_board.push(move)
                        except: break
                    f.write(str(game) + "\n\n")
                    if idx % 100 == 0: self.progress.emit(int((idx / total) * 100))
            self.finished.emit(self.output_path)
        except: self.finished.emit("")

class CachePopulatorWorker(QThread):
    progress = Signal(int); status = Signal(str); finished = Signal(int)
    def __init__(self, db_manager, app_db, min_games=50000): super().__init__(); self.db = db_manager; self.app_db = app_db; self.min_games = min_games; self.running = True
    def run(self):
        try:
            ref_path = self.db.get_reference_path(); lazy_view = self.db.get_reference_view()
            if not ref_path or lazy_view is None: self.finished.emit(0); return
            from collections import deque
            queue = deque([chess.Board()]); processed_hashes = set(); count = 0
            while queue and self.running:
                board = queue.popleft()
                if len(board.move_stack) >= 30: continue
                pos_hash = chess.polyglot.zobrist_hash(board)
                if pos_hash in processed_hashes: continue
                processed_hashes.add(pos_hash)
                stats, _ = self.app_db.get_opening_stats(ref_path, pos_hash)
                if stats is None:
                    stats = self._calculate_stats_sync(lazy_view, pos_hash)
                    if stats is not None:
                        self.app_db.save_opening_stats(ref_path, pos_hash, stats, None); count += 1
                        if count % 10 == 0: self.progress.emit(count)
                if stats is not None and not stats.is_empty() and "uci" in stats.columns:
                    for row in stats.rows(named=True):
                        if row["c"] >= self.min_games:
                            nb = board.copy()
                            try: nb.push_uci(row["uci"]); queue.append(nb)
                            except: pass
                if count > 5000: break
            self.finished.emit(count)
        except: self.finished.emit(0)
    def _calculate_stats_sync(self, lazy_view, pos_hash):
        target = int(pos_hash)
        q = (lazy_view.filter(pl.col("fens").list.contains(pl.lit(target, dtype=pl.UInt64))).with_columns([pl.col("full_line").str.split(" ").alias("_m"), pl.col("fens").list.eval(pl.element() == target).list.arg_max().alias("_i")]).filter(pl.col("_i") < pl.col("_m").list.len()).with_columns(pl.col("_m").list.get(pl.col("_i")).alias("uci")).filter((pl.col("uci").str.len_chars() >= 4) & (pl.col("uci").str.len_chars() <= 5)).group_by("uci").agg([pl.len().alias("c"), (pl.col("result") == "1-0").sum().alias("w"), (pl.col("result") == "1/2-1/2").sum().alias("d"), (pl.col("result") == "0-1").sum().alias("b"), pl.col("w_elo").mean().fill_null(0).alias("avg_w_elo"), pl.col("b_elo").mean().fill_null(0).alias("avg_b_elo")]))
        try: return q.collect(streaming=True)
        except: return None
    def stop(self): self.running = False

class RefutationWorker(QThread):
    finished = Signal(str, str)
    def __init__(self, engine_path, fen):
        super().__init__(); self.engine_path = engine_path; self.fen = fen; self.running = True
    def run(self):
        try:
            import chess.engine
            engine = chess.engine.SimpleEngine.popen_uci(self.engine_path)
            board = chess.Board(self.fen)
            info = engine.analyse(board, chess.engine.Limit(time=0.2, depth=14))
            best_move = info.get("pv")[0] if info.get("pv") else None
            if best_move:
                score = info.get("score").relative.score(mate_score=10000) / 100.0
                msg = f"El rival responde {board.san(best_move)}."
                if score > 2: msg += " Pierdes material."
                self.finished.emit(best_move.uci(), msg)
            engine.quit()
        except: self.finished.emit("", "Incorrecto")
    def stop(self): self.running = False
