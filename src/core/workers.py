import os
import chess
import chess.pgn
import polars as pl
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
                    if count % 1000 == 0:
                        self.progress.emit(int((pgn.tell() / file_size) * 100))
                        self.status.emit(f"Cargando partida {count}...")
            
            out = self.path.replace(".pgn", ".parquet")
            pl.DataFrame(games).write_parquet(out)
            self.finished.emit(out)
        except Exception as e:
            self.status.emit(f"Error: {e}")

class StatsWorker(QThread):
    finished = Signal(object) # Envía el DataFrame resultante

    def __init__(self, db_manager, line_uci, is_white):
        super().__init__()
        self.db_manager = db_manager
        self.line_uci = line_uci
        self.is_white = is_white

    def run(self):
        try:
            res = self.db_manager.get_stats_for_position(self.line_uci, self.is_white)
            self.finished.emit(res)
        except:
            self.finished.emit(None)

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
            line = row["full_line"].split()
            board = chess.Board()
            positions.add(board.epd())
            for uci in line:
                try:
                    board.push_uci(uci)
                    positions.add(board.epd())
                except: break
            if idx % 500 == 0:
                self.progress.emit(int((idx/total)*100))
        self.finished.emit(positions)
