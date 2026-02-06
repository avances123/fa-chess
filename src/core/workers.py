import os
import chess
import chess.pgn
import polars as pl
from PySide6.QtCore import QThread, Signal

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
                    
                    def safe_int(val):
                        if not val: return 0
                        try:
                            clean_val = "".join(filter(str.isdigit, str(val)))
                            return int(clean_val) if clean_val else 0
                        except: return 0

                    full_line = [node.move.uci() for node in game.mainline()]
                    games.append({
                        "id": count,
                        "white": game.headers.get("White", "?"),
                        "black": game.headers.get("Black", "?"),
                        "w_elo": safe_int(game.headers.get("WhiteElo")),
                        "b_elo": safe_int(game.headers.get("BlackElo")),
                        "result": game.headers.get("Result", "*"),
                        "date": game.headers.get("Date", "????.??.??"),
                        "event": game.headers.get("Event", "?"),
                        "line": " ".join(full_line[:12]),
                        "full_line": " ".join(full_line)
                    })
                    count += 1
                    if count % 1000 == 0:
                        self.progress.emit(int((pgn.tell() / file_size) * 100))
                        self.status.emit(f"Cargando partida {count}...")
            
            out = self.path.replace(".pgn", ".parquet")
            pl.DataFrame(games).write_parquet(out)
            self.finished.emit(out)
        except Exception as e:
            self.status.emit(f"Error: {e}")

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
