import polars as pl
import chess
import random

class PuzzleManager:
    def __init__(self, parquet_path):
        self.path = parquet_path
        self.lf = pl.scan_parquet(parquet_path)
        self.current_view = self.lf

    def apply_filters(self, min_rating=0, max_rating=4000, theme=None, opening=None):
        """Aplica filtros al LazyFrame"""
        # Empezamos siempre desde la base completa
        q = self.lf
        
        # Filtro de ELO obligatorio
        q = q.filter(
            (pl.col("Rating") >= int(min_rating)) & 
            (pl.col("Rating") <= int(max_rating))
        )
        
        # Filtro de Temas opcional
        if theme and len(theme) > 1:
            q = q.filter(pl.col("Themes").str.contains(theme.lower()))
        
        # Filtro de Apertura opcional
        if opening and len(opening) > 1:
            q = q.filter(pl.col("OpeningTags").str.contains(opening))
            
        self.current_view = q
        return self

    def get_sample(self):
        """Obtiene TODOS los resultados de la vista filtrada actual"""
        return self.current_view.collect()

    def get_random_puzzle(self):
        """Elegimos uno al azar de la vista filtrada"""
        df = self.current_view.head(500).collect()
        if df.is_empty(): return None
        
        row = df.row(random.randint(0, df.height - 1), named=True)
        return self.prepare_puzzle_data(row)

    def prepare_puzzle_data(self, row):
        """Aplica las reglas de Lichess al FEN y movimientos"""
        moves = row["Moves"].split()
        board = chess.Board(row["FEN"])
        
        # El oponente hace el primer movimiento
        first_move = chess.Move.from_uci(moves[0])
        board.push(first_move)
        
        return {
            "id": row["PuzzleId"],
            "start_fen": board.fen(),
            "solution": moves[1:], # Lista de jugadas UCI
            "rating": row["Rating"],
            "themes": row["Themes"],
            "opening": row["OpeningTags"]
        }
