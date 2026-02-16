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
        q = self.lf
        
        # Filtro de ELO
        q = q.filter(
            (pl.col("Rating") >= int(min_rating)) & 
            (pl.col("Rating") <= int(max_rating))
        )
        
        # Filtro de Temas (Lógica AND estricta, Insensible a mayúsculas)
        if theme and len(theme.strip()) > 1:
            words = theme.split()
            for word in words:
                if len(word) > 1:
                    q = q.filter(pl.col("Themes").str.contains(f"(?i){word}"))
        
        # Ordenación por ELO ascendente por defecto
        q = q.sort("Rating", descending=False)
            
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
        """Devuelve los datos necesarios para iniciar el puzzle con historial"""
        moves = row["Moves"].split()
        return {
            "id": row["PuzzleId"],
            "initial_fen": row["FEN"], # FEN antes del movimiento del oponente
            "opponent_move": moves[0], # El movimiento que el oponente hace para plantear el reto
            "solution": moves[1:],     # Lista de jugadas UCI de la solución
            "rating": row["Rating"],
            "themes": row["Themes"],
            "opening": row["OpeningTags"]
        }
