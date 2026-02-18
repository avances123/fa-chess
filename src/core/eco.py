import chess
import os
import re

class ECOManager:
    def __init__(self, eco_path):
        self.eco_db = {} # FEN_key -> name
        self.openings = []
        if os.path.exists(eco_path):
            self.load_eco(eco_path)

    def _get_fen_key(self, board):
        """Genera una clave basada en la posición, turno y enroques (ignora relojes)."""
        parts = board.fen().split()
        return " ".join(parts[:4]) # rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -

    def load_eco(self, path):
        self.openings = [] # Para compatibilidad con tests antiguos
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("[") or '"' not in line: continue
                    self.openings.append(line)
                    
                    # Formato: A00 "Nombre" 1. e4
                    match = re.search(r'^(\S+)\s+"([^"]+)"\s+(.+)$', line)
                    if not match: continue
                    
                    eco_code, name, moves_part = match.groups()
                    
                    # Limpieza básica
                    moves_part = re.sub(r'\{.*?\}|\(.*?\)', '', moves_part)
                    clean_moves = re.sub(r'\d+\.+\s*', '', moves_part)
                    clean_moves = re.sub(r'[\!\?\+\#\*]', '', clean_moves)
                    
                    try:
                        board = chess.Board()
                        for san in clean_moves.split():
                            if not san: continue
                            board.push_san(san)
                        
                        key = self._get_fen_key(board)
                        full_name = f"{eco_code}: {name}" if eco_code else name
                        self.eco_db[key] = full_name
                    except:
                        continue
            print(f"ECOManager: Cargadas {len(self.eco_db)} aperturas")
        except Exception as e:
            print(f"Error cargando ECO: {e}")

    def get_opening_name(self, board):
        """Busca el nombre de la apertura basándose en el estado actual del tablero."""
        if isinstance(board, str):
            temp_board = chess.Board()
            for move in board.split():
                try: temp_board.push_uci(move)
                except: break
        else:
            temp_board = board.copy()
        
        orig_stack_len = len(temp_board.move_stack)
        while True:
            key = self._get_fen_key(temp_board)
            if key in self.eco_db:
                return self.eco_db[key], len(temp_board.move_stack)
            
            if len(temp_board.move_stack) == 0: break
            temp_board.pop()
            
        return "Posición Inicial" if orig_stack_len == 0 else "Variante Desconocida", 0
