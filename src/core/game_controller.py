import chess
import chess.pgn
from datetime import datetime
from PySide6.QtCore import QObject, Signal

class GameController(QObject):
    # Señales para que la UI reaccione a cambios en el juego
    position_changed = Signal() # Se emite cuando cambia la posición actual (FEN)
    game_loaded = Signal()     # Se emite cuando se carga una partida completa
    metadata_changed = Signal() # Se emite cuando cambian nombres de jugadores, etc.

    def __init__(self):
        super().__init__()
        self.board = chess.Board()
        self.metadata = {
            "White": "Jugador Blanco",
            "Black": "Jugador Negro",
            "Result": "*",
            "Date": "????.??.??",
            "Event": "Partida Casual",
            "Site": "Local",
            "WhiteElo": "",
            "BlackElo": ""
        }
        self.full_mainline = []
        self.current_idx = 0

    def reset(self):
        self.board.reset()
        self.full_mainline = []
        self.current_idx = 0
        self.metadata = {
            "White": "Jugador Blanco",
            "Black": "Jugador Negro",
            "Result": "*",
            "Date": datetime.now().strftime("%Y.%m.%d"),
            "Event": "Nueva Partida",
            "Site": "fa-chess",
            "WhiteElo": "",
            "BlackElo": ""
        }
        self.metadata_changed.emit()
        self.position_changed.emit()

    def load_uci_line(self, uci_string):
        """Carga una línea de movimientos UCI (principalmente para compatibilidad y tests)."""
        self.reset()
        for uci in uci_string.split():
            if uci:
                try:
                    move = chess.Move.from_uci(uci)
                    self.full_mainline.append(move)
                    self.board.push(move)
                    self.current_idx += 1
                except: break
        self.game_loaded.emit()
        self.position_changed.emit()

    def update_metadata(self, new_meta):
        self.metadata.update(new_meta)
        self.metadata_changed.emit()

    def load_pgn_game(self, game: chess.pgn.Game):
        self.reset()
        for key in self.metadata:
            if key in game.headers:
                self.metadata[key] = game.headers[key]
        
        # Cargar movimientos
        moves = []
        node = game
        while node.variations:
            next_node = node.variation(0)
            moves.append(next_node.move)
            node = next_node
        
        self.full_mainline = moves
        self.metadata_changed.emit()
        self.go_start() # Opcional: ir al principio o al final
        self.game_loaded.emit()

    def get_pgn_object(self):
        game = chess.pgn.Game()
        for key, value in self.metadata.items():
            game.headers[key] = str(value)
        
        node = game
        temp_board = chess.Board()
        for move in self.full_mainline:
            node = node.add_main_variation(move)
            temp_board.push(move)
        return game

    def make_move(self, move):
        if self.current_idx < len(self.full_mainline):
            if self.full_mainline[self.current_idx] == move:
                self.step_forward()
                return
            self.full_mainline = self.full_mainline[:self.current_idx]
        
        self.board.push(move)
        self.full_mainline.append(move)
        self.current_idx += 1
        self.position_changed.emit()

    def step_back(self):
        if self.current_idx > 0:
            self.current_idx -= 1
            self.board.pop()
            self.position_changed.emit()

    def step_forward(self):
        if self.current_idx < len(self.full_mainline):
            self.board.push(self.full_mainline[self.current_idx])
            self.current_idx += 1
            self.position_changed.emit()

    def go_start(self):
        while self.current_idx > 0:
            self.current_idx -= 1
            self.board.pop()
        self.position_changed.emit()

    def go_end(self):
        while self.current_idx < len(self.full_mainline):
            self.board.push(self.full_mainline[self.current_idx])
            self.current_idx += 1
        self.position_changed.emit()

    def jump_to_move(self, idx):
        self.go_start()
        for _ in range(idx):
            self.step_forward()

    @property
    def current_line_uci(self):
        return " ".join([m.uci() for m in self.board.move_stack])
