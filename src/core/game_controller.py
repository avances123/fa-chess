import chess
from PySide6.QtCore import QObject, Signal

class GameController(QObject):
    # Señales para que la UI reaccione a cambios en el juego
    position_changed = Signal() # Se emite cuando cambia la posición actual
    game_loaded = Signal()     # Se emite cuando se carga una partida completa

    def __init__(self):
        super().__init__()
        self.board = chess.Board()
        self.full_mainline = []
        self.current_idx = 0

    def reset(self):
        self.board.reset()
        self.full_mainline = []
        self.current_idx = 0
        self.position_changed.emit()

    def load_uci_line(self, uci_string):
        self.reset()
        for uci in uci_string.split():
            if uci:
                try:
                    self.full_mainline.append(chess.Move.from_uci(uci))
                except: break
        self.game_loaded.emit()
        self.position_changed.emit()

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
