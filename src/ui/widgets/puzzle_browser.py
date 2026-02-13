from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                             QPushButton, QFrame, QLineEdit, QListWidget, QListWidgetItem, QSplitter, QSlider)
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QColor, QFont
import chess
import os
import polars as pl
import qtawesome as qta
from src.ui.board import ChessBoard
from src.core.game_controller import GameController
from src.core.puzzle_manager import PuzzleManager
from src.config import PUZZLE_FILE

class PuzzleBrowserWidget(QWidget):
    def __init__(self, parent_main=None):
        super().__init__(parent_main)
        self.parent_main = parent_main
        self.game = GameController()
        self.manager = None
        self.puzzle_df = None
        self.current_puzzle = None
        self.current_index = 0
        self.solution_idx = 0
        self.has_failed_current = False 
        
        self.game.position_changed.connect(self.update_ui)
        self.init_ui()
        
        # Carga automática silenciosa
        from src.config import PUZZLE_FILE
        if os.path.exists(PUZZLE_FILE):
            self.load_db(PUZZLE_FILE)
        else:
            self.label_feedback.setText(f"Aviso: No se encontró {os.path.basename(PUZZLE_FILE)} en assets/")

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # --- PANEL DE FILTROS SUPERIOR (SLIDER) ---
        filter_panel = QFrame()
        filter_panel.setStyleSheet("background: #f8f9fa; border-bottom: 1px solid #ddd;")
        fl = QHBoxLayout(filter_panel)
        
        # Slider de ELO
        vl_elo = QVBoxLayout()
        self.label_elo_val = QLabel("ELO Objetivo: <b>600</b> (rango 550-650)")
        self.slider_elo = QSlider(Qt.Horizontal)
        self.slider_elo.setRange(400, 3000)
        self.slider_elo.setValue(600)
        self.slider_elo.setFixedWidth(250)
        self.slider_elo.valueChanged.connect(self.on_slider_changed)
        vl_elo.addWidget(self.label_elo_val)
        vl_elo.addWidget(self.slider_elo)
        fl.addLayout(vl_elo)
        
        fl.addSpacing(20)
        
        # Filtro de Tema
        self.edit_theme = QLineEdit()
        self.edit_theme.setPlaceholderText("Filtrar por tema (mate, fork...)")
        self.edit_theme.textChanged.connect(self.apply_filters)
        fl.addWidget(QLabel("Tema:"))
        fl.addWidget(self.edit_theme)
        
        fl.addStretch()
        layout.addWidget(filter_panel)

        # --- CUERPO CENTRAL (Splitter) ---
        self.splitter = QSplitter(Qt.Horizontal)
        self.chess_board = ChessBoard(self.game.board, self)
        if self.parent_main:
            self.chess_board.color_light = self.parent_main.board_ana.color_light
            self.chess_board.color_dark = self.parent_main.board_ana.color_dark
        self.chess_board.piece_moved.connect(self.check_move)
        self.splitter.addWidget(self.chess_board)
        
        self.list_view = QListWidget()
        self.list_view.itemClicked.connect(self.on_item_selected)
        self.splitter.addWidget(self.list_view)
        
        self.splitter.setStretchFactor(0, 3)
        self.splitter.setStretchFactor(1, 1)
        layout.addWidget(self.splitter)

        self.label_feedback = QLabel("Esperando carga de base...")
        self.label_feedback.setStyleSheet("font-size: 14px; font-weight: bold; background: #333; color: #fff; padding: 10px;")
        self.label_feedback.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.label_feedback)

    def on_slider_changed(self, val):
        self.label_elo_val.setText(f"ELO Objetivo: <b>{val}</b> (rango {val-50}-{val+50})")
        self.apply_filters()

    def load_db(self, path):
        try:
            self.manager = PuzzleManager(path)
            self.label_feedback.setText(f"Base de ejercicios cargada.")
            self.apply_filters()
        except Exception as e:
            self.label_feedback.setText(f"Error al cargar base: {e}")

    def apply_filters(self):
        if not self.manager: return
        try:
            target_elo = self.slider_elo.value()
            theme = self.edit_theme.text().strip()
            
            # Margen de +-50 puntos solicitado
            self.manager.apply_filters(min_rating=target_elo-50, max_rating=target_elo+50, theme=theme)
            self.puzzle_df = self.manager.get_sample()
            
            if "status" not in self.puzzle_df.columns:
                self.puzzle_df = self.puzzle_df.with_columns(pl.lit("pending").alias("status"))
            
            self.refresh_puzzle_list()
            self.label_feedback.setText(f"Encontrados {self.puzzle_df.height} puzzles en este nivel.")
        except Exception as e:
            self.label_feedback.setText(f"Error al filtrar: {e}")

    def refresh_puzzle_list(self):
        self.list_view.clear()
        for i in range(self.puzzle_df.height):
            row = self.puzzle_df.row(i, named=True)
            it = QListWidgetItem(f" [{row['Rating']}] {row['Themes'][:35]}...")
            status = row.get("status", "pending")
            if status == "success":
                it.setIcon(qta.icon('fa5s.star', color='#2e7d32'))
                it.setForeground(QColor("#2e7d32"))
            elif status == "fail":
                it.setIcon(qta.icon('fa5s.star', color='#c62828'))
                it.setForeground(QColor("#c62828"))
            else:
                it.setIcon(qta.icon('fa5s.star', color='#ccc'))
            it.setData(Qt.UserRole, row)
            self.list_view.addItem(it)

    def on_item_selected(self, item):
        self.current_index = self.list_view.row(item)
        row = item.data(Qt.UserRole)
        puzzle = self.manager.prepare_puzzle_data(row)
        self.start_puzzle(puzzle)

    def start_puzzle(self, puzzle):
        self.current_puzzle = puzzle
        self.solution_idx = 0
        self.has_failed_current = False
        self.game.board.set_fen(puzzle["start_fen"])
        self.game.full_mainline = []
        self.game.current_idx = 0
        self.game.position_changed.emit()
        is_white = self.game.board.turn == chess.WHITE
        self.chess_board.flipped = not is_white
        turno_str = "⚪ Juegan BLANCAS" if is_white else "⚫ Juegan NEGRAS"
        self.label_feedback.setText(f"{turno_str} | Encuentra la solución.")
        self.label_feedback.setStyleSheet("background: #1976d2; color: #fff; padding: 10px;")
        self.chess_board.setEnabled(True)
        self.chess_board.update_board()

    def check_move(self, move_uci):
        if not self.current_puzzle: return
        correct_move = self.current_puzzle["solution"][self.solution_idx]
        if move_uci == correct_move:
            self.solution_idx += 1
            if self.solution_idx >= len(self.current_puzzle["solution"]):
                self.label_feedback.setText("¡RESUELTO! 🏆")
                self.label_feedback.setStyleSheet("background: #2e7d32; color: #fff; padding: 10px;")
                self.chess_board.setEnabled(False)
                status = "fail" if self.has_failed_current else "success"
                self.update_status(status)
            else:
                opp_move_uci = self.current_puzzle["solution"][self.solution_idx]
                self.game.make_move(chess.Move.from_uci(opp_move_uci))
                self.solution_idx += 1
                self.label_feedback.setText("¡Bien! El oponente responde. Sigue...")
        else:
            self.has_failed_current = True
            self.label_feedback.setText("Incorrecto. Prueba otra vez.")
            self.label_feedback.setStyleSheet("background: #c62828; color: #fff; padding: 10px;")
            self.game.step_back()
            self.update_status("fail")

    def update_status(self, status):
        self.puzzle_df[self.current_index, "status"] = status
        it = self.list_view.item(self.current_index)
        if status == "success":
            it.setIcon(qta.icon('fa5s.star', color='#2e7d32'))
            it.setForeground(QColor("#2e7d32"))
        else:
            it.setIcon(qta.icon('fa5s.star', color='#c62828'))
            it.setForeground(QColor("#c62828"))
        # Guardado en segundo plano
        if self.manager and self.manager.path:
            from src.core.workers import PuzzleSaveWorker
            self._save_worker = PuzzleSaveWorker(self.manager.path, self.current_puzzle["id"], status)
            self._save_worker.start()

    def update_ui(self):
        self.chess_board.update_board()

    def resizeEvent(self, event):
        h = self.height() - 120 
        self.chess_board.setFixedWidth(h)
        self.chess_board.setFixedHeight(h)
        super().resizeEvent(event)
