from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                             QPushButton, QFrame, QLineEdit, QListWidget, QListWidgetItem, QSplitter, QSlider)
from PySide6.QtCore import Qt, Signal, QTimer, QSize
from PySide6.QtGui import QColor, QFont, QPalette
import chess
import os
import polars as pl
import qtawesome as qta
from src.ui.board import ChessBoard
from src.core.game_controller import GameController
from src.core.puzzle_manager import PuzzleManager
from src.config import PUZZLE_FILE, logger

class ThemeChip(QPushButton):
    clicked_theme = Signal(str)
    def __init__(self, text):
        super().__init__(text)
        self.theme_text = text
        self.setCursor(Qt.PointingHandCursor)
        self.setFocusPolicy(Qt.NoFocus) # Evita que robe el foco al clicar el item
        self.setStyleSheet("""
            QPushButton {
                background-color: #e9ecef;
                color: #495057;
                border: 1px solid #dee2e6;
                border-radius: 10px;
                padding: 10px 8px;
                font-size: 10px;
            }
            QPushButton:hover {
                background-color: #dee2e6;
                border-color: #ced4da;
            }
        """)
        self.clicked.connect(lambda: self.clicked_theme.emit(self.theme_text))

class PuzzleListItemWidget(QWidget):
    def __init__(self, row, status, parent_browser):
        super().__init__()
        layout = QHBoxLayout(self)
        layout.setContentsMargins(5, 2, 5, 2)
        
        # Estrella y ELO (Izquierda)
        self.label_status = QLabel()
        icon = 'fa5s.star'
        color = '#2e7d32' if status == "success" else ('#c62828' if status == "fail" else '#ccc')
        self.label_status.setPixmap(qta.icon(icon, color=color).pixmap(16, 16))
        layout.addWidget(self.label_status)
        
        elo_label = QLabel(f"<b>{row['Rating']}</b>")
        elo_label.setFixedWidth(40)
        layout.addWidget(elo_label)
        
        layout.addSpacing(10)
        
        # Chips (Derecha, con scroll horizontal si fuera necesario, pero aquí solo 3)
        self.chips_layout = QHBoxLayout()
        self.chips_layout.setSpacing(4)
        themes = row["Themes"].split()[:3] 
        for t in themes:
            chip = ThemeChip(t)
            chip.clicked_theme.connect(parent_browser.set_theme_filter)
            self.chips_layout.addWidget(chip)
        
        layout.addLayout(self.chips_layout)
        layout.addStretch()

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
        self.batch_size = 50
        self.loaded_count = 0
        self.filter_status = "all" 
        
        self.filter_timer = QTimer()
        self.filter_timer.setSingleShot(True)
        self.filter_timer.timeout.connect(self.apply_filters)
        
        self.game.position_changed.connect(self.update_ui)
        self.init_ui()
        if os.path.exists(PUZZLE_FILE):
            self.load_db(PUZZLE_FILE)

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # --- PANEL DE FILTROS SUPERIOR ---
        filter_panel = QFrame()
        filter_panel.setStyleSheet("background: #f8f9fa; border-bottom: 1px solid #ddd;")
        fl = QHBoxLayout(filter_panel)
        
        vl_elo = QVBoxLayout()
        saved_elo = 600
        if self.parent_main and hasattr(self.parent_main, 'app_db'):
            saved_elo = self.parent_main.app_db.get_config("puzzle_slider_elo", 600)

        self.label_elo_val = QLabel(f"ELO Objetivo: <b>{saved_elo}</b> (±50)")
        self.slider_elo = QSlider(Qt.Horizontal)
        self.slider_elo.setRange(400, 3000)
        self.slider_elo.setValue(saved_elo)
        self.slider_elo.setFixedWidth(200)
        self.slider_elo.setTracking(False)
        self.slider_elo.valueChanged.connect(self.on_slider_changed)
        self.slider_elo.sliderMoved.connect(lambda v: self.label_elo_val.setText(f"ELO Objetivo: <b>{v}</b> (±50)"))
        
        vl_elo.addWidget(self.label_elo_val)
        vl_elo.addWidget(self.slider_elo)
        fl.addLayout(vl_elo)
        
        fl.addSpacing(15)
        fl.addWidget(QLabel("Tema:"))
        self.edit_theme = QLineEdit()
        self.edit_theme.setPlaceholderText("Filtrar tema...")
        self.edit_theme.textChanged.connect(self.trigger_filter)
        fl.addWidget(self.edit_theme)
        
        self.btn_clear_theme = QPushButton(qta.icon('fa5s.times'), "")
        self.btn_clear_theme.setFixedWidth(25)
        self.btn_clear_theme.clicked.connect(lambda: self.edit_theme.clear())
        fl.addWidget(self.btn_clear_theme)
        
        fl.addSpacing(15)
        fl.addWidget(QLabel("Estado:"))
        self.btn_all = QPushButton(qta.icon('fa5s.layer-group'), "")
        self.btn_pending = QPushButton(qta.icon('fa5s.star', color='#888'), "")
        self.btn_success = QPushButton(qta.icon('fa5s.star', color='#2e7d32'), "")
        self.btn_fail = QPushButton(qta.icon('fa5s.star', color='#c62828'), "")
        
        self.status_btns = [self.btn_all, self.btn_pending, self.btn_success, self.btn_fail]
        for btn in self.status_btns:
            btn.setCheckable(True); btn.setFixedWidth(35)
            btn.clicked.connect(self.on_status_filter_clicked)
            fl.addWidget(btn)
        self.btn_all.setChecked(True)
            
        fl.addSpacing(20)
        self.btn_hint = QPushButton(qta.icon('fa5s.lightbulb', color='#fbc02d'), " Pista")
        self.btn_hint.clicked.connect(self.give_hint)
        fl.addWidget(self.btn_hint)
            
        fl.addStretch()
        layout.addWidget(filter_panel)

        # --- CUERPO CENTRAL (Splitter) ---
        self.splitter = QSplitter(Qt.Horizontal)
        
        # IZQUIERDA: Tablero
        self.chess_board = ChessBoard(self.game.board, self)
        if self.parent_main:
            self.chess_board.color_light = self.parent_main.board_ana.color_light
            self.chess_board.color_dark = self.parent_main.board_ana.color_dark
        self.chess_board.piece_moved.connect(self.check_move)
        self.splitter.addWidget(self.chess_board)
        
        # DERECHA: Lista y Dashboard
        right_panel = QWidget(); rp_layout = QVBoxLayout(right_panel); rp_layout.setContentsMargins(0,0,0,0)
        
        self.list_view = QListWidget()
        self.list_view.itemClicked.connect(self.on_item_selected)
        self.list_view.verticalScrollBar().valueChanged.connect(self.on_scroll)
        rp_layout.addWidget(self.list_view, 3) # Lista arriba
        
        # Dashboard de progreso (Nuevo)
        self.dash_frame = QFrame()
        self.dash_frame.setStyleSheet("background: #fff; border-top: 1px solid #ddd;")
        dash_l = QVBoxLayout(self.dash_frame)
        
        self.label_dash_title = QLabel("📊 TU PROGRESO")
        self.label_dash_title.setStyleSheet("font-weight: bold; color: #555; font-size: 11px;")
        dash_l.addWidget(self.label_dash_title)
        
        stats_row = QHBoxLayout()
        self.label_stat_total = self.create_stat_widget("Total", "0")
        self.label_stat_success = self.create_stat_widget("Éxitos", "0", "#2e7d32")
        self.label_stat_fail = self.create_stat_widget("Fallos", "0", "#c62828")
        stats_row.addLayout(self.label_stat_total); stats_row.addLayout(self.label_stat_success); stats_row.addLayout(self.label_stat_fail)
        dash_l.addLayout(stats_row)
        
        rp_layout.addWidget(self.dash_frame, 1) # Dashboard abajo
        
        self.splitter.addWidget(right_panel)
        self.splitter.setStretchFactor(0, 3)
        self.splitter.setStretchFactor(1, 1)
        layout.addWidget(self.splitter)

        self.label_feedback = QLabel("Cargando base...")
        self.label_feedback.setStyleSheet("font-size: 14px; font-weight: bold; background: #333; color: #fff; padding: 10px;")
        self.label_feedback.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.label_feedback)

    def create_stat_widget(self, label, value, color="#333"):
        v = QVBoxLayout()
        l = QLabel(label); l.setStyleSheet("font-size: 9px; color: #888; text-transform: uppercase;")
        val = QLabel(value); val.setStyleSheet(f"font-size: 18px; font-weight: bold; color: {color};")
        v.addWidget(l); v.addWidget(val)
        # Guardamos la referencia para actualizarla
        setattr(self, f"val_{label.lower()}", val)
        return v

    def update_dashboard(self):
        if not self.parent_main or not hasattr(self.parent_main, 'app_db'): return
        stats = self.parent_main.app_db.get_all_puzzle_stats()
        success = sum(1 for s in stats.values() if s == "success")
        fail = sum(1 for s in stats.values() if s == "fail")
        
        self.val_total.setText(str(len(stats)))
        self.val_éxitos.setText(str(success))
        self.val_fallos.setText(str(fail))

    def set_theme_filter(self, theme):
        current = self.edit_theme.text().strip()
        if not current: self.edit_theme.setText(theme)
        elif theme not in current: self.edit_theme.setText(f"{current} {theme}")
        self.trigger_filter()

    def on_status_filter_clicked(self):
        sender = self.sender()
        for btn in self.status_btns: btn.setChecked(False)
        sender.setChecked(True)
        if sender == self.btn_all: self.filter_status = "all"
        elif sender == self.btn_pending: self.filter_status = "pending"
        elif sender == self.btn_success: self.filter_status = "success"
        elif sender == self.btn_fail: self.filter_status = "fail"
        self.apply_filters()

    def give_hint(self):
        if not self.current_puzzle: return
        try:
            correct_move_uci = self.current_puzzle["solution"][self.solution_idx]
            correct_move = chess.Move.from_uci(correct_move_uci)
            self.chess_board.highlighted_square = correct_move.from_square
            self.chess_board.update_board()
            self.label_feedback.setText("💡 ¡Pista! Te he señalado la pieza que debes mover.")
        except: pass

    def apply_filters(self):
        if not self.manager: return
        try:
            target_elo = self.slider_elo.value()
            theme = self.edit_theme.text().strip()
            self.manager.apply_filters(min_rating=target_elo-50, max_rating=target_elo+50, theme=theme)
            full_set = self.manager.get_sample()
            
            saved_stats = {}
            if self.parent_main and hasattr(self.parent_main, 'app_db'):
                saved_stats = self.parent_main.app_db.get_all_puzzle_stats()
            
            statuses = [saved_stats.get(str(pid), "pending") for pid in full_set["PuzzleId"]]
            full_set = full_set.with_columns(pl.Series("status", statuses))
            if self.filter_status != "all":
                full_set = full_set.filter(pl.col("status") == self.filter_status)
            
            self.puzzle_df = full_set
            self.loaded_count = 0
            self.list_view.clear()
            self.load_more_puzzles()
            self.label_feedback.setText(f"Encontrados {self.puzzle_df.height} puzzles.")
            self.update_dashboard() # Refrescar stats del panel derecho
            
            if self.puzzle_df.height > 0:
                self.load_puzzle_by_index(0)
        except Exception as e:
            self.label_feedback.setText(f"Error: {e}")

    def on_slider_changed(self, val):
        if self.parent_main and hasattr(self.parent_main, 'app_db'):
            self.parent_main.app_db.set_config("puzzle_slider_elo", val)
        self.trigger_filter()

    def trigger_filter(self):
        self.filter_timer.start(300)

    def on_scroll(self, value):
        scrollbar = self.list_view.verticalScrollBar()
        if value > scrollbar.maximum() - 20: self.load_more_puzzles()

    def load_db(self, path):
        try:
            self.manager = PuzzleManager(path)
            self.apply_filters()
        except Exception as e:
            self.label_feedback.setText(f"Error: {e}")

    def load_more_puzzles(self):
        if self.puzzle_df is None: return
        start = self.loaded_count
        end = min(start + self.batch_size, self.puzzle_df.height)
        if start >= end: return 
        
        for i in range(start, end):
            row = self.puzzle_df.row(i, named=True)
            status = row.get("status", "pending")
            it = QListWidgetItem(); it.setSizeHint(QSize(0, 35)); it.setData(Qt.UserRole, row)
            widget = PuzzleListItemWidget(row, status, self)
            self.list_view.addItem(it)
            self.list_view.setItemWidget(it, widget)
        self.loaded_count = end

    def on_item_selected(self, item):
        self.current_index = self.list_view.row(item)
        row = item.data(Qt.UserRole)
        puzzle = self.manager.prepare_puzzle_data(row)
        self.start_puzzle(puzzle)

    def load_puzzle_by_index(self, idx):
        if self.puzzle_df is None or idx >= self.puzzle_df.height: return
        self.current_index = idx
        item = self.list_view.item(idx)
        if item:
            row = item.data(Qt.UserRole)
            puzzle = self.manager.prepare_puzzle_data(row)
            self.start_puzzle(puzzle)

    def start_puzzle(self, puzzle):
        self.current_puzzle = puzzle
        self.solution_idx = 0
        self.has_failed_current = False
        self.chess_board.highlighted_square = None
        self.game.board.set_fen(puzzle["initial_fen"])
        self.game.full_mainline = []
        self.game.current_idx = 0
        opp_move = chess.Move.from_uci(puzzle["opponent_move"])
        self.game.make_move(opp_move)
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
        item = self.list_view.item(self.current_index)
        if item:
            widget = self.list_view.itemWidget(item)
            if widget:
                color = '#2e7d32' if status == "success" else '#c62828'
                widget.label_status.setPixmap(qta.icon('fa5s.star', color=color).pixmap(16, 16))
        
        if self.parent_main and hasattr(self.parent_main, 'app_db'):
            from src.core.workers import PuzzleSaveWorker
            p_id = str(self.current_puzzle["id"])
            self._save_worker = PuzzleSaveWorker(self.parent_main.app_db, p_id, status)
            self._save_worker.start()
            self.update_dashboard() # Actualizar números globales

    def update_ui(self):
        self.chess_board.update_board()

    def resizeEvent(self, event):
        h = self.height() - 120 
        self.chess_board.setFixedWidth(h)
        self.chess_board.setFixedHeight(h)
        super().resizeEvent(event)

    def statusBar_msg(self, msg):
        if self.parent_main: self.parent_main.statusBar().showMessage(msg, 5000)
