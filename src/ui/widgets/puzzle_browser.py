from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                             QPushButton, QFrame, QLineEdit, QListWidget, QListWidgetItem, QSplitter, QSlider, QScrollArea, QComboBox, QMenu)
from PySide6.QtCore import Qt, Signal, QTimer, QSize
from PySide6.QtGui import QColor, QFont, QPalette, QAction
import chess
import os
import polars as pl
import qtawesome as qta
from src.ui.board import ChessBoard
from src.core.game_controller import GameController
from src.core.puzzle_manager import PuzzleManager
from src.config import PUZZLE_FILE, logger

# Traducción de etiquetas técnicas a lenguaje humano
THEME_MAP = {
    "mate": "👑 Mate",
    "fork": "🍴 Ataque Doble",
    "pin": "📌 Clavada",
    "skewer": "🏹 Enfilada",
    "sacrifice": "🎁 Sacrificio",
    "attraction": "🧲 Atracción",
    "deflection": "🔄 Desviación",
    "discoveredAttack": "🎭 Descubierta",
    "zugzwang": "⏳ Zugzwang",
    "endgame": "🏁 Final",
    "advancedPawn": "♟️ Peón Avanzado",
    "backRankMate": "🪜 Mate de Pasillo",
    "trappedPiece": "🕸️ Pieza Atrapada",
    "defensiveMove": "🛡️ Jugada Defensiva",
    "crushing": "💥 Aplastante",
    "advantage": "📈 Ventaja"
}

class MultiThemeSelector(QPushButton):
    """Un botón que abre un menú de multi-selección de temas"""
    themes_changed = Signal(list)

    def __init__(self, themes_dict, parent=None):
        super().__init__("Seleccionar Temas...", parent)
        self.themes_dict = themes_dict
        self.selected_themes = []
        self.setFixedWidth(180)
        self.setMenu(QMenu(self))
        self.setStyleSheet("QPushButton { text-align: left; padding: 5px; background: #fff; border: 1px solid #ddd; }")
        self.init_menu()

    def init_menu(self):
        menu = self.menu()
        for tag, human in self.themes_dict.items():
            action = QAction(human, menu)
            action.setCheckable(True)
            action.setData(tag)
            action.triggered.connect(self.on_item_toggled)
            menu.addAction(action)

    def on_item_toggled(self):
        self.selected_themes = [a.data() for a in self.menu().actions() if a.isChecked()]
        count = len(self.selected_themes)
        if count == 0: self.setText("Seleccionar Temas...")
        elif count == 1: self.setText(self.themes_dict[self.selected_themes[0]])
        else: self.setText(f"{count} temas activos")
        self.themes_changed.emit(self.selected_themes)

class PuzzleListItemWidget(QWidget):
    def __init__(self, row, status, parent_browser):
        super().__init__()
        layout = QHBoxLayout(self); layout.setContentsMargins(5, 2, 5, 2)
        self.label_status = QLabel()
        icon = 'fa5s.star'
        color = '#2e7d32' if status == "success" else ('#c62828' if status == "fail" else '#ccc')
        self.label_status.setPixmap(qta.icon(icon, color=color).pixmap(14, 14))
        layout.addWidget(self.label_status)
        elo_label = QLabel(f"<b>{row['Rating']}</b>"); elo_label.setFixedWidth(35); layout.addWidget(elo_label)
        layout.addSpacing(5)
        # Mostrar etiquetas humanizadas si es posible
        tags = row["Themes"].split()
        for t in tags:
            human = THEME_MAP.get(t, t)
            chip = QLabel(human); chip.setStyleSheet("background: #f0f0f0; font-size: 8px; padding: 1px 4px; border-radius: 3px; color: #777; border: 1px solid #ddd;")
            layout.addWidget(chip)
        layout.addStretch()

class PuzzleBrowserWidget(QWidget):
    def __init__(self, parent_main=None):
        super().__init__(parent_main)
        self.parent_main = parent_main
        self.game = GameController(); self.manager = None; self.puzzle_df = None; self.current_puzzle = None; self.current_index = 0; self.solution_idx = 0; self.has_failed_current = False; self.hint_level = 0; self.batch_size = 50; self.loaded_count = 0; self.filter_status = "all"; self.filter_themes = []
        self.filter_timer = QTimer(); self.filter_timer.setSingleShot(True); self.filter_timer.timeout.connect(self.apply_filters)
        self.game.position_changed.connect(self.update_ui); self.init_ui()
        if os.path.exists(PUZZLE_FILE): self.load_db(PUZZLE_FILE)

    def init_ui(self):
        layout = QVBoxLayout(self); layout.setContentsMargins(0, 0, 0, 0); layout.setSpacing(0)
        top_bar = QFrame(); top_bar.setStyleSheet("background: #f8f9fa; border-bottom: 1px solid #ddd;"); top_layout = QHBoxLayout(top_bar)
        
        # Elo
        vl_elo = QVBoxLayout(); saved_elo = 1200
        if self.parent_main and hasattr(self.parent_main, 'app_db'): saved_elo = self.parent_main.app_db.get_config("puzzle_slider_elo", 1200)
        self.label_elo_val = QLabel(f"Dificultad: <b>{saved_elo}</b>"); self.slider_elo = QSlider(Qt.Horizontal); self.slider_elo.setRange(400, 3000); self.slider_elo.setValue(saved_elo); self.slider_elo.setFixedWidth(120); self.slider_elo.valueChanged.connect(self.on_slider_changed); vl_elo.addWidget(self.label_elo_val); vl_elo.addWidget(self.slider_elo); top_layout.addLayout(vl_elo)
        
        # Selector de Temas Humanizado
        top_layout.addSpacing(15); top_layout.addWidget(QLabel("Temas:"))
        self.theme_selector = MultiThemeSelector(THEME_MAP)
        self.theme_selector.themes_changed.connect(self.set_themes_filter)
        top_layout.addWidget(self.theme_selector)
        
        # Estado
        top_layout.addSpacing(15); self.btn_pending = QPushButton(qta.icon('fa5s.star', color='#888'), ""); self.btn_success = QPushButton(qta.icon('fa5s.star', color='#2e7d32'), ""); self.btn_fail = QPushButton(qta.icon('fa5s.star', color='#c62828'), "")
        for btn in [self.btn_pending, self.btn_success, self.btn_fail]:
            btn.setCheckable(True); btn.setFixedWidth(30); btn.clicked.connect(self.on_status_filter_clicked); top_layout.addWidget(btn)
            
        top_layout.addStretch(); self.btn_hint = QPushButton(qta.icon('fa5s.lightbulb', color='#fbc02d'), " Pista"); self.btn_hint.clicked.connect(self.give_hint); top_layout.addWidget(self.btn_hint); layout.addWidget(top_bar)

        # Cuerpo
        self.splitter = QSplitter(Qt.Horizontal); self.chess_board = ChessBoard(self.game.board, self)
        if self.parent_main: self.chess_board.color_light, self.chess_board.color_dark = self.parent_main.board_ana.color_light, self.parent_main.board_ana.color_dark
        self.chess_board.piece_moved.connect(self.check_move); self.splitter.addWidget(self.chess_board)
        right_panel = QWidget(); rp_layout = QVBoxLayout(right_panel); rp_layout.setContentsMargins(0,0,0,0); self.list_view = QListWidget(); self.list_view.itemClicked.connect(self.on_item_selected); self.list_view.verticalScrollBar().valueChanged.connect(self.on_scroll); rp_layout.addWidget(self.list_view, 3)
        self.dash_frame = QFrame(); self.dash_frame.setStyleSheet("background: #fff; border-top: 1px solid #ddd;"); dash_l = QVBoxLayout(self.dash_frame); self.label_elo_tactico = QLabel("ELO TÁCTICO: 1200"); self.label_elo_tactico.setStyleSheet("font-size: 16px; font-weight: bold; color: #1976d2;"); dash_l.addWidget(self.label_elo_tactico, 0, Qt.AlignCenter)
        stats_row = QHBoxLayout(); self.val_total = QLabel("0"); self.val_success = QLabel("0"); self.val_fail = QLabel("0")
        for label, val, color in [("Total", self.val_total, "#333"), ("Éxito", self.val_success, "#2e7d32"), ("Fallo", self.val_fail, "#c62828")]:
            v = QVBoxLayout(); l = QLabel(label); l.setStyleSheet("font-size: 9px; color: #888;"); v.addWidget(l, 0, Qt.AlignCenter); val.setStyleSheet(f"font-size: 14px; font-weight: bold; color: {color};"); v.addWidget(val, 0, Qt.AlignCenter); stats_row.addLayout(v)
        dash_l.addLayout(stats_row); rp_layout.addWidget(self.dash_frame, 1); self.splitter.addWidget(right_panel); self.splitter.setStretchFactor(0, 3); self.splitter.setStretchFactor(1, 1); layout.addWidget(self.splitter)
        self.label_feedback = QLabel("Selecciona un ejercicio"); self.label_feedback.setStyleSheet("font-size: 14px; font-weight: bold; background: #333; color: #fff; padding: 10px;"); self.label_feedback.setAlignment(Qt.AlignCenter); layout.addWidget(self.label_feedback)

    def set_themes_filter(self, themes_list):
        self.filter_themes = themes_list; self.apply_filters()

    def update_dashboard(self):
        if not self.parent_main or not hasattr(self.parent_main, 'app_db'): return
        stats = self.parent_main.app_db.get_all_puzzle_stats(); success = sum(1 for s in stats.values() if s == "success"); fail = sum(1 for s in stats.values() if s == "fail")
        self.val_total.setText(str(len(stats))); self.val_success.setText(str(success)); self.val_fail.setText(str(fail))
        elo = self.parent_main.app_db.get_tactical_elo(); self.label_elo_tactico.setText(f"ELO TÁCTICO: {elo}")

    def apply_filters(self):
        if not self.manager: return
        target_elo = self.slider_elo.value(); theme_query = " ".join(self.filter_themes)
        self.manager.apply_filters(min_rating=target_elo-100, max_rating=target_elo+100, theme=theme_query)
        full_set = self.manager.get_sample()
        saved_stats = self.parent_main.app_db.get_all_puzzle_stats() if self.parent_main else {}
        statuses = [saved_stats.get(str(pid), "pending") for pid in full_set["PuzzleId"]]
        full_set = full_set.with_columns(pl.Series("status", statuses))
        if self.filter_status != "all": full_set = full_set.filter(pl.col("status") == self.filter_status)
        self.puzzle_df = full_set; self.loaded_count = 0; self.list_view.clear(); self.load_more_puzzles(); self.update_dashboard()
        if self.puzzle_df.height > 0: self.load_puzzle_by_index(0)

    def on_slider_changed(self, val):
        self.label_elo_val.setText(f"Dificultad: <b>{val}</b>")
        if self.parent_main: self.parent_main.app_db.set_config("puzzle_slider_elo", val)
        self.trigger_filter()

    def trigger_filter(self): self.filter_timer.start(300)

    def on_status_filter_clicked(self):
        sender = self.sender()
        if sender.isChecked():
            for b in [self.btn_pending, self.btn_success, self.btn_fail]: 
                if b != sender: b.setChecked(False)
            if sender == self.btn_pending: self.filter_status = "pending"
            elif sender == self.btn_success: self.filter_status = "success"
            elif sender == self.btn_fail: self.filter_status = "fail"
        else: self.filter_status = "all"
        self.apply_filters()

    def start_puzzle(self, puzzle):
        self.current_puzzle = puzzle; self.solution_idx = 0; self.has_failed_current = False; self.hint_level = 0
        self.chess_board.highlighted_square = None; self.chess_board.set_engine_move(None)
        self.game.board.set_fen(puzzle["initial_fen"]); self.game.full_mainline = []; self.game.current_idx = 0
        self.game.make_move(chess.Move.from_uci(puzzle["opponent_move"]))
        is_white = self.game.board.turn == chess.WHITE; self.chess_board.flipped = not is_white
        self.label_feedback.setText("⚪ Juegan BLANCAS" if is_white else "⚫ Juegan NEGRAS")
        self.label_feedback.setStyleSheet("background: #1976d2; color: #fff; padding: 10px;"); self.chess_board.setEnabled(True); self.chess_board.update_board()

    def check_move(self, move_uci):
        if not self.current_puzzle: return
        correct = self.current_puzzle["solution"][self.solution_idx]
        if move_uci == correct:
            self.solution_idx += 1
            if self.solution_idx >= len(self.current_puzzle["solution"]):
                self.label_feedback.setText("¡RESUELTO! 🏆"); self.label_feedback.setStyleSheet("background: #2e7d32; color: #fff; padding: 10px;")
                self.chess_board.setEnabled(False); self.update_elo(True)
                QTimer.singleShot(2000, self.load_next_puzzle)
            else:
                self.game.make_move(chess.Move.from_uci(self.current_puzzle["solution"][self.solution_idx]))
                self.solution_idx += 1; self.label_feedback.setText("¡Bien! Sigue...")
        else:
            self.has_failed_current = True; self.chess_board.setEnabled(False); self.label_feedback.setText("Analizando error...")
            from src.core.workers import RefutationWorker
            self._ref_worker = RefutationWorker(self.parent_main.engine_path, self.game.board.fen())
            self._ref_worker.finished.connect(self.on_refutation_ready); self._ref_worker.start(); self.update_elo(False)

    def on_refutation_ready(self, move_uci, msg):
        if move_uci:
            self.chess_board.set_engine_move(move_uci); self.game.board.push_uci(move_uci); self.chess_board.update_board()
            self.label_feedback.setText(f"Incorrecto. {msg}"); self.label_feedback.setStyleSheet("background: #c62828; color: #fff; padding: 10px;")
        QTimer.singleShot(2000, self.reset_to_last_correct)

    def reset_to_last_correct(self):
        self.game.board.pop(); self.game.board.pop(); self.chess_board.set_engine_move(None); self.chess_board.setEnabled(True); self.chess_board.update_board()
        self.label_feedback.setText("Inténtalo de nuevo..."); self.label_feedback.setStyleSheet("background: #1976d2; color: #fff; padding: 10px;")

    def update_elo(self, success):
        if not self.parent_main: return
        
        my_elo = self.parent_main.app_db.get_tactical_elo()
        puzzle_elo = self.current_puzzle.get("rating", 1200)
        
        # 1. Calcular la probabilidad esperada de éxito (Fórmula Elo)
        # E = 1 / (1 + 10^((puzzle_elo - my_elo) / 400))
        exponent = (puzzle_elo - my_elo) / 400.0
        expected_score = 1 / (1 + 10**exponent)
        
        # 2. Determinar el resultado real
        # Si acertó a la primera: 1.0. Si falló o usó pistas: proporcional.
        if success and not self.has_failed_current:
            if self.hint_level == 0: actual_score = 1.0
            else: actual_score = 0.7 # Penalización suave por pistas
        else:
            actual_score = 0.0
            
        # 3. Calcular la variación (K-Factor de 32 para ajuste rápido)
        k_factor = 32
        variation = int(k_factor * (actual_score - expected_score))
        
        # Asegurar un mínimo de +1 o -1 para que siempre haya movimiento
        if variation == 0:
            variation = 1 if actual_score > 0.5 else -1
            
        new_elo = max(100, my_elo + variation) # El Elo no baja de 100
        
        self.parent_main.app_db.set_tactical_elo(new_elo)
        self.update_status("success" if success and not self.has_failed_current else "fail")

    def update_status(self, status):
        p_id = str(self.current_puzzle["id"])
        if self.parent_main:
            from src.core.workers import PuzzleSaveWorker
            self._sw = PuzzleSaveWorker(self.parent_main.app_db, p_id, status); self._sw.start()
        self.update_dashboard()

    def give_hint(self):
        if not self.current_puzzle: return
        self.hint_level += 1; move = chess.Move.from_uci(self.current_puzzle["solution"][self.solution_idx])
        if self.hint_level == 1: self.chess_board.highlighted_square = move.from_square; self.label_feedback.setText("💡 Pista: Pieza a mover")
        else: self.chess_board.highlighted_square = move.to_square; self.label_feedback.setText("💡 Pista: Casilla de destino")
        self.chess_board.update_board()

    def load_next_puzzle(self):
        if self.current_index + 1 < self.puzzle_df.height: self.load_puzzle_by_index(self.current_index + 1)

    def load_db(self, path): self.manager = PuzzleManager(path); self.apply_filters()
    def load_more_puzzles(self):
        if self.puzzle_df is None: return
        s = self.loaded_count; e = min(s + self.batch_size, self.puzzle_df.height)
        for i in range(s, e):
            row = self.puzzle_df.row(i, named=True); it = QListWidgetItem(); it.setSizeHint(QSize(0, 35)); it.setData(Qt.UserRole, row)
            self.list_view.addItem(it); self.list_view.setItemWidget(it, PuzzleListItemWidget(row, row.get("status", "pending"), self))
        self.loaded_count = e
    def on_item_selected(self, it): self.current_index = self.list_view.row(it); self.start_puzzle(self.manager.prepare_puzzle_data(it.data(Qt.UserRole)))
    def load_puzzle_by_index(self, i):
        it = self.list_view.item(i)
        if it: self.current_index = i; self.start_puzzle(self.manager.prepare_puzzle_data(it.data(Qt.UserRole)))
    def update_ui(self): self.chess_board.update_board()
    def on_scroll(self, v):
        if v > self.list_view.verticalScrollBar().maximum() - 20: self.load_more_puzzles()
    def resizeEvent(self, e):
        h = self.height() - 120; self.chess_board.setFixedSize(h, h); super().resizeEvent(e)
