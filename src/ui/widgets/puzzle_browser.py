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

# Mapeo de temas humanizados
THEME_MAP = {
    "mate": "👑 Mate", "fork": "🍴 Ataque Doble", "pin": "📌 Clavada", "skewer": "🏹 Enfilada", "sacrifice": "🎁 Sacrificio",
    "attraction": "🧲 Atracción", "deflection": "🔄 Desviación", "discoveredAttack": "🎭 Descubierta", "zugzwang": "⏳ Zugzwang",
    "endgame": "🏁 Final", "advancedPawn": "♟️ Peón Avanzado", "backRankMate": "🪜 Mate de Pasillo", "trappedPiece": "🕸️ Pieza Atrapada",
    "defensiveMove": "🛡️ Jugada Defensiva", "crushing": "💥 Aplastante", "advantage": "📈 Ventaja"
}

class MultiThemeSelector(QPushButton):
    themes_changed = Signal(list)
    def __init__(self, themes_dict, parent=None):
        super().__init__("Seleccionar Temas...", parent); self.themes_dict = themes_dict; self.selected_themes = []
        self.setFixedWidth(180); self.setMenu(QMenu(self)); self.setStyleSheet("QPushButton { text-align: left; padding: 5px; background: #fff; border: 1px solid #ddd; }"); self.init_menu()
    def init_menu(self):
        menu = self.menu()
        for tag, human in self.themes_dict.items():
            action = QAction(human, menu); action.setCheckable(True); action.setData(tag); action.triggered.connect(self.on_item_toggled); menu.addAction(action)
    def on_item_toggled(self):
        self.selected_themes = [a.data() for a in self.menu().actions() if a.isChecked()]; count = len(self.selected_themes)
        if count == 0: self.setText("Seleccionar Temas...")
        elif count == 1: self.setText(self.themes_dict[self.selected_themes[0]])
        else: self.setText(f"{count} temas activos")
        self.themes_changed.emit(self.selected_themes)

class PuzzleListItemWidget(QWidget):
    def __init__(self, row, status, parent_browser):
        super().__init__()
        layout = QHBoxLayout(self); layout.setContentsMargins(5, 2, 5, 2)
        self.label_status = QLabel(); icon = 'fa5s.star'; color = '#2e7d32' if status == "success" else ('#c62828' if status == "fail" else '#ccc')
        self.label_status.setPixmap(qta.icon(icon, color=color).pixmap(14, 14)); layout.addWidget(self.label_status)
        elo_label = QLabel(f"<b>{row['Rating']}</b>"); elo_label.setFixedWidth(35); layout.addWidget(elo_label); layout.addSpacing(5)
        for t in row["Themes"].split():
            human = THEME_MAP.get(t, t); chip = QLabel(human); chip.setStyleSheet("background: #f0f0f0; font-size: 8px; padding: 1px 4px; border-radius: 3px; color: #777; border: 1px solid #ddd;"); layout.addWidget(chip)
        layout.addStretch()

class PuzzleBrowserWidget(QWidget):
    def __init__(self, parent_main=None):
        super().__init__(parent_main); self.parent_main = parent_main; self.game = GameController(); self.manager = None; self.puzzle_df = None; self.current_puzzle = None; self.current_index = 0; self.solution_idx = 0; self.has_failed_current = False; self.hint_level = 0; self.batch_size = 50; self.loaded_count = 0; self.filter_status = "all"; self.filter_themes = []
        self.filter_timer = QTimer(); self.filter_timer.setSingleShot(True); self.filter_timer.timeout.connect(self.apply_filters); self.game.position_changed.connect(self.update_ui); self.init_ui()
        if os.path.exists(PUZZLE_FILE): self.load_db(PUZZLE_FILE)

    def init_ui(self):
        layout = QVBoxLayout(self); layout.setContentsMargins(0, 0, 0, 0); layout.setSpacing(0)
        top_bar = QFrame(); top_bar.setStyleSheet("background: #f8f9fa; border-bottom: 1px solid #ddd;"); top_layout = QHBoxLayout(top_bar)
        
        vl_elo = QVBoxLayout(); saved_elo = 1200; self.label_elo_val = QLabel(f"Dificultad: <b>{saved_elo}</b>"); self.slider_elo = QSlider(Qt.Horizontal); self.slider_elo.setRange(400, 3000); self.slider_elo.setValue(saved_elo); self.slider_elo.setFixedWidth(120); self.slider_elo.valueChanged.connect(self.on_slider_changed); vl_elo.addWidget(self.label_elo_val); vl_elo.addWidget(self.slider_elo); top_layout.addLayout(vl_elo)
        top_layout.addSpacing(15); top_layout.addWidget(QLabel("Temas:")); self.theme_selector = MultiThemeSelector(THEME_MAP); self.theme_selector.themes_changed.connect(self.set_themes_filter); top_layout.addWidget(self.theme_selector)
        
        top_layout.addSpacing(15); self.btn_pending = QPushButton(qta.icon('fa5s.star', color='#888'), ""); self.btn_success = QPushButton(qta.icon('fa5s.star', color='#2e7d32'), ""); self.btn_fail = QPushButton(qta.icon('fa5s.star', color='#c62828'), "")
        for btn in [self.btn_pending, self.btn_success, self.btn_fail]:
            btn.setCheckable(True); btn.setFixedWidth(30); btn.clicked.connect(self.on_status_filter_clicked); top_layout.addWidget(btn)
            
        top_layout.addStretch()
        # BOTONES DE AYUDA (TOGGLE)
        self.btn_hint_tension = QPushButton(qta.icon('fa5s.eye', color='#ff9800'), ""); self.btn_hint_piece = QPushButton(qta.icon('fa5s.chess-pawn', color='#2196f3'), ""); self.btn_hint_dest = QPushButton(qta.icon('fa5s.bullseye', color='#2196f3'), "")
        for btn, tip, func in [(self.btn_hint_tension, "Ver conflictos", self.toggle_hint_tension), (self.btn_hint_piece, "Ver pieza a mover", self.toggle_hint_piece), (self.btn_hint_dest, "Ver destino", self.toggle_hint_dest)]:
            btn.setCheckable(True); btn.setFixedWidth(30); btn.setToolTip(tip); btn.clicked.connect(func); top_layout.addWidget(btn)
        layout.addWidget(top_bar)

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

    def toggle_hint_tension(self):
        if self.btn_hint_tension.isChecked():
            self.chess_board.tension_squares = self.get_board_tensions()
            self.label_feedback.setText("💡 Conflictos visualizados.")
            self.hint_level = max(self.hint_level, 1)
        else: self.chess_board.tension_squares = []
        self.chess_board.update_board()

    def toggle_hint_piece(self):
        if self.btn_hint_piece.isChecked():
            move = chess.Move.from_uci(self.current_puzzle["solution"][self.solution_idx])
            self.chess_board.highlighted_square = move.from_square; self.btn_hint_dest.setChecked(False)
            self.label_feedback.setText("💡 Pieza a mover señalada."); self.hint_level = max(self.hint_level, 2)
        else: self.chess_board.highlighted_square = None
        self.chess_board.update_board()

    def toggle_hint_dest(self):
        if self.btn_hint_dest.isChecked():
            move = chess.Move.from_uci(self.current_puzzle["solution"][self.solution_idx])
            self.chess_board.highlighted_square = move.to_square; self.btn_hint_piece.setChecked(False)
            self.label_feedback.setText("💡 Casilla de destino señalada."); self.hint_level = max(self.hint_level, 3)
        else: self.chess_board.highlighted_square = None
        self.chess_board.update_board()

    def get_board_tensions(self):
        """Lógica de Lupa Táctica: Resalta solo las piezas involucradas en el motivo táctico"""
        if not self.current_puzzle: return []
        board = self.game.board.copy()
        
        try:
            # 1. Analizar la primera jugada de la solución
            move_uci = self.current_puzzle["solution"][self.solution_idx]
            move = chess.Move.from_uci(move_uci)
            
            tensions = set()
            # La pieza que mueve y la casilla a la que va son fundamentales
            tensions.add(move.from_square)
            
            # Si hay una captura, la pieza capturada es un actor clave
            if board.piece_at(move.to_square):
                tensions.add(move.to_square)
            
            # 2. Buscar piezas atacadas tras el movimiento (Tenedores, Clavadas, etc.)
            board.push(move)
            # Piezas del oponente que ahora están atacadas por la pieza que movimos
            for sq in chess.SQUARES:
                piece = board.piece_at(sq)
                if piece and piece.color == board.turn: # Pieza del rival
                    # ¿La ataca nuestra pieza recién movida?
                    if move.to_square in board.attackers(not board.turn, sq):
                        tensions.add(sq)
            
            return list(tensions)
        except: return []

    def start_puzzle(self, puzzle):
        self.current_puzzle = puzzle; self.solution_idx = 0; self.has_failed_current = False; self.hint_level = 0
        for b in [self.btn_hint_tension, self.btn_hint_piece, self.btn_hint_dest]: b.setChecked(False)
        self.chess_board.highlighted_square = None; self.chess_board.tension_squares = []; self.chess_board.set_engine_move(None)
        self.game.board.set_fen(puzzle["initial_fen"]); self.game.full_mainline = []; self.game.current_idx = 0
        self.game.make_move(chess.Move.from_uci(puzzle["opponent_move"]))
        is_white = self.game.board.turn == chess.WHITE; self.chess_board.flipped = not is_white
        self.label_feedback.setText("⚪ Juegan BLANCAS" if is_white else "⚫ Juegan NEGRAS")
        self.label_feedback.setStyleSheet("background: #1976d2; color: #fff; padding: 10px;"); self.chess_board.setEnabled(True); self.chess_board.update_board()

    def check_move(self, move_uci):
        if not self.current_puzzle or not self.chess_board.isEnabled(): return
        correct = self.current_puzzle["solution"][self.solution_idx]
        
        if move_uci == correct:
            self.solution_idx += 1
            if self.solution_idx >= len(self.current_puzzle["solution"]):
                self.label_feedback.setText("¡RESUELTO! 🏆"); self.label_feedback.setStyleSheet("background: #2e7d32; color: #fff; padding: 10px;"); self.update_elo(True)
                QTimer.singleShot(2000, self.load_next_puzzle)
            else:
                self.game.make_move(chess.Move.from_uci(self.current_puzzle["solution"][self.solution_idx]))
                self.solution_idx += 1; self.label_feedback.setText("¡Bien! Sigue...")
        else:
            self.has_failed_current = True
            self.chess_board.setEnabled(False) # BLOQUEO TOTAL
            self.label_feedback.setText("Analizando error...")
            self.label_feedback.setStyleSheet("background: #c62828; color: #fff; padding: 10px;")
            
            from src.core.workers import RefutationWorker
            self._ref_worker = RefutationWorker(self.parent_main.engine_path, self.game.board.fen())
            self._ref_worker.finished.connect(self.on_refutation_ready)
            self._ref_worker.start()
            self.update_elo(False)

    def on_refutation_ready(self, move_uci, msg):
        if move_uci:
            # Dibujar flecha y ejecutar jugada de castigo
            self.chess_board.set_engine_move(move_uci)
            self.game.board.push_uci(move_uci)
            self.chess_board.update_board()
            self.label_feedback.setText(f"Incorrecto. {msg}")
        
        # Tiempo para que el usuario vea el castigo
        QTimer.singleShot(2500, self.reset_to_last_correct)

    def reset_to_last_correct(self):
        try:
            # Revertir el castigo y el error del usuario
            if len(self.game.board.move_stack) > 0: self.game.board.pop()
            if len(self.game.board.move_stack) > 0: self.game.board.pop()
            self.chess_board.set_engine_move(None)
            self.chess_board.setEnabled(True)
            self.chess_board.update_board()
            self.label_feedback.setText("Inténtalo de nuevo...")
            self.label_feedback.setStyleSheet("background: #1976d2; color: #fff; padding: 10px;")
        except: pass

    def update_elo(self, success):
        if not self.parent_main: return
        my_elo = self.parent_main.app_db.get_tactical_elo(); puzzle_elo = self.current_puzzle.get("rating", 1200)
        expected = 1 / (1 + 10**((puzzle_elo - my_elo) / 400.0)); actual = 1.0 if success and not self.has_failed_current else 0.0
        if success and self.hint_level > 0: actual = 0.7
        variation = int(32 * (actual - expected))
        if variation == 0: variation = 1 if actual > 0.5 else -1
        self.parent_main.app_db.set_tactical_elo(max(100, my_elo + variation)); self.update_status("success" if success and not self.has_failed_current else "fail")

    def load_next_puzzle(self):
        """Carga automáticamente el siguiente ejercicio de la lista"""
        if self.current_index + 1 < self.puzzle_df.height:
            self.load_puzzle_by_index(self.current_index + 1)

    def update_status(self, status):
        """Actualiza el estado del puzzle tanto en el DF como en la lista visual y DB"""
        # 1. Actualizar DataFrame
        self.puzzle_df[self.current_index, "status"] = status
        
        # 2. Actualizar icono en la lista visual
        item = self.list_view.item(self.current_index)
        if item:
            widget = self.list_view.itemWidget(item)
            if widget:
                color = '#2e7d32' if status == "success" else '#c62828'
                widget.label_status.setPixmap(qta.icon('fa5s.star', color=color).pixmap(14, 14))
        
        # 3. Persistencia en SQLite
        if self.parent_main and hasattr(self.parent_main, 'app_db'):
            from src.core.workers import PuzzleSaveWorker
            p_id = str(self.current_puzzle["id"])
            self._sw = PuzzleSaveWorker(self.parent_main.app_db, p_id, status)
            self._sw.start()
            self.update_dashboard()

    def update_dashboard(self):
        if not self.parent_main: return
        stats = self.parent_main.app_db.get_all_puzzle_stats(); success = sum(1 for s in stats.values() if s == "success"); fail = sum(1 for s in stats.values() if s == "fail")
        self.val_total.setText(str(len(stats))); self.val_success.setText(str(success)); self.val_fail.setText(str(fail)); self.label_elo_tactico.setText(f"ELO TÁCTICO: {self.parent_main.app_db.get_tactical_elo()}")

    def set_themes_filter(self, themes_list): self.filter_themes = themes_list; self.apply_filters()
    def apply_filters(self):
        if not self.manager: return
        target_elo = self.slider_elo.value(); self.manager.apply_filters(min_rating=target_elo-100, max_rating=target_elo+100, theme=" ".join(self.filter_themes))
        full_set = self.manager.get_sample(); saved_stats = self.parent_main.app_db.get_all_puzzle_stats() if self.parent_main else {}
        full_set = full_set.with_columns(pl.Series("status", [saved_stats.get(str(pid), "pending") for pid in full_set["PuzzleId"]]))
        if self.filter_status != "all": full_set = full_set.filter(pl.col("status") == self.filter_status)
        self.puzzle_df = full_set; self.loaded_count = 0; self.list_view.clear(); self.load_more_puzzles(); self.update_dashboard()
        if self.puzzle_df.height > 0: self.load_puzzle_by_index(0)

    def on_status_filter_clicked(self):
        s = self.sender()
        if s.isChecked():
            for b in [self.btn_pending, self.btn_success, self.btn_fail]:
                if b != s: b.setChecked(False)
            self.filter_status = "pending" if s == self.btn_pending else ("success" if s == self.btn_success else "fail")
        else: self.filter_status = "all"
        self.apply_filters()

    def on_slider_changed(self, val):
        self.label_elo_val.setText(f"Dificultad: <b>{val}</b>")
        if self.parent_main: self.parent_main.app_db.set_config("puzzle_slider_elo", val)
        self.filter_timer.start(300)

    def load_db(self, path): self.manager = PuzzleManager(path); self.apply_filters()
    def load_more_puzzles(self):
        if self.puzzle_df is None: return
        s = self.loaded_count; e = min(s + self.batch_size, self.puzzle_df.height)
        for i in range(s, e):
            row = self.puzzle_df.row(i, named=True); it = QListWidgetItem(); it.setSizeHint(QSize(0, 35)); it.setData(Qt.UserRole, row); self.list_view.addItem(it); self.list_view.setItemWidget(it, PuzzleListItemWidget(row, row.get("status", "pending"), self))
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
