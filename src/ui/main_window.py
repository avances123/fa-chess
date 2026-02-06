import os
import json
import time
from datetime import datetime
import chess
from PySide6.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                             QTableWidget, QTableWidgetItem, QLabel, QPushButton, 
                             QFileDialog, QProgressBar, QHeaderView, QTextBrowser, 
                             QStatusBar, QTabWidget, QListWidget, QMenu, 
                             QColorDialog, QMenuBar, QAbstractItemView, QToolBar, 
                             QStyle, QSizePolicy, QMessageBox)
from PySide6.QtCore import Qt, QPointF
from PySide6.QtGui import QAction, QFont, QShortcut, QKeySequence, QPainter, QColor, QBrush
import qtawesome as qta

from config import CONFIG_FILE, LIGHT_STYLE, ECO_FILE
from core.workers import PGNWorker
from core.eco import ECOManager
from core.db_manager import DBManager
from ui.board import ChessBoard
from ui.settings_dialog import SettingsDialog
from ui.search_dialog import SearchDialog
from ui.edit_game_dialog import EditGameDialog
from core.engine_worker import EngineWorker

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("fa-chess")
        self.setStyleSheet(LIGHT_STYLE)
        
        self.board = chess.Board()
        self.full_mainline = []
        self.current_idx = 0
        
        self.db = DBManager()
        self.eco = ECOManager(ECO_FILE)
        
        self.load_config() 
        self.init_ui()
        self.init_menu()
        self.init_shortcuts()
        self.statusBar().showMessage("Listo")

    def init_shortcuts(self):
        QShortcut(QKeySequence(Qt.Key_Left), self, self.step_back)
        QShortcut(QKeySequence(Qt.Key_Right), self, self.step_forward)
        QShortcut(QKeySequence(Qt.Key_Home), self, self.go_start)
        QShortcut(QKeySequence(Qt.Key_End), self, self.go_end)
        QShortcut(QKeySequence("F"), self, self.flip_boards)
        QShortcut(QKeySequence("E"), self, self.toggle_engine_shortcut)

    def flip_boards(self):
        self.board_ana.flip()

    def init_ui(self):
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)
        table_font = QFont("monospace", 9)

        # --- TAB 1: ANÁLISIS ---
        self.tab_analysis = QWidget()
        self.tabs.addTab(self.tab_analysis, "Tablero")
        ana_layout = QHBoxLayout(self.tab_analysis); ana_layout.setContentsMargins(0, 0, 0, 0)
        
        board_container = QWidget(); board_container_layout = QVBoxLayout(board_container)
        board_container_layout.setContentsMargins(0,0,0,0); board_container_layout.setSpacing(0)
        self.toolbar_ana = QToolBar(); self.toolbar_ana.setMovable(False); self.setup_toolbar(self.toolbar_ana)
        board_container_layout.addWidget(self.toolbar_ana)
        
        board_eval_layout = QHBoxLayout(); board_eval_layout.setSpacing(5)
        self.eval_bar = QProgressBar(); self.eval_bar.setOrientation(Qt.Vertical); self.eval_bar.setRange(-1000, 1000); self.eval_bar.setValue(0); self.eval_bar.setTextVisible(False); self.eval_bar.setFixedWidth(15); self.eval_bar.setStyleSheet("QProgressBar { border: 1px solid #777; background-color: #333; } QProgressBar::chunk { background-color: #eee; }"); self.eval_bar.setVisible(False)
        self.board_ana = ChessBoard(self.board, self); self.board_ana.color_light, self.board_ana.color_dark = self.def_light, self.def_dark
        board_eval_layout.addWidget(self.eval_bar); board_eval_layout.addWidget(self.board_ana); board_container_layout.addLayout(board_eval_layout)
        ana_layout.addWidget(board_container)
        
        panel_ana = QWidget(); p_ana_layout = QVBoxLayout(panel_ana)
        info_box = QWidget(); info_layout = QHBoxLayout(info_box); info_layout.setContentsMargins(0, 0, 0, 0)
        self.label_apertura = QLabel("<b>Apertura:</b> Inicial"); self.label_eval = QLabel(""); self.label_eval.setStyleSheet("font-weight: bold; font-size: 14px; color: #d32f2f; margin-left: 10px;")
        info_layout.addWidget(self.label_apertura, 1); info_layout.addWidget(self.label_eval); p_ana_layout.addWidget(info_box)
        
        self.tree_ana = self.create_scid_table(["Movim.", "Frec.", "Barra", "Win %", "AvElo", "Perf"])
        self.tree_ana.setFont(table_font); self.tree_ana.itemDoubleClicked.connect(self.on_tree_cell_double_click); self.tree_ana.itemClicked.connect(self.on_tree_cell_click)
        self.tree_ana.setMouseTracking(True); self.tree_ana.cellEntered.connect(self.on_tree_cell_hover); p_ana_layout.addWidget(self.tree_ana)
        
        self.hist_ana = QTextBrowser(); self.hist_ana.setOpenLinks(False); self.hist_ana.anchorClicked.connect(self.jump_to_move); p_ana_layout.addWidget(self.hist_ana)
        btn_save = QPushButton("💾 Guardar Partida en Clipbase"); btn_save.clicked.connect(self.add_to_clipbase); p_ana_layout.addWidget(btn_save)
        ana_layout.addWidget(panel_ana, 1)

        # --- TAB 2: GESTOR ---
        self.tab_db = QWidget(); self.tabs.addTab(self.tab_db, "Gestor Bases"); db_layout = QHBoxLayout(self.tab_db); db_sidebar = QVBoxLayout()
        self.db_list_widget = QListWidget(); self.db_list_widget.addItem("Clipbase"); self.db_list_widget.setContextMenuPolicy(Qt.CustomContextMenu); self.db_list_widget.currentRowChanged.connect(self.switch_db); self.db_list_widget.customContextMenuRequested.connect(self.on_db_list_context_menu)
        self.progress = QProgressBar(); self.progress.setVisible(False); db_sidebar.addWidget(self.db_list_widget); self.btn_search = QPushButton("🔍 Filtrar Partidas"); self.btn_search.clicked.connect(self.open_search); db_sidebar.addWidget(self.btn_search); self.label_db_stats = QLabel("[0/0]"); self.label_db_stats.setAlignment(Qt.AlignCenter); db_sidebar.addWidget(self.label_db_stats); db_sidebar.addWidget(self.progress); db_layout.addLayout(db_sidebar, 1)
        self.search_criteria = {"white": "", "black": "", "min_elo": "", "result": "Cualquiera"}
        db_content = QVBoxLayout(); self.db_table = self.create_scid_table(["Fecha", "Blancas", "Elo B", "Negras", "Elo N", "Res"]); self.db_table.setFont(table_font); self.db_table.itemDoubleClicked.connect(self.load_game_from_list); self.db_table.customContextMenuRequested.connect(self.on_db_table_context_menu); db_content.addWidget(self.db_table); db_layout.addLayout(db_content, 4)
        for path in getattr(self, 'pending_dbs', []):
            if os.path.exists(path): self.load_parquet(path)

    def toggle_engine(self, checked):
        self.eval_bar.setVisible(checked)
        if checked:
            self.engine_worker = EngineWorker(); self.engine_worker.info_updated.connect(self.on_engine_update); self.engine_worker.update_position(self.board.fen()); self.engine_worker.start()
        else:
            if hasattr(self, 'engine_worker'): self.engine_worker.stop(); self.engine_worker.wait()
            self.label_eval.setText(""); self.board_ana.set_engine_move(None)

    def on_engine_update(self, eval_str, best_move, mainline):
        self.label_eval.setText(eval_str); self.board_ana.set_engine_move(best_move if best_move else None)
        try:
            if "M" in eval_str: val = 1000 if "+" in eval_str or eval_str[0].isdigit() or (eval_str.startswith("M") and not eval_str.startswith("-M")) else -1000
            else: val = int(float(eval_str) * 100)
            self.eval_bar.setValue(val)
        except: pass

    def closeEvent(self, event):
        if hasattr(self, 'engine_worker'): self.engine_worker.stop(); self.engine_worker.wait()
        super().closeEvent(event)

    def on_tree_cell_click(self, item):
        it_move = self.tree_ana.item(item.row(), 0)
        if it_move: self.board_ana.set_hover_move(it_move.data(Qt.UserRole))

    def on_tree_cell_double_click(self, item):
        it_move = self.tree_ana.item(item.row(), 0); uci = it_move.data(Qt.UserRole)
        if uci: self.make_move(chess.Move.from_uci(uci))

    def on_tree_cell_hover(self, row, column):
        pass

    def leaveEvent(self, event):
        super().leaveEvent(event)

    class ResultsWidget(QWidget):
        def __init__(self, w, d, b, total, is_white):
            super().__init__()
            self.w, self.d, self.b, self.total, self.is_white = w, d, b, total, is_white
            self.setMinimumWidth(80); self.setFixedHeight(18)
            if total > 0:
                p_win = ((w + 0.5 * d) / total) * 100 if is_white else ((b + 0.5 * d) / total) * 100
                self.setToolTip(f"Éxito: {p_win:.1f}% (W:{w} D:{d} L:{b})")

        def paintEvent(self, event):
            if self.total == 0: return
            p = QPainter(self); p.setRenderHint(QPainter.Antialiasing); rect = self.rect().adjusted(2, 4, -2, -4)
            w_px = int(rect.width() * (self.w / self.total)); d_px = int(rect.width() * (self.d / self.total)); b_px = max(0, rect.width() - w_px - d_px)
            p.fillRect(rect.x(), rect.y(), w_px, rect.height(), QColor("#eee")); p.fillRect(rect.x() + w_px, rect.y(), d_px, rect.height(), QColor("#999")); p.fillRect(rect.x() + w_px + d_px, rect.y(), b_px, rect.height(), QColor("#333")); p.setPen(QColor("#777")); p.drawRect(rect)

    def create_scid_table(self, headers):
        table = QTableWidget(0, len(headers)); table.setHorizontalHeaderLabels(headers); table.setEditTriggers(QAbstractItemView.NoEditTriggers); table.setSelectionBehavior(QAbstractItemView.SelectRows); table.setContextMenuPolicy(Qt.CustomContextMenu); table.verticalHeader().setVisible(False); table.verticalHeader().setDefaultSectionSize(22); table.horizontalHeader().setHighlightSections(True); table.setSortingEnabled(True); table.setShowGrid(True); table.setStyleSheet("QHeaderView::section { font-weight: bold; }")
        return table

    def setup_toolbar(self, toolbar):
        toolbar.setToolButtonStyle(Qt.ToolButtonIconOnly); left_spacer = QWidget(); left_spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred); toolbar.addWidget(left_spacer)
        actions = [(qta.icon('fa5s.step-backward'), self.go_start, "Inicio (Home)"), (qta.icon('fa5s.chevron-left'), self.step_back, "Anterior (Izquierda)"), (qta.icon('fa5s.chevron-right'), self.step_forward, "Siguiente (Derecha)"), (qta.icon('fa5s.step-forward'), self.go_end, "Final (End)"), (None, None, None), (qta.icon('fa5s.retweet'), self.flip_boards, "Girar Tablero (F)")]
        for icon, func, tip in actions:
            if icon is None: toolbar.addSeparator()
            else: action = toolbar.addAction(icon, ""); action.triggered.connect(func); action.setToolTip(tip)
        self.action_engine = QAction(qta.icon('fa5s.microchip', color='#444'), "", self); self.action_engine.setCheckable(True); self.action_engine.setToolTip("Activar/Desactivar Motor (E)"); self.action_engine.triggered.connect(self.toggle_engine); toolbar.addAction(self.action_engine); right_spacer = QWidget(); right_spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred); toolbar.addWidget(right_spacer)

    def toggle_engine_shortcut(self): self.action_engine.toggle(); self.toggle_engine(self.action_engine.isChecked())

    def add_to_clipbase(self):
        line_uci = " ".join([m.uci() for m in self.full_mainline])
        game_data = {"id": int(time.time()), "white": "Jugador Blanco", "black": "Jugador Negro", "w_elo": 2500, "b_elo": 2500, "result": "*", "date": datetime.now().strftime("%Y.%m.%d"), "event": "Análisis Local", "line": line_uci, "full_line": line_uci}
        self.db.add_to_clipbase(game_data); self.statusBar().showMessage("Partida guardada en Clipbase", 3000)
        if self.db.active_db_name == "Clipbase": self.refresh_db_list()

    def delete_selected_game(self):
        row = self.db_table.currentRow()
        if row >= 0:
            game_id = self.db_table.item(row, 0).data(Qt.UserRole)
            if self.db.delete_game(self.db.active_db_name, game_id): self.refresh_db_list(); self.statusBar().showMessage("Partida eliminada", 2000)

    def on_db_table_context_menu(self, pos):
        menu = QMenu(); copy_action = QAction("📋 Copiar a Clipbase", self); copy_action.triggered.connect(self.copy_to_clipbase); menu.addAction(copy_action); edit_action = QAction("📝 Editar Datos Partida", self); edit_action.triggered.connect(self.edit_selected_game); menu.addAction(edit_action); is_readonly = self.db.db_metadata.get(self.db.active_db_name, {}).get("read_only", True)
        if not is_readonly or self.db.active_db_name == "Clipbase": menu.addSeparator(); del_action = QAction("❌ Eliminar Partida", self); del_action.triggered.connect(self.delete_selected_game); menu.addAction(del_action)
        menu.exec(self.db_table.viewport().mapToGlobal(pos))

    def on_db_list_context_menu(self, pos):
        item = self.db_list_widget.itemAt(pos)
        if not item or item.text() == "Clipbase": return
        menu = QMenu(); remove_action = QAction("❌ Quitar de la lista", self); remove_action.triggered.connect(lambda: self.remove_database(item)); menu.addAction(remove_action); menu.exec(self.db_list_widget.mapToGlobal(pos))

    def remove_database(self, item):
        name = item.text()
        if self.db.remove_database(name):
            self.db_list_widget.takeItem(self.db_list_widget.row(item))
            if self.db.active_db_name == name: self.db_list_widget.setCurrentRow(0)
            self.save_config(); self.statusBar().showMessage(f"Base '{name}' quitada", 2000)

    def copy_to_clipbase(self):
        row = self.db_table.currentRow()
        if row >= 0:
            game_id = self.db_table.item(row, 0).data(Qt.UserRole); game_data = self.db.get_game_by_id(self.db.active_db_name, game_id)
            if game_data:
                game_data["id"] = int(time.time() * 1000); cols = self.db.dbs["Clipbase"].columns; clean_data = {k: game_data[k] for k in cols}; self.db.add_to_clipbase(clean_data); self.statusBar().showMessage("Partida copiada a Clipbase", 2000)
                if self.db.active_db_name == "Clipbase": self.refresh_db_list()

    def edit_selected_game(self):
        row = self.db_table.currentRow()
        if row < 0: return
        game_id = self.db_table.item(row, 0).data(Qt.UserRole); is_readonly = self.db.db_metadata.get(self.db.active_db_name, {}).get("read_only", True); target_db = self.db.active_db_name
        if is_readonly and target_db != "Clipbase":
            ret = QMessageBox.question(self, "Base de Solo Lectura", "¿Deseas copiar esta partida a la Clipbase para editarla?", QMessageBox.Yes | QMessageBox.No)
            if ret == QMessageBox.Yes: self.copy_to_clipbase(); target_db = "Clipbase"; self.refresh_db_list(); row = self.db_table.rowCount() - 1; game_id = self.db_table.item(row, 0).data(Qt.UserRole)
            else: return
        current_row = self.db.get_game_by_id(target_db, game_id)
        if current_row:
            dialog = EditGameDialog(current_row, self)
            if dialog.exec_():
                if self.db.update_game(target_db, game_id, dialog.get_data()): self.refresh_db_list(); self.statusBar().showMessage("Partida actualizada", 2000)

    def load_parquet(self, path):
        self.progress.setVisible(False); name = self.db.load_parquet(path)
        if not self.db_list_widget.findItems(name, Qt.MatchExactly): self.db_list_widget.addItem(name)
        self.db_list_widget.setCurrentRow(self.db_list_widget.count()-1); self.save_config(); self.refresh_db_list()

    def init_menu(self):
        menubar = self.menuBar(); file_menu = menubar.addMenu("&Archivo"); open_action = QAction("&Abrir PGN...", self); open_action.setShortcut("Ctrl+O"); open_action.triggered.connect(self.open_file); file_menu.addAction(open_action); exit_action = QAction("&Salir", self); exit_action.setShortcut("Ctrl+Q"); exit_action.triggered.connect(self.close); file_menu.addAction(exit_action); board_menu = menubar.addMenu("&Tablero"); settings_action = QAction("&Configuración...", self); settings_action.triggered.connect(self.open_settings); board_menu.addAction(settings_action)

    def open_settings(self):
        dialog = SettingsDialog(self.board_ana.color_light, self.board_ana.color_dark, self)
        if dialog.exec_(): light, dark = dialog.get_colors(); self.board_ana.color_light = light; self.board_ana.color_dark = dark; self.board_ana.update_board(); self.save_config()

    def resizeEvent(self, event): h = self.centralWidget().height() - 40; self.board_ana.setFixedWidth(h); super().resizeEvent(event)

    def open_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Abrir", "/data/chess", "Chess (*.pgn *.parquet)")
        if path:
            p_path = path.replace(".pgn", ".parquet")
            if os.path.exists(p_path): self.load_parquet(p_path)
            else: self.progress.setVisible(True); self.worker = PGNWorker(path); self.worker.progress.connect(self.progress.setValue); self.worker.status.connect(self.statusBar().showMessage); self.worker.finished.connect(self.load_parquet); self.worker.start()

    def switch_db(self, row):
        if row < 0: return
        self.db.active_db_name = self.db_list_widget.item(row).text(); self.refresh_db_list(); self.update_stats()

    def open_search(self):
        dialog = SearchDialog(self); dialog.white_input.setText(self.search_criteria["white"]); dialog.black_input.setText(self.search_criteria["black"]); dialog.min_elo_input.setText(self.search_criteria["min_elo"]); dialog.result_combo.setCurrentText(self.search_criteria["result"])
        if dialog.exec_():
            self.search_criteria = dialog.get_criteria(); filtered = self.db.filter_db(self.db.active_db_name, self.search_criteria)
            if filtered is not None: self.label_db_stats.setText(f"[{filtered.height}/{self.db.get_active_df().height}]"); self.refresh_db_list(filtered)

    def refresh_db_list(self, df_to_show=None):
        df = df_to_show if df_to_show is not None else self.db.get_active_df()
        if df is None: return
        
        # Desactivar ordenación mientras rellenamos para evitar filas en blanco
        self.db_table.setSortingEnabled(False)
        
        if df_to_show is None:
            total = df.height
            self.label_db_stats.setText(f"[{total}/{total}]")
            self.search_criteria = {"white": "", "black": "", "min_elo": "", "result": "Cualquiera"}
            
        disp = df.head(1000)
        self.db_table.setRowCount(disp.height)
        for i, r in enumerate(disp.rows(named=True)):
            self.db_table.setItem(i, 0, QTableWidgetItem(r["date"]))
            self.db_table.setItem(i, 1, QTableWidgetItem(r["white"]))
            self.db_table.setItem(i, 2, QTableWidgetItem(str(r["w_elo"])))
            self.db_table.setItem(i, 3, QTableWidgetItem(r["black"]))
            self.db_table.setItem(i, 4, QTableWidgetItem(str(r["b_elo"])))
            self.db_table.setItem(i, 5, QTableWidgetItem(r["result"]))
            self.db_table.item(i, 0).setData(Qt.UserRole, r["id"])
            
        self.db_table.setSortingEnabled(True)
        self.db_table.resizeColumnsToContents()

    def load_game_from_list(self, item):
        game_id = self.db_table.item(item.row(), 0).data(Qt.UserRole); row = self.db.get_game_by_id(self.db.active_db_name, game_id)
        if row:
            self.board.reset(); self.full_mainline = []
            for uci in row["full_line"].split():
                if uci: self.full_mainline.append(chess.Move.from_uci(uci))
            self.current_idx = 0; self.tabs.setCurrentIndex(0); self.update_ui()

    def make_move(self, move):
        # Limpiar selección y flecha azul al mover
        self.tree_ana.clearSelection()
        self.board_ana.set_hover_move(None)
        
        if self.current_idx < len(self.full_mainline):
            if self.full_mainline[self.current_idx] == move: self.step_forward(); return
            self.full_mainline = self.full_mainline[:self.current_idx]
        self.board.push(move); self.full_mainline.append(move); self.current_idx += 1; self.update_ui()

    def step_back(self):
        self.tree_ana.clearSelection()
        self.board_ana.set_hover_move(None)
        if self.current_idx > 0: self.current_idx -= 1; self.board.pop(); self.update_ui()

    def step_forward(self):
        self.tree_ana.clearSelection()
        self.board_ana.set_hover_move(None)
        if self.current_idx < len(self.full_mainline): self.board.push(self.full_mainline[self.current_idx]); self.current_idx += 1; self.update_ui()

    def go_start(self):
        while self.current_idx > 0: self.step_back()

    def go_end(self):
        while self.current_idx < len(self.full_mainline): self.step_forward()

    def jump_to_move(self, url):
        idx = int(url.toString()); self.go_start()
        for _ in range(idx): self.step_forward()

    def update_ui(self):
        self.board_ana.update_board(); self.update_stats()
        if hasattr(self, 'engine_worker') and self.engine_worker.isRunning(): self.engine_worker.update_position(self.board.fen())
        temp = chess.Board(); html = "<style>a { text-decoration: none; color: #222; } .active { background-color: #f6f669; }</style>"
        for i, m in enumerate(self.full_mainline):
            san_move = temp.san(m); num = (i//2)+1
            if i % 2 == 0: html += f"<b>{num}.</b> "
            st = "class='active'" if i == self.current_idx - 1 else ""
            html += f"<a {st} href='{i+1}'>{san_move}</a> "; temp.push(m)
        self.hist_ana.setHtml(html)

    def save_config(self):
        dbs_info = [{"path": m["path"]} for m in self.db.db_metadata.values() if m.get("path")]; colors = {"light": self.board_ana.color_light, "dark": self.board_ana.color_dark}; config = {"dbs": dbs_info, "colors": colors}; json.dump(config, open(CONFIG_FILE, "w"))

    def load_config(self):
        self.def_light, self.def_dark = "#eeeed2", "#8ca2ad"
        if os.path.exists(CONFIG_FILE):
            try:
                data = json.load(open(CONFIG_FILE, "r")); self.pending_dbs = [info["path"] if isinstance(info, dict) else info for info in data.get("dbs", [])]; colors = data.get("colors")
                if colors: self.def_light = colors.get("light", self.def_light); self.def_dark = colors.get("dark", self.def_dark)
            except: pass

    def update_stats(self):
        moves = self.board.move_stack; line_uci = " ".join([m.uci() for m in moves]); self.label_apertura.setText(f"<b>Apertura:</b> {self.eco.get_opening_name(line_uci)}"); res = self.db.get_stats_for_position(line_uci, self.board.turn == chess.WHITE)
        if res is None: self.tree_ana.setRowCount(0); return
        table = self.tree_ana; table.setSortingEnabled(False); table.setRowCount(res.height); is_white = self.board.turn == chess.WHITE
        for i, r in enumerate(res.rows(named=True)):
            mv = chess.Move.from_uci(r["uci"]); san = self.board.san(mv); it_move = QTableWidgetItem(san); it_move.setData(Qt.UserRole, r["uci"]); table.setItem(i, 0, it_move); it_count = QTableWidgetItem(); it_count.setData(Qt.DisplayRole, r["c"]); table.setItem(i, 1, it_count); table.setCellWidget(i, 2, self.ResultsWidget(r["w"], r["d"], r["b"], r["c"], is_white)); win_rate = ((r["w"] + 0.5 * r["d"]) / r["c"] if is_white else (r["b"] + 0.5 * r["d"]) / r["c"]) * 100; it_win = QTableWidgetItem(f"{win_rate:.1f}%"); it_win.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter); table.setItem(i, 3, it_win); score = (r["w"] + 0.5 * r["d"]) / r["c"] if is_white else (r["b"] + 0.5 * r["d"]) / r["c"]; perf = int(r["avg_b_elo" if is_white else "avg_w_elo"] + (score - 0.5) * 800); it_elo = QTableWidgetItem(); it_elo.setData(Qt.DisplayRole, int(r["avg_w_elo" if is_white else "avg_b_elo"])); table.setItem(i, 4, it_elo); it_perf = QTableWidgetItem(); it_perf.setData(Qt.DisplayRole, perf); table.setItem(i, 5, it_perf)
        table.setSortingEnabled(True); table.resizeColumnsToContents()
