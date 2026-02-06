import os
import json
import time
from datetime import datetime
import chess
import polars as pl
from PySide6.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                             QTableWidget, QTableWidgetItem, QLabel, QPushButton, 
                             QFileDialog, QProgressBar, QHeaderView, QTextBrowser, 
                             QStatusBar, QTabWidget, QListWidget, QMenu, 
                             QColorDialog, QMenuBar, QAbstractItemView, QToolBar, 
                             QStyle, QSizePolicy, QMessageBox)
from PySide6.QtCore import Qt, QPointF, QTimer
from PySide6.QtGui import QAction, QFont, QShortcut, QKeySequence, QPainter, QColor, QBrush
import qtawesome as qta

from config import CONFIG_FILE, LIGHT_STYLE, ECO_FILE
from core.workers import PGNWorker, StatsWorker
from core.eco import ECOManager
from core.db_manager import DBManager
from core.game_controller import GameController
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
        
        # Gestores y Controladores
        self.db = DBManager()
        self.game = GameController()
        self.eco = ECOManager(ECO_FILE)
        
        # Conectar señales del controlador de juego
        self.game.position_changed.connect(self.update_ui)
        
        # Conectar señales del gestor de base de datos
        self.db.active_db_changed.connect(self.refresh_db_list)
        self.db.active_db_changed.connect(self.update_stats)
        self.db.filter_updated.connect(self.refresh_db_list)
        self.db.filter_updated.connect(self.update_stats)
        
        # Temporizador para debouncing de estadísticas (evita lag)
        self.stats_timer = QTimer()
        self.stats_timer.setSingleShot(True)
        self.stats_timer.timeout.connect(self.run_stats_worker)
        
        self.load_config() 
        self.init_ui()
        self.init_menu()
        self.init_shortcuts()
        self.update_ui() # Cargar árbol y tablero inicial
        self.statusBar().showMessage("Listo")

    def init_shortcuts(self):
        QShortcut(QKeySequence(Qt.Key_Left), self, self.game.step_back)
        QShortcut(QKeySequence(Qt.Key_Right), self, self.game.step_forward)
        QShortcut(QKeySequence(Qt.Key_Home), self, self.game.go_start)
        QShortcut(QKeySequence(Qt.Key_End), self, self.game.go_end)
        QShortcut(QKeySequence("F"), self, self.flip_boards)
        QShortcut(QKeySequence("E"), self, self.toggle_engine_shortcut)
        QShortcut(QKeySequence("S"), self, self.search_current_position)

    def flip_boards(self):
        self.board_ana.flip()

    def search_current_position(self):
        import chess.polyglot
        pos_hash = chess.polyglot.zobrist_hash(self.game.board)
        df = self.db.get_active_df()
        if df is None:
            self.statusBar().showMessage("No hay ninguna base de datos activa", 3000)
            return
        if "fens" not in df.columns:
            QMessageBox.warning(self, "Búsqueda por Posición", f"La base '{self.db.active_db_name}' no tiene el índice de posiciones.\n\nPor favor, vuelve a abrir el archivo PGN original para re-importarla.")
            return
        self.search_criteria = {"white": "", "black": "", "min_elo": "", "result": "Cualquiera", "position_hash": pos_hash}
        filtered = self.db.filter_db(self.search_criteria)
        self.refresh_db_list(filtered)
        self.tabs.setCurrentIndex(1)

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
        self.board_ana = ChessBoard(self.game.board, self); self.board_ana.color_light, self.board_ana.color_dark = self.def_light, self.def_dark
        board_eval_layout.addWidget(self.eval_bar); board_eval_layout.addWidget(self.board_ana); board_container_layout.addLayout(board_eval_layout)
        ana_layout.addWidget(board_container)
        
        panel_ana = QWidget(); p_ana_layout = QVBoxLayout(panel_ana)
        info_box = QWidget(); info_layout = QHBoxLayout(info_box); info_layout.setContentsMargins(0, 0, 0, 0)
        self.label_apertura = QLabel("<b>Apertura:</b> Inicial"); self.label_eval = QLabel(""); self.label_eval.setStyleSheet("font-weight: bold; font-size: 14px; color: #d32f2f; margin-left: 10px;")
        info_layout.addWidget(self.label_apertura, 1); info_layout.addWidget(self.label_eval); p_ana_layout.addWidget(info_box)
        
        self.tree_ana = self.create_scid_table(["Movim.", "Frec.", "Barra", "Win %", "AvElo", "Perf"])
        self.tree_ana.setFont(table_font); self.tree_ana.itemDoubleClicked.connect(self.on_tree_cell_double_click); self.tree_ana.itemClicked.connect(self.on_tree_cell_click)
        self.tree_ana.setMouseTracking(True); self.tree_ana.cellEntered.connect(self.on_tree_cell_hover); p_ana_layout.addWidget(self.tree_ana)
        
        self.hist_ana = QTextBrowser(); self.hist_ana.setOpenLinks(False); self.hist_ana.anchorClicked.connect(self.jump_to_move_link); p_ana_layout.addWidget(self.hist_ana)
        btn_save = QPushButton("💾 Guardar Partida en Clipbase"); btn_save.clicked.connect(self.add_to_clipbase); p_ana_layout.addWidget(btn_save)
        ana_layout.addWidget(panel_ana, 1)

        # --- TAB 2: GESTOR ---
        self.tab_db = QWidget(); self.tabs.addTab(self.tab_db, "Gestor Bases"); db_layout = QHBoxLayout(self.tab_db); db_sidebar = QVBoxLayout()
        self.db_list_widget = QListWidget(); self.db_list_widget.addItem("Clipbase"); self.db_list_widget.setContextMenuPolicy(Qt.CustomContextMenu); self.db_list_widget.currentRowChanged.connect(self.switch_db); self.db_list_widget.customContextMenuRequested.connect(self.on_db_list_context_menu)
        self.progress = QProgressBar(); self.progress.setVisible(False); db_sidebar.addWidget(self.db_list_widget); self.btn_search = QPushButton("🔍 Filtrar Partidas"); self.btn_search.clicked.connect(self.open_search); db_sidebar.addWidget(self.btn_search)
        self.btn_clear_filter = QPushButton("🧹 Quitar Filtros"); self.btn_clear_filter.clicked.connect(lambda: self.db.set_active_db(self.db.active_db_name))
        db_sidebar.addWidget(self.btn_clear_filter)
        self.label_db_stats = QLabel("[0/0]"); self.label_db_stats.setAlignment(Qt.AlignCenter); db_sidebar.addWidget(self.label_db_stats); db_sidebar.addWidget(self.progress); db_layout.addLayout(db_sidebar, 1)
        self.search_criteria = {"white": "", "black": "", "min_elo": "", "result": "Cualquiera"}
        db_content = QVBoxLayout(); self.db_table = self.create_scid_table(["Fecha", "Blancas", "Elo B", "Negras", "Elo N", "Res"]); self.db_table.setFont(table_font); self.db_table.itemDoubleClicked.connect(self.load_game_from_list); self.db_table.customContextMenuRequested.connect(self.on_db_table_context_menu); db_content.addWidget(self.db_table); db_layout.addLayout(db_content, 4)
        for path in getattr(self, 'pending_dbs', []):
            if os.path.exists(path): self.load_parquet(path)

    def toggle_engine(self, checked):
        self.eval_bar.setVisible(checked)
        if checked:
            self.engine_worker = EngineWorker(); self.engine_worker.info_updated.connect(self.on_engine_update); self.engine_worker.update_position(self.game.board.fen()); self.engine_worker.start()
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
        if uci: self.game.make_move(chess.Move.from_uci(uci))

    def on_tree_cell_hover(self, row, column):
        pass

    def leaveEvent(self, event):
        if hasattr(self, 'board_ana') and not self.tree_ana.selectedItems(): self.board_ana.set_hover_move(None)
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
        
        # Botón para Búsqueda por Posición (Diana)
        self.action_search_pos = QAction(qta.icon('fa5s.crosshairs'), "", self)
        self.action_search_pos.setToolTip("Buscar partidas con esta posición (S)")
        self.action_search_pos.triggered.connect(self.search_current_position)
        toolbar.addAction(self.action_search_pos)
        
        toolbar.addSeparator()
        
        actions = [(qta.icon('fa5s.step-backward'), self.game.go_start, "Inicio (Home)"), (qta.icon('fa5s.chevron-left'), self.game.step_back, "Anterior (Izquierda)"), (qta.icon('fa5s.chevron-right'), self.game.step_forward, "Siguiente (Derecha)"), (qta.icon('fa5s.step-forward'), self.game.go_end, "Final (End)"), (None, None, None), (qta.icon('fa5s.retweet'), self.flip_boards, "Girar Tablero (F)")]
        for icon, func, tip in actions:
            if icon is None: toolbar.addSeparator()
            else: action = toolbar.addAction(icon, ""); action.triggered.connect(func); action.setToolTip(tip)
        self.action_engine = QAction(qta.icon('fa5s.microchip', color='#444'), "", self); self.action_engine.setCheckable(True); self.action_engine.setToolTip("Activar/Desactivar Motor (E)")
        self.action_engine.triggered.connect(self.toggle_engine); toolbar.addAction(self.action_engine); right_spacer = QWidget(); right_spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        toolbar.addWidget(right_spacer)

    def toggle_engine_shortcut(self): self.action_engine.toggle(); self.toggle_engine(self.action_engine.isChecked())

    def add_to_clipbase(self):
        line_uci = self.game.current_line_uci
        game_data = {"id": int(time.time()), "white": "Jugador Blanco", "black": "Jugador Negro", "w_elo": 2500, "b_elo": 2500, "result": "*", "date": datetime.now().strftime("%Y.%m.%d"), "event": "Análisis Local", "line": line_uci, "full_line": line_uci, "fens": []}
        self.db.add_to_clipbase(game_data); self.statusBar().showMessage("Partida guardada en Clipbase", 3000)
        if self.db.active_db_name == "Clipbase": self.db.set_active_db("Clipbase")

    def delete_selected_game(self):
        row = self.db_table.currentRow()
        if row >= 0:
            game_id = self.db_table.item(row, 0).data(Qt.UserRole)
            if self.db.delete_game(self.db.active_db_name, game_id): self.db.set_active_db(self.db.active_db_name); self.statusBar().showMessage("Partida eliminada", 2000)

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
                if self.db.active_db_name == "Clipbase": self.db.set_active_db("Clipbase")

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
                if self.db.update_game(target_db, game_id, dialog.get_data()): self.db.set_active_db(target_db); self.statusBar().showMessage("Partida actualizada", 2000)

    def load_parquet(self, path):
        self.progress.setVisible(False); name = self.db.load_parquet(path)
        if not self.db_list_widget.findItems(name, Qt.MatchExactly): self.db_list_widget.addItem(name)
        self.db_list_widget.setCurrentRow(self.db_list_widget.count()-1); self.save_config()

    def init_menu(self):
        menubar = self.menuBar()
        
        # Menú Archivo
        file_menu = menubar.addMenu("&Archivo")
        exit_action = QAction("&Salir", self)
        exit_action.setShortcut("Ctrl+Q")
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # Menú Bases de Datos
        db_menu = menubar.addMenu("&Bases de Datos")
        
        open_pgn_action = QAction("Importar &PGN...", self)
        open_pgn_action.setShortcut("Ctrl+I")
        open_pgn_action.triggered.connect(self.import_pgn)
        db_menu.addAction(open_pgn_action)
        
        open_parquet_action = QAction("Abrir &Parquet...", self)
        open_parquet_action.setShortcut("Ctrl+O")
        open_parquet_action.triggered.connect(self.open_parquet_file)
        db_menu.addAction(open_parquet_action)
        
        # Menú Tablero
        board_menu = menubar.addMenu("&Tablero")
        settings_action = QAction("&Configuración...", self)
        settings_action.triggered.connect(self.open_settings)
        board_menu.addAction(settings_action)

    def import_pgn(self):
        path, _ = QFileDialog.getOpenFileName(self, "Importar PGN", "/data/chess", "Chess PGN (*.pgn)")
        if path:
            self.progress.setVisible(True)
            self.worker = PGNWorker(path)
            self.worker.progress.connect(self.progress.setValue)
            self.worker.status.connect(self.statusBar().showMessage)
            self.worker.finished.connect(self.load_parquet)
            self.worker.start()

    def open_parquet_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Abrir Parquet", "/data/chess", "Chess Parquet (*.parquet)")
        if path:
            self.load_parquet(path)

    def open_settings(self):
        dialog = SettingsDialog(self.board_ana.color_light, self.board_ana.color_dark, self)
        if dialog.exec_(): light, dark = dialog.get_colors(); self.board_ana.color_light = light; self.board_ana.color_dark = dark; self.board_ana.update_board(); self.save_config()

    def resizeEvent(self, event): h = self.centralWidget().height() - 40; self.board_ana.setFixedWidth(h); super().resizeEvent(event)

    def switch_db(self, row):
        if row < 0: return
        self.db.set_active_db(self.db_list_widget.item(row).text())

    def open_search(self):
        dialog = SearchDialog(self); dialog.white_input.setText(self.search_criteria["white"]); dialog.black_input.setText(self.search_criteria["black"]); dialog.min_elo_input.setText(self.search_criteria["min_elo"]); dialog.result_combo.setCurrentText(self.search_criteria["result"])
        if dialog.exec_():
            self.search_criteria = dialog.get_criteria(); self.db.filter_db(self.search_criteria)

    def refresh_db_list(self, df_to_show=None):
        df = df_to_show if isinstance(df_to_show, pl.DataFrame) else self.db.get_active_df()
        if df is None: return
        self.db_table.setSortingEnabled(False)
        total = self.db.get_active_df().height
        if df_to_show is None or not isinstance(df_to_show, pl.DataFrame):
            self.label_db_stats.setText(f"[{total}/{total}]"); self.search_criteria = {"white": "", "black": "", "min_elo": "", "result": "Cualquiera"}
        else:
            self.label_db_stats.setText(f"[{df_to_show.height}/{total}]")
        disp = df.head(1000); self.db_table.setRowCount(disp.height)
        for i, r in enumerate(disp.rows(named=True)):
            self.db_table.setItem(i, 0, QTableWidgetItem(r["date"])); self.db_table.setItem(i, 1, QTableWidgetItem(r["white"])); self.db_table.setItem(i, 2, QTableWidgetItem(str(r["w_elo"]))); self.db_table.setItem(i, 3, QTableWidgetItem(r["black"])); self.db_table.setItem(i, 4, QTableWidgetItem(str(r["b_elo"]))); self.db_table.setItem(i, 5, QTableWidgetItem(r["result"])); self.db_table.item(i, 0).setData(Qt.UserRole, r["id"])
        self.db_table.setSortingEnabled(True); self.db_table.resizeColumnsToContents()

    def load_game_from_list(self, item):
        game_id = self.db_table.item(item.row(), 0).data(Qt.UserRole); row = self.db.get_game_by_id(self.db.active_db_name, game_id)
        if row: self.game.load_uci_line(row["full_line"]); self.tabs.setCurrentIndex(0)

    def jump_to_move_link(self, url):
        self.game.jump_to_move(int(url.toString()))

    def update_ui(self):
        self.tree_ana.clearSelection(); self.board_ana.set_hover_move(None); self.board_ana.update_board(); self.update_stats()
        if hasattr(self, 'engine_worker') and self.engine_worker.isRunning(): self.engine_worker.update_position(self.game.board.fen())
        temp = chess.Board(); html = "<style>a { text-decoration: none; color: #222; } .active { background-color: #f6f669; }</style>"
        for i, m in enumerate(self.game.full_mainline):
            san_move = temp.san(m); num = (i//2)+1
            if i % 2 == 0: html += f"<b>{num}.</b> "
            st = "class='active'" if i == self.game.current_idx - 1 else ""
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

    def run_stats_worker(self):
        if hasattr(self, 'stats_worker') and self.stats_worker.isRunning(): self.stats_worker.terminate(); self.stats_worker.wait()
        self.stats_worker = StatsWorker(self.db, self.game.current_line_uci, self.game.board.turn == chess.WHITE); self.stats_worker.finished.connect(self.on_stats_finished); self.stats_worker.start()

    def update_stats(self):
        is_filtered = self.db.current_filter_df is not None
        status_text = " (Filtrado)" if is_filtered else ""
        self.label_apertura.setText(f"<b>Apertura:</b> {self.eco.get_opening_name(self.game.current_line_uci)}{status_text}")
        self.stats_timer.start(150)

    def on_stats_finished(self, res):
        if res is None: self.tree_ana.setRowCount(0); return
        table = self.tree_ana; table.setSortingEnabled(False); table.setRowCount(res.height); is_white = self.game.board.turn == chess.WHITE
        
        valid_rows = 0
        for i, r in enumerate(res.rows(named=True)):
            try:
                mv = chess.Move.from_uci(r["uci"])
                if mv not in self.game.board.legal_moves:
                    continue # Saltar jugadas que ya no son legales
                
                san = self.game.board.san(mv)
                it_move = QTableWidgetItem(san); it_move.setData(Qt.UserRole, r["uci"]); table.setItem(valid_rows, 0, it_move)
                it_count = QTableWidgetItem(); it_count.setData(Qt.DisplayRole, r["c"]); table.setItem(valid_rows, 1, it_count)
                table.setCellWidget(valid_rows, 2, self.ResultsWidget(r["w"], r["d"], r["b"], r["c"], is_white))
                win_rate = ((r["w"] + 0.5 * r["d"]) / r["c"] if is_white else (r["b"] + 0.5 * r["d"]) / r["c"]) * 100
                it_win = QTableWidgetItem(f"{win_rate:.1f}%"); it_win.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter); table.setItem(valid_rows, 3, it_win)
                score = (r["w"] + 0.5 * r["d"]) / r["c"] if is_white else (r["b"] + 0.5 * r["d"]) / r["c"]
                perf = int(r["avg_b_elo" if is_white else "avg_w_elo"] + (score - 0.5) * 800)
                it_elo = QTableWidgetItem(); it_elo.setData(Qt.DisplayRole, int(r["avg_w_elo" if is_white else "avg_b_elo"])); table.setItem(valid_rows, 4, it_elo)
                it_perf = QTableWidgetItem(); it_perf.setData(Qt.DisplayRole, perf); table.setItem(valid_rows, 5, it_perf)
                valid_rows += 1
            except: continue
            
        table.setRowCount(valid_rows)
        table.setSortingEnabled(True)
        table.sortByColumn(1, Qt.DescendingOrder)
        table.resizeColumnsToContents()