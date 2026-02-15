import os
import json
import time
import io
from datetime import datetime
import chess
import chess.pgn
import chess.polyglot
import polars as pl
from PySide6.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                             QTableWidget, QTableWidgetItem, QLabel, QPushButton, 
                             QFileDialog, QProgressBar, QHeaderView, QTextBrowser, 
                             QStatusBar, QTabWidget, QListWidget, QListWidgetItem, QMenu, 
                             QColorDialog, QMenuBar, QAbstractItemView, QToolBar, 
                             QStyle, QSizePolicy, QMessageBox, QApplication, QLineEdit, QTabBar)
from PySide6.QtCore import Qt, QPointF, QTimer, QSize
from PySide6.QtGui import QAction, QFont, QShortcut, QKeySequence, QPainter, QColor, QBrush
import qtawesome as qta

from src.config import CONFIG_FILE, LIGHT_STYLE, ECO_FILE, APP_DB_FILE, logger
from src.core.workers import PGNWorker, StatsWorker, PGNExportWorker, PGNAppendWorker, PuzzleGeneratorWorker
from src.core.eco import ECOManager
from src.core.db_manager import DBManager
from src.core.app_db import AppDBManager
from src.core.game_controller import GameController
from src.ui.board import ChessBoard
from src.ui.settings_dialog import SettingsDialog
from src.ui.search_dialog import SearchDialog
from src.ui.edit_game_dialog import EditGameDialog
from src.ui.player_report_widget import PlayerReportWidget
from src.ui.widgets.results_bar import ResultsWidget
from src.ui.widgets.eval_graph import EvaluationGraph
from src.ui.widgets.analysis_report import AnalysisReport
from src.ui.widgets.game_info_header import GameInfoHeader
from src.ui.widgets.db_sidebar import DBSidebar
from src.ui.widgets.opening_tree_table import OpeningTreeTable
from src.ui.widgets.puzzle_browser import PuzzleBrowserWidget
from src.ui.utils import format_qty
from src.ui.styles import (STYLE_EVAL_BAR, STYLE_LABEL_EVAL, STYLE_TABLE_HEADER, 
                       STYLE_PROGRESS_BAR, STYLE_BADGE_NORMAL, STYLE_BADGE_SUCCESS, 
                       STYLE_BADGE_ERROR, STYLE_GAME_HEADER, STYLE_ACTION_BUTTON)
from src.core.engine_worker import EngineWorker, FullAnalysisWorker

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("fa-chess")
        self.setStyleSheet(LIGHT_STYLE)
        
        self.app_db = AppDBManager(APP_DB_FILE)
        self.db = DBManager()
        self.game = GameController()
        self.eco = ECOManager(ECO_FILE)
        
        self.sort_col = None
        self.sort_desc = False
        self.col_mapping = {0: "id", 1: "date", 2: "white", 3: "w_elo", 4: "black", 5: "b_elo", 6: "result"}
        self.game_evals = []
        self.db_batch_size = 100
        self.db_loaded_count = 0
        self.current_db_df = None
        self.last_pos_count = 1000000 
        
        self.game.position_changed.connect(self.update_ui)
        self.db.active_db_changed.connect(self.refresh_db_list)
        self.db.active_db_changed.connect(self.update_stats)
        self.db.active_db_changed.connect(self.refresh_reference_combo)
        self.db.filter_updated.connect(self.refresh_db_list)
        self.db.filter_updated.connect(self.update_stats)
        
        self.stats_timer = QTimer()
        self.stats_timer.setSingleShot(True)
        self.stats_timer.timeout.connect(self.run_stats_worker)
        
        self.load_config() 
        self.init_ui()
        self.init_menu()
        self.init_shortcuts()
        
        # CARGA INICIAL
        for path in self.pending_dbs:
            if path and os.path.exists(path):
                self.load_parquet(path)
        
        # Restaurar Base Activa persistida
        if hasattr(self, 'pending_active_db') and self.pending_active_db:
            target = self.pending_active_db
            for i in range(self.db_sidebar.list_widget.count()):
                if self.db_sidebar.list_widget.item(i).data(Qt.UserRole + 1) == target:
                    self.db_sidebar.list_widget.setCurrentRow(i)
                    self.db.set_active_db(target)
                    break
        elif self.db_sidebar.list_widget.count() > 0:
            self.db_sidebar.list_widget.setCurrentRow(0)
        
        self.update_ui()
        self.tabs.setCurrentIndex(1)
        self._fix_tab_buttons()
        self.statusBar().showMessage("Listo")

    def init_shortcuts(self):
        QShortcut(QKeySequence(Qt.Key_Left), self, self.game.step_back)
        QShortcut(QKeySequence(Qt.Key_Right), self, self.game.step_forward)
        QShortcut(QKeySequence(Qt.Key_Home), self, self.game.go_start)
        QShortcut(QKeySequence(Qt.Key_End), self, self.game.go_end)
        QShortcut(QKeySequence("F"), self, self.flip_boards)
        QShortcut(QKeySequence("E"), self, self.toggle_engine_shortcut)
        QShortcut(QKeySequence("S"), self, self.search_current_position)
        QShortcut(QKeySequence("Ctrl+S"), self, self.save_to_active_db)

    def flip_boards(self): self.board_ana.flip()

    def search_current_position(self):
        pos_hash = chess.polyglot.zobrist_hash(self.game.board)
        if not self.db.active_db_name:
            self.statusBar().showMessage("No hay ninguna base de datos activa", 3000)
            return
        
        QApplication.setOverrideCursor(Qt.WaitCursor)
        self.progress.setRange(0, 0); self.progress.show()
        self.statusBar().showMessage("Buscando posición...")
        QApplication.processEvents()
        try:
            self.search_criteria = {"white": "", "black": "", "min_elo": "", "result": "Cualquiera", "position_hash": pos_hash, "use_position": True}
            self.db.filter_db(self.search_criteria)
        finally:
            QApplication.restoreOverrideCursor()

    def init_ui(self):
        self.tabs = QTabWidget(); self.tabs.setTabsClosable(True); self.tabs.tabCloseRequested.connect(self.close_tab)
        self.tabs.tabBar().setTabButton(0, QTabBar.RightSide, None); self.tabs.tabBar().setTabButton(1, QTabBar.RightSide, None)
        self.setCentralWidget(self.tabs)

        # TAB 1: ANÁLISIS
        self.tab_analysis = QWidget(); self.tabs.addTab(self.tab_analysis, "Tablero")
        ana_layout = QHBoxLayout(self.tab_analysis); ana_layout.setContentsMargins(0, 0, 0, 0)
        board_container = QWidget(); board_container_layout = QVBoxLayout(board_container); board_container_layout.setContentsMargins(0,0,0,0); board_container_layout.setSpacing(0)
        self.game_header = GameInfoHeader(); board_container_layout.addWidget(self.game_header)
        self.toolbar_ana = QToolBar(); self.toolbar_ana.setMovable(False); self.setup_toolbar(self.toolbar_ana); board_container_layout.addWidget(self.toolbar_ana)
        board_eval_layout = QHBoxLayout(); board_eval_layout.setSpacing(5)
        self.eval_bar = QProgressBar(); self.eval_bar.setOrientation(Qt.Vertical); self.eval_bar.setRange(-1000, 1000); self.eval_bar.setValue(0); self.eval_bar.setTextVisible(False); self.eval_bar.setFixedWidth(15); self.eval_bar.setStyleSheet(STYLE_EVAL_BAR); self.eval_bar.setVisible(False)
        self.board_ana = ChessBoard(self.game.board, self); self.board_ana.color_light, self.board_ana.color_dark = self.def_light, self.def_dark
        board_eval_layout.addWidget(self.eval_bar); board_eval_layout.addWidget(self.board_ana); board_container_layout.addLayout(board_eval_layout)
        ana_layout.addWidget(board_container)
        
        panel_ana = QWidget(); p_ana_layout = QVBoxLayout(panel_ana)
        info_box = QWidget(); info_layout = QHBoxLayout(info_box); info_layout.setContentsMargins(0, 0, 0, 0); self.label_eval = QLabel(""); self.label_eval.setStyleSheet(STYLE_LABEL_EVAL); info_layout.addStretch(); info_layout.addWidget(self.label_eval); p_ana_layout.addWidget(info_box)
        self.opening_tree = OpeningTreeTable(); self.opening_tree.perf_threshold = self.perf_threshold
        self.opening_tree.move_selected.connect(lambda uci: self.game.make_move(chess.Move.from_uci(uci)))
        self.opening_tree.move_hovered.connect(self.board_ana.set_hover_move)
        self.opening_tree.label_global_stats.clicked.connect(self.open_search)
        self.opening_tree.reference_changed.connect(self.change_reference_db)
        p_ana_layout.addWidget(self.opening_tree)
        
        self.tabs_side = QTabWidget()
        tab_notacion = QWidget(); layout_notacion = QVBoxLayout(tab_notacion); layout_notacion.setContentsMargins(0,0,0,0); self.hist_ana = QTextBrowser(); self.hist_ana.setOpenLinks(False); self.hist_ana.anchorClicked.connect(self.jump_to_move_link); layout_notacion.addWidget(self.hist_ana)
        
        game_actions_layout = QHBoxLayout(); game_actions_layout.setContentsMargins(5, 5, 5, 5); game_actions_layout.setSpacing(8)
        self.btn_save_game = QPushButton(qta.icon('fa5s.save', color='#1976d2'), " Guardar")
        self.btn_save_game.setStyleSheet(STYLE_ACTION_BUTTON); self.btn_save_game.setToolTip("Añadir partida a la base activa (Ctrl+S para persistir)"); self.btn_save_game.clicked.connect(self.add_current_game_to_db)
        self.btn_new = QPushButton(qta.icon('fa5s.file-alt', color='#555'), " Nueva"); self.btn_new.setStyleSheet(STYLE_ACTION_BUTTON); self.btn_new.clicked.connect(self.start_new_game)
        game_actions_layout.addWidget(self.btn_save_game); game_actions_layout.addStretch(); game_actions_layout.addWidget(self.btn_new); layout_notacion.addLayout(game_actions_layout)
        self.tabs_side.addTab(tab_notacion, qta.icon('fa5s.list-ol'), "Notación")
        
        tab_grafico = QWidget(); layout_grafico = QVBoxLayout(tab_grafico); layout_grafico.setContentsMargins(0,0,0,0); self.eval_graph = EvaluationGraph(); self.eval_graph.move_selected.connect(self.game.jump_to_move); layout_grafico.addWidget(self.eval_graph); self.tabs_side.addTab(tab_grafico, qta.icon('fa5s.chart-area'), "Gráfico")
        self.analysis_report = AnalysisReport(); self.tabs_side.addTab(self.analysis_report, qta.icon('fa5s.chart-pie'), "Informe"); p_ana_layout.addWidget(self.tabs_side)
        
        btn_analyze = QPushButton(qta.icon('fa5s.magic', color='#673ab7'), " Analizar Partida Completa"); btn_analyze.setStyleSheet(STYLE_ACTION_BUTTON); btn_analyze.clicked.connect(self.start_full_analysis); p_ana_layout.addWidget(btn_analyze)
        ana_layout.addWidget(panel_ana, 1)

        # TAB 2: GESTOR
        self.tab_db = QWidget(); self.tabs.addTab(self.tab_db, "Gestor Bases")
        db_layout = QHBoxLayout(self.tab_db); self.db_sidebar = DBSidebar(); self.db_sidebar.new_db_requested.connect(self.create_new_db); self.db_sidebar.open_db_requested.connect(self.open_parquet_file); self.db_sidebar.search_requested.connect(self.open_search); self.db_sidebar.invert_filter_requested.connect(self.trigger_invert_filter); self.db_sidebar.clear_filter_requested.connect(self.reset_filters); self.db_sidebar.db_switched.connect(self.switch_database_with_feedback); self.db_sidebar.readonly_toggled.connect(self.toggle_db_readonly_logic); self.db_sidebar.context_menu_requested.connect(self.on_db_list_context_menu); db_layout.addWidget(self.db_sidebar, 1)
        db_content = QVBoxLayout(); self.db_table = self.create_scid_table(["ID", "Fecha", "Blancas", "Elo B", "Negras", "Elo N", "Res"]); self.db_table.itemDoubleClicked.connect(self.load_game_from_list); self.db_table.customContextMenuRequested.connect(self.on_db_table_context_menu); self.db_table.verticalScrollBar().valueChanged.connect(self.on_db_scroll); db_content.addWidget(self.db_table); db_layout.addLayout(db_content, 4)
        
        # TAB 3: EJERCICIOS
        self.tab_puzzles = PuzzleBrowserWidget(self); self.tabs.addTab(self.tab_puzzles, "Ejercicios")
        
        self.progress = QProgressBar(); self.progress.setMaximumWidth(150); self.progress.setFixedHeight(14); self.progress.setTextVisible(True); self.progress.setVisible(False)
        self.btn_stop_op = QPushButton(qta.icon('fa5s.times-circle', color='#c62828'), ""); self.btn_stop_op.setFixedWidth(24); self.btn_stop_op.setFixedHeight(24); self.btn_stop_op.setFlat(True); self.btn_stop_op.setVisible(False); self.btn_stop_op.clicked.connect(self.stop_current_operation)
        self.statusBar().addPermanentWidget(self.progress); self.statusBar().addPermanentWidget(self.btn_stop_op)
        self.search_criteria = {"white": "", "black": "", "min_elo": "", "result": "Cualquiera"}

    def start_full_analysis(self):
        if self.action_engine.isChecked(): self.action_engine.toggle(); self.toggle_engine(False)
        self.board_ana.setEnabled(False); self.opening_tree.setEnabled(False)
        self.progress.setRange(0, 100); self.progress.setValue(0); self.progress.show()
        total_moves = len(self.game.full_mainline) + 1; self.game_evals = [0] * total_moves; self.eval_graph.set_evaluations(self.game_evals)
        self.analysis_worker = FullAnalysisWorker(self.game.full_mainline, depth=self.engine_depth, engine_path=self.engine_path)
        self.analysis_worker.progress.connect(lambda curr, total: self.progress.setValue(int((curr/total)*100)))
        self.analysis_worker.analysis_result.connect(self.on_analysis_update); self.analysis_worker.finished.connect(self.on_analysis_finished); self.analysis_worker.start()

    def on_analysis_update(self, idx, cp_score):
        if 0 <= idx < len(self.game_evals): self.game_evals[idx] = cp_score; self.eval_graph.set_evaluations(self.game_evals)

    def on_analysis_finished(self):
        self.progress.hide(); self.board_ana.setEnabled(True); self.opening_tree.setEnabled(True)
        w_name = getattr(self, 'current_white', "Blancas"); b_name = getattr(self, 'current_black', "Negras")
        self.analysis_report.update_stats(self.game_evals, [m.uci() for m in self.game.full_mainline], w_name, b_name)

    def toggle_engine(self, checked):
        self.eval_bar.setVisible(checked)
        if checked:
            self.engine_worker = EngineWorker(engine_path=self.engine_path, threads=self.engine_threads, hash_mb=self.engine_hash, depth_limit=self.engine_depth)
            self.engine_worker.info_updated.connect(self.on_engine_update); self.engine_worker.update_position(self.game.board.fen()); self.engine_worker.start()
        else:
            if hasattr(self, 'engine_worker'): self.engine_worker.stop(); self.engine_worker.wait()
            self.label_eval.setText(""); self.board_ana.set_engine_move(None)

    def on_engine_update(self, eval_str, best_move, mainline):
        if not getattr(self, 'is_dragging', False): self.board_ana.set_engine_move(best_move if best_move else None)
        try:
            self.label_eval.setText(eval_str)
            if "M" in eval_str: val = 1000 if "+" in eval_str or eval_str[0].isdigit() or (eval_str.startswith("M") and not eval_str.startswith("-M")) else -1000; cp_val = 2000 if val > 0 else -2000
            else: 
                v = eval_str.split("|")[-1].strip(); val = int(float(v) * 100); cp_val = val
            self.eval_bar.setValue(val)
            if 0 <= self.game.current_idx < len(self.game_evals): self.game_evals[self.game.current_idx] = cp_val; self.eval_graph.set_evaluations(self.game_evals)
        except: pass

    def closeEvent(self, event):
        if hasattr(self, 'engine_worker'): self.engine_worker.stop()
        super().closeEvent(event)

    def create_scid_table(self, headers):
        table = QTableWidget(0, len(headers)); table.setHorizontalHeaderLabels(headers); table.setEditTriggers(QAbstractItemView.NoEditTriggers); table.setSelectionBehavior(QAbstractItemView.SelectRows); table.setContextMenuPolicy(Qt.CustomContextMenu); table.verticalHeader().setVisible(False); table.verticalHeader().setDefaultSectionSize(22); table.setStyleSheet(STYLE_TABLE_HEADER); table.horizontalHeader().sectionClicked.connect(self.sort_database); return table

    def setup_toolbar(self, toolbar):
        toolbar.setToolButtonStyle(Qt.ToolButtonIconOnly); left_spacer = QWidget(); left_spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred); toolbar.addWidget(left_spacer)
        self.action_search_pos = QAction(qta.icon('fa5s.crosshairs'), "", self); self.action_search_pos.triggered.connect(self.search_current_position); toolbar.addAction(self.action_search_pos); toolbar.addSeparator()
        for icon, func, tip in [(qta.icon('fa5s.step-backward'), self.game.go_start, "Inicio"), (qta.icon('fa5s.chevron-left'), self.game.step_back, "Anterior"), (qta.icon('fa5s.chevron-right'), self.game.step_forward, "Anterior"), (qta.icon('fa5s.step-forward'), self.game.go_end, "Final"), (None, None, None), (qta.icon('fa5s.retweet'), self.flip_boards, "Girar")]:
            if icon is None: toolbar.addSeparator()
            else: act = toolbar.addAction(icon, ""); act.triggered.connect(func)
        self.action_engine = QAction(qta.icon('fa5s.microchip'), "", self); self.action_engine.setCheckable(True); self.action_engine.triggered.connect(self.toggle_engine); toolbar.addAction(self.action_engine); right_spacer = QWidget(); right_spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred); toolbar.addWidget(right_spacer)

    def toggle_engine_shortcut(self): self.action_engine.toggle(); self.toggle_engine(self.action_engine.isChecked())
    def start_new_game(self): self.last_pos_count = 1000000; self.game.load_uci_line(""); self.game_header.clear_info()
    def load_game_from_list(self, item):
        game_id = self.db_table.item(item.row(), 0).data(Qt.UserRole); row = self.db.get_game_by_id(self.db.active_db_name, game_id)
        if row: self.game.load_uci_line(row["full_line"]); self.game.go_start(); self.game_header.update_info(row); self.tabs.setCurrentIndex(0)
    def jump_to_move_link(self, url): self.game.jump_to_move(int(url.toString()))

    def update_ui(self):
        if hasattr(self, 'opening_tree'): self.opening_tree.clear_selection()
        self.board_ana.update_board(); self.update_stats()
        l = len(self.game.full_mainline)
        if len(self.game_evals) != l + 1: self.game_evals = [None] * (l + 1)
        self.eval_graph.set_evaluations(self.game_evals); self.eval_graph.set_current_move(self.game.current_idx)
        if hasattr(self, 'engine_worker') and self.engine_worker.isRunning(): self.engine_worker.update_position(self.game.board.fen())
        temp = chess.Board(); html = "<style>a { text-decoration: none; color: #222; } .active { background-color: #f6f669; }</style>"
        for i, m in enumerate(self.game.full_mainline):
            san = temp.san(m); num = (i//2)+1; 
            if i % 2 == 0: html += f"<b>{num}.</b> "
            st = "class='active'" if i == self.game.current_idx - 1 else ""
            html += f"<a {st} href='{i+1}'>{san}</a> "; temp.push(m)
        self.hist_ana.setHtml(html)

    def run_stats_worker(self):
        current_hash = chess.polyglot.zobrist_hash(self.game.board)
        # PARADA INTELIGENTE
        if self.game.board.fen() != chess.STARTING_FEN:
            parent = self.game.board.copy()
            if parent.move_stack:
                parent.pop(); p_hash = chess.polyglot.zobrist_hash(parent); ref_path = self.db.get_reference_path()
                p_stats, _ = self.db.get_cached_stats(p_hash)
                if p_stats is None and ref_path: p_stats, _ = self.app_db.get_opening_stats(ref_path, p_hash)
                if p_stats is not None and not p_stats.is_empty() and "c" in p_stats.columns:
                    if p_stats["c"].sum() <= 10:
                        self.opening_tree.update_tree(None, self.game.board, "")
                        self.db.cache_stats(current_hash, pl.DataFrame(schema={"uci": pl.String, "c": pl.UInt32}), None)
                        return

        cached_res, cached_eval = self.db.get_cached_stats(current_hash)
        if cached_res is not None and "uci" in cached_res.columns:
            self.on_stats_finished(cached_res, cached_eval); return

        self.opening_tree.set_loading(True); self.progress.setRange(0, 100); self.progress.setValue(0); self.progress.show()
        self.stats_worker = StatsWorker(self.db, self.game.current_line_uci, self.game.board.turn == chess.WHITE, current_hash, app_db=self.app_db)
        self.stats_worker.progress.connect(self.progress.setValue); self.stats_worker.finished.connect(self.on_stats_finished); self.stats_worker.start()

    def update_stats(self): self.stats_timer.start(50)

    def on_stats_finished(self, res, engine_eval):
        self.progress.hide(); opening_name, _ = self.eco.get_opening_name(self.game.current_line_uci)
        next_move = self.game.full_mainline[self.game.current_idx].uci() if self.game.current_idx < len(self.game.full_mainline) else None
        
        branch_evals = {}
        if res is not None and not res.is_empty() and "c" in res.columns:
            self.last_pos_count = res["c"].sum()
            ref_path = self.db.get_reference_path()
            if ref_path:
                for move_uci in res["uci"]:
                    try:
                        temp_b = self.game.board.copy(); temp_b.push_uci(move_uci); h = chess.polyglot.zobrist_hash(temp_b)
                        _, score = self.app_db.get_opening_stats(ref_path, h)
                        if score is not None: branch_evals[move_uci] = score
                    except: continue
            self.opening_tree.update_tree(res, self.game.board, opening_name, total_view_count=self.last_pos_count, next_move_uci=next_move, engine_eval=engine_eval)
            if branch_evals: self.opening_tree.update_branch_evals(branch_evals, self.game.board.turn == chess.WHITE)
            if res is not None and not res.is_empty(): self.start_tree_scanner(res["uci"].to_list())
        else:
            self.last_pos_count = 0
            self.opening_tree.update_tree(res, self.game.board, opening_name, engine_eval=engine_eval)

    def start_tree_scanner(self, moves_uci):
        if hasattr(self, 'tree_scanner') and self.tree_scanner.isRunning():
            try:
                self.tree_scanner.eval_ready.disconnect()
                self.tree_scanner.stop()
                self.tree_scanner.wait()
            except: pass
            
        from src.core.engine_worker import TreeScannerWorker
        self.tree_scanner = TreeScannerWorker(self.engine_path, self.game.board.fen(), moves_uci, depth=self.tree_depth)
        self.tree_scanner.eval_ready.connect(self.on_tree_scan_result)
        self.tree_scanner.start()

    def on_tree_scan_result(self, uci, score_str):
        if not hasattr(self, 'opening_tree') or self.opening_tree.isHidden(): return
        self.opening_tree.update_branch_evals({uci: score_str}, self.game.board.turn == chess.WHITE)
        try:
            num_score = (10.0 if not score_str.startswith("-") else -10.0) if "M" in score_str else float(score_str)
            ref_path = self.db.get_reference_path()
            if ref_path:
                temp_b = self.game.board.copy(); temp_b.push_uci(uci); h = chess.polyglot.zobrist_hash(temp_b)
                self.app_db.update_opening_eval(ref_path, h, num_score)
        except: pass

    def change_reference_db(self, display_name):
        idx = self.opening_tree.combo_ref.currentIndex()
        real_name = self.opening_tree.combo_ref.itemData(idx) or display_name
        if self.db.set_reference_db(real_name): self.last_pos_count = 1000000; self.update_stats()

    def refresh_reference_combo(self):
        if not hasattr(self, 'opening_tree'): return
        self.opening_tree.combo_ref.blockSignals(True); current = self.opening_tree.combo_ref.currentText(); self.opening_tree.combo_ref.clear(); self.opening_tree.combo_ref.addItem("Base Activa")
        for name in sorted(self.db.dbs.keys()): self.opening_tree.combo_ref.addItem(name.replace(".parquet", ""), name)
        idx = self.opening_tree.combo_ref.findText(current)
        if idx >= 0: self.opening_tree.combo_ref.setCurrentIndex(idx)
        else: self.opening_tree.combo_ref.setCurrentIndex(0)
        self.opening_tree.combo_ref.blockSignals(False)

    def _create_action(self, text, icon_name, shortcut="", slot=None, tip="", color=None, is_checkable=False):
        icon = qta.icon(icon_name, color=color) if icon_name else None; action = QAction(icon, text, self)
        if shortcut: action.setShortcut(QKeySequence(shortcut)); action.setShortcutContext(Qt.WindowShortcut)
        if slot: action.triggered.connect(slot)
        if tip: action.setStatusTip(tip)
        if is_checkable: action.setCheckable(True)
        return action

    def init_menu(self):
        menubar = self.menuBar()
        file_menu = menubar.addMenu("&Archivo")
        file_menu.addAction(self._create_action("Nueva Base...", 'fa5s.plus-circle', "Ctrl+N", self.create_new_db))
        file_menu.addAction(self._create_action("Abrir Base...", 'fa5s.folder-open', "Ctrl+O", self.open_parquet_file))
        self.save_action = self._create_action("Guardar Base Activa", 'fa5s.save', "Ctrl+S", self.save_to_active_db)
        file_menu.addAction(self.save_action); file_menu.addSeparator(); file_menu.addAction(self._create_action("Configuración...", 'fa5s.cog', slot=self.open_settings))
        
        db_menu = menubar.addMenu("&Base de Datos")
        import_menu = db_menu.addMenu(qta.icon('fa5s.file-import'), "&Importar")
        import_menu.addAction(self._create_action("PGN a Nueva Base...", 'fa5s.file-code', "Ctrl+I", self.import_pgn))
        import_menu.addAction(self._create_action("Añadir PGN a Base Activa...", 'fa5s.file-medical', slot=self.append_pgn_to_current_db))
        db_menu.addAction(self._create_action("Filtrar...", 'fa5s.search', "Ctrl+F", self.open_search))
        db_menu.addAction(self._create_action("Calentar Caché...", 'fa5s.fire', slot=self.warm_up_opening_cache, color='#e65100'))
        db_menu.addAction(self._create_action("Quitar Filtros", 'fa5s.eraser', "Ctrl+L", self.reset_filters, color='red'))
        
        board_menu = menubar.addMenu("&Tablero")
        board_menu.addAction(self._create_action("Girar Tablero", 'fa5s.retweet', "F", self.flip_boards))
        self.action_engine.setText("Activar Motor"); board_menu.addAction(self.action_engine)
        
        help_menu = menubar.addMenu("&Ayuda"); help_menu.addAction(self._create_action("Acerca de...", 'fa5s.info-circle', slot=self.show_about_dialog))

    def create_new_db(self):
        path, _ = QFileDialog.getSaveFileName(self, "Crear Nueva Base", "", "Chess Parquet (*.parquet)")
        if path: self.db.create_new_database(path); self.load_parquet(path)

    def open_parquet_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Abrir Parquet", "", "Chess Parquet (*.parquet)")
        if path: self.load_parquet(path)

    def load_parquet(self, path):
        name = self.db.load_parquet(path)
        if name:
            it = self.db_sidebar.add_db_item(name); self.db_sidebar.list_widget.setCurrentItem(it)
            self.save_config()
        return name

    def save_to_active_db(self):
        if self.db.save_active_db(): self.statusBar().showMessage("Base guardada en disco", 3000)

    def add_current_game_to_db(self):
        if not self.db.active_db_name: return
        import chess.polyglot; b = chess.Board(); fens = [chess.polyglot.zobrist_hash(b)]
        for m in self.game.full_mainline: b.push(m); fens.append(chess.polyglot.zobrist_hash(b))
        data = {"id": int(time.time()*1000), "white": "Jugador", "black": "Oponente", "w_elo": 0, "b_elo": 0, "result": "*", "date": datetime.now().strftime("%Y.%m.%d"), "event": "Local", "site": "", "line": " ".join([m.uci() for m in self.game.full_mainline[:12]]), "full_line": self.game.current_line_uci, "fens": fens}
        if self.db.add_game(self.db.active_db_name, data): self.refresh_db_list()

    def open_settings(self):
        cfg = {"color_light": self.board_ana.color_light, "color_dark": self.board_ana.color_dark, "perf_threshold": self.perf_threshold, "engine_path": self.engine_path, "engine_threads": self.engine_threads, "engine_hash": self.engine_hash, "engine_depth": self.engine_depth, "tree_depth": self.tree_depth, "min_games": self.min_games, "venom_eval": self.venom_eval, "venom_win": self.venom_win, "practical_win": self.practical_win}
        dialog = SettingsDialog(cfg, self)
        if dialog.exec_():
            n = dialog.get_config(); self.board_ana.color_light = n["color_light"]; self.board_ana.color_dark = n["color_dark"]; self.board_ana.update_board()
            self.perf_threshold = n["perf_threshold"]; self.engine_path = n["engine_path"]; self.engine_threads = n["engine_threads"]; self.engine_hash = n["engine_hash"]; self.engine_depth = n["engine_depth"]; self.tree_depth = n["tree_depth"]
            self.venom_eval = n["venom_eval"]; self.venom_win = n["venom_win"]; self.practical_win = n["practical_win"]
            self.opening_tree.perf_threshold = self.perf_threshold; self.opening_tree.venom_eval = self.venom_eval; self.opening_tree.venom_win = self.venom_win; self.opening_tree.practical_win = self.practical_win
            self.save_config()
            if self.action_engine.isChecked(): self.toggle_engine(False); self.toggle_engine(True)

    def load_config(self):
        self.def_light, self.def_dark = "#eeeed2", "#8ca2ad"
        self.perf_threshold = self.app_db.get_config("perf_threshold", 25)
        colors = self.app_db.get_config("colors")
        if colors: self.def_light = colors.get("light", self.def_light); self.def_dark = colors.get("dark", self.def_dark)
        self.engine_path = self.app_db.get_config("engine_path", "/usr/bin/stockfish")
        self.engine_threads = self.app_db.get_config("engine_threads", 1); self.engine_hash = self.app_db.get_config("engine_hash", 64); self.engine_depth = self.app_db.get_config("engine_depth", 10); self.tree_depth = self.app_db.get_config("tree_depth", 12); self.min_games = self.app_db.get_config("min_games", 20)
        self.venom_eval = self.app_db.get_config("venom_eval", 0.5); self.venom_win = self.app_db.get_config("venom_win", 52); self.practical_win = self.app_db.get_config("practical_win", 60)
        self.pending_dbs = self.app_db.get_config("open_dbs", [])
        self.pending_active_db = self.app_db.get_config("active_db", None)

    def save_config(self):
        dbs = [m["path"] for m in self.db.db_metadata.values() if m.get("path")]
        self.app_db.set_config("open_dbs", dbs)
        self.app_db.set_config("active_db", self.db.active_db_name)
        self.app_db.set_config("colors", {"light": self.board_ana.color_light, "dark": self.board_ana.color_dark})
        self.app_db.set_config("engine_path", self.engine_path); self.app_db.set_config("engine_threads", self.engine_threads); self.app_db.set_config("engine_hash", self.engine_hash); self.app_db.set_config("engine_depth", self.engine_depth); self.app_db.set_config("tree_depth", self.tree_depth); self.app_db.set_config("min_games", self.min_games); self.app_db.set_config("venom_eval", self.venom_eval); self.app_db.set_config("venom_win", self.venom_win); self.app_db.set_config("practical_win", self.practical_win); self.app_db.set_config("perf_threshold", self.perf_threshold)

    def refresh_db_list(self):
        if not self.db.active_db_name: return
        for i in range(self.db_sidebar.list_widget.count()):
            it = self.db_sidebar.list_widget.item(i)
            name = it.data(Qt.UserRole + 1)
            if not name: continue
            
            disp = name.replace(".parquet", "")
            is_readonly = self.db.db_metadata.get(name, {}).get("read_only", True)
            
            # Icono según estado
            if is_readonly:
                it.setIcon(qta.icon('fa5s.lock', color='#888'))
                it.setForeground(QColor("#888"))
            else:
                it.setIcon(qta.icon('fa5s.unlock', color='#2e7d32'))
                it.setForeground(QColor("#000"))
                
            if self.db.is_dirty(name):
                it.setText(f"{disp} *")
                it.setForeground(QColor("#1976d2"))
            else:
                it.setText(disp)
        
        df = self.db.current_filter_df if self.db.current_filter_df is not None else self.db.get_active_df()
        if df is not None:
            self.current_db_df = df; self.db_loaded_count = 0; self.db_table.setRowCount(0); self.load_more_db_rows()
            c = self.db.get_view_count(); t = self.db.get_active_count(); self.opening_tree.label_global_stats.setText(f"{format_qty(c)} / {format_qty(t)}"); self.db_sidebar.update_stats(format_qty(c), format_qty(t))

    def load_more_db_rows(self):
        if self.current_db_df is None: return
        s = self.db_loaded_count; e = min(s + self.db_batch_size, self.current_db_df.height)
        if s >= e: return
        self.db_table.setSortingEnabled(False); curr = self.db_table.rowCount(); self.db_table.setRowCount(curr + (e - s))
        chunk = self.current_db_df.slice(s, e - s)
        for i, r in enumerate(chunk.rows(named=True)):
            row = curr + i
            for col, key in enumerate(["id", "date", "white", "w_elo", "black", "b_elo", "result"]):
                self.db_table.setItem(row, col, QTableWidgetItem(str(r[key])))
            self.db_table.item(row, 0).setData(Qt.UserRole, r["id"])
        self.db_loaded_count = e; self.db_table.resizeColumnsToContents()

    def on_db_scroll(self, v):
        if v > self.db_table.verticalScrollBar().maximum() - 20: self.load_more_db_rows()
    def reset_filters(self): self.db.set_active_db(self.db.active_db_name)
    def trigger_invert_filter(self): pass
    def switch_database_with_feedback(self, n): self.db.set_active_db(n)
    def toggle_db_readonly_logic(self, it):
        n = it.data(Qt.UserRole + 1); ro = not self.db.db_metadata[n]["read_only"]; self.db.set_readonly(n, ro); self.refresh_db_list()
    def on_db_list_context_menu(self, pos, it):
        if not it: return
        m = QMenu(); n = it.data(Qt.UserRole + 1)
        if not self.db.db_metadata[n]["read_only"]: m.addAction("Persistir Base", self.save_to_active_db)
        m.addAction("Quitar de la lista", lambda: self.remove_database(it)).exec(self.db_sidebar.list_widget.mapToGlobal(pos))
    def remove_database(self, it):
        n = it.data(Qt.UserRole + 1); del self.db.dbs[n]; del self.db.db_metadata[n]; self.db_sidebar.list_widget.takeItem(self.db_sidebar.list_widget.row(it)); self.save_config()
    def on_db_table_context_menu(self, pos): pass
    def _fix_tab_buttons(self):
        for i in [0, 1, 2]:
            for side in [QTabBar.LeftSide, QTabBar.RightSide]:
                btn = self.tabs.tabBar().tabButton(i, side)
                if btn: btn.hide(); self.tabs.tabBar().setTabButton(i, side, None)
    def close_tab(self, i):
        if i > 2: self.tabs.removeTab(i)
    def show_about_dialog(self): QMessageBox.about(self, "fa-chess", "Clon de Scid vs PC ultrarápido con Polars.")
    def import_pgn(self):
        p, _ = QFileDialog.getOpenFileName(self, "Importar PGN", "", "Chess PGN (*.pgn)")
        if p: self.progress.show(); self.worker = PGNWorker(p); self.worker.finished.connect(self.load_parquet); self.worker.start()
    def append_pgn_to_current_db(self): pass
    def sort_database(self, idx):
        col = self.col_mapping.get(idx)
        if col: self.sort_desc = not self.sort_desc; self.db.sort_active_db(col, self.sort_desc)
    def open_search(self):
        d = SearchDialog(self)
        if d.exec_(): self.db.filter_db(d.get_criteria())
    def warm_up_opening_cache(self):
        n = self.opening_tree.combo_ref.currentText(); t = self.db.get_active_count(); d = max(10, int(t * 0.005))
        if QMessageBox.question(self, "Calentar", f"¿Calentar {n} con umbral {d}?") == QMessageBox.Yes:
            self.progress.show(); self.btn_stop_op.show(); self.warm_worker = CachePopulatorWorker(self.db, self.app_db, min_games=d); self.warm_worker.finished.connect(self.on_warm_up_finished); self.warm_worker.start()
    def on_warm_up_finished(self, c): self.progress.hide(); self.btn_stop_op.hide(); QMessageBox.information(self, "Fin", f"Cacheadas {c} posiciones.")
    def stop_current_operation(self):
        if hasattr(self, 'warm_worker'): self.warm_worker.stop()
        self.btn_stop_op.hide()
