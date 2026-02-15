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
        
        # Gestor de Base de Datos de la App (SQLite)
        self.app_db = AppDBManager(APP_DB_FILE)
        
        # Gestores y Controladores
        self.db = DBManager()
        self.game = GameController()
        self.eco = ECOManager(ECO_FILE)
        
        # Estado de Ordenación
        self.sort_col = None
        self.sort_desc = False
        self.col_mapping = {0: "id", 1: "date", 2: "white", 3: "w_elo", 4: "black", 5: "b_elo", 6: "result"}
        
        self.game_evals = [] # Almacenar evaluaciones de la partida actual
        
        # Paginación para la tabla de bases
        self.db_batch_size = 100
        self.db_loaded_count = 0
        self.current_db_df = None
        self.last_pos_count = 1000000 # Inicializar alto para permitir el primer cálculo
        
        # Conectar señales del controlador de juego
        self.game.position_changed.connect(self.update_ui)
        
        # Conectar señales del gestor de base de datos
        self.db.active_db_changed.connect(self.refresh_db_list)
        self.db.active_db_changed.connect(self.update_stats)
        self.db.active_db_changed.connect(self.refresh_reference_combo)
        
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
        self.update_ui()
        self.tabs.setCurrentIndex(1) # Arrancar en la pestaña de Gestor de Bases
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

    def flip_boards(self):
        self.board_ana.flip()

    def search_current_position(self):
        pos_hash = chess.polyglot.zobrist_hash(self.game.board)
        df = self.db.get_active_df()
        if df is None:
            self.statusBar().showMessage("No hay ninguna base de datos activa", 3000)
            return
        
        QApplication.setOverrideCursor(Qt.WaitCursor)
        self.progress.setRange(0, 0)
        self.progress.show()
        self.statusBar().showMessage("Buscando posición...")
        QApplication.processEvents()

        try:
            self.search_criteria = {
                "white": "", "black": "", "min_elo": "", "result": "Cualquiera", 
                "position_hash": pos_hash,
                "use_position": True
            }
            filtered = self.db.filter_db(self.search_criteria)
            self.refresh_db_list(filtered)
        finally:
            QApplication.restoreOverrideCursor()

    def init_ui(self):
        self.tabs = QTabWidget()
        self.tabs.setTabsClosable(True)
        self.tabs.tabCloseRequested.connect(self.close_tab)
        
        # Ocultar botones de cierre en las pestañas fijas
        self.tabs.tabBar().setTabButton(0, QTabBar.RightSide, None)
        self.tabs.tabBar().setTabButton(1, QTabBar.RightSide, None)
        
        self.setCentralWidget(self.tabs)
        table_font = QFont("monospace", 9)

        # --- TAB 1: ANÁLISIS ---
        self.tab_analysis = QWidget()
        self.tabs.addTab(self.tab_analysis, "Tablero")
        ana_layout = QHBoxLayout(self.tab_analysis); ana_layout.setContentsMargins(0, 0, 0, 0)
        
        board_container = QWidget(); board_container_layout = QVBoxLayout(board_container)
        board_container_layout.setContentsMargins(0,0,0,0); board_container_layout.setSpacing(0)
        
        # Cabecera de Partida
        self.game_header = GameInfoHeader()
        board_container_layout.addWidget(self.game_header)

        self.toolbar_ana = QToolBar(); self.toolbar_ana.setMovable(False); self.setup_toolbar(self.toolbar_ana)
        board_container_layout.addWidget(self.toolbar_ana)
        
        board_eval_layout = QHBoxLayout(); board_eval_layout.setSpacing(5)
        self.eval_bar = QProgressBar(); self.eval_bar.setOrientation(Qt.Vertical); self.eval_bar.setRange(-1000, 1000); self.eval_bar.setValue(0); self.eval_bar.setTextVisible(False); self.eval_bar.setFixedWidth(15); self.eval_bar.setStyleSheet(STYLE_EVAL_BAR); self.eval_bar.setVisible(False)
        self.board_ana = ChessBoard(self.game.board, self); self.board_ana.color_light, self.board_ana.color_dark = self.def_light, self.def_dark
        self.board_ana.piece_drag_started.connect(lambda: setattr(self, 'is_dragging', True))
        self.board_ana.piece_drag_finished.connect(lambda: setattr(self, 'is_dragging', False))
        
        board_eval_layout.addWidget(self.eval_bar); board_eval_layout.addWidget(self.board_ana); board_container_layout.addLayout(board_eval_layout)
        ana_layout.addWidget(board_container)
        
        panel_ana = QWidget(); p_ana_layout = QVBoxLayout(panel_ana)
        
        # Info Box (Evaluación)
        info_box = QWidget(); info_layout = QHBoxLayout(info_box); info_layout.setContentsMargins(0, 0, 0, 0)
        self.label_eval = QLabel("")
        self.label_eval.setStyleSheet(STYLE_LABEL_EVAL)
        info_layout.addStretch()
        info_layout.addWidget(self.label_eval)
        p_ana_layout.addWidget(info_box)

        # Árbol de Aperturas
        self.opening_tree = OpeningTreeTable()
        self.opening_tree.perf_threshold = self.perf_threshold # Sincronizar configuración
        self.opening_tree.move_selected.connect(lambda uci: self.game.make_move(chess.Move.from_uci(uci)))
        self.opening_tree.move_hovered.connect(self.board_ana.set_hover_move)
        self.opening_tree.label_global_stats.clicked.connect(self.open_search)
        self.opening_tree.reference_changed.connect(self.change_reference_db)
        p_ana_layout.addWidget(self.opening_tree)
        
        self.tabs_side = QTabWidget()
        tab_notacion = QWidget(); layout_notacion = QVBoxLayout(tab_notacion); layout_notacion.setContentsMargins(0,0,0,0)
        self.hist_ana = QTextBrowser(); self.hist_ana.setOpenLinks(False); self.hist_ana.anchorClicked.connect(self.jump_to_move_link)
        layout_notacion.addWidget(self.hist_ana)
        
        # Barra de Acciones de Partida (Mejorada)
        game_actions_layout = QHBoxLayout()
        game_actions_layout.setContentsMargins(5, 5, 5, 5)
        game_actions_layout.setSpacing(8)
        
        self.btn_save = QPushButton(qta.icon('fa5s.save', color='#1976d2'), " Guardar")
        self.btn_save.setStyleSheet(STYLE_ACTION_BUTTON)
        self.btn_save.setToolTip("Persistir cambios: Guarda la base de datos activa en el disco (Ctrl+S)")
        self.btn_save.clicked.connect(self.save_to_active_db)
        
        self.btn_clip = QPushButton(qta.icon('fa5s.clipboard', color='#2e7d32'), " a Clipbase")
        self.btn_clip.setStyleSheet(STYLE_ACTION_BUTTON)
        self.btn_clip.setToolTip("Copiar a Clipbase: Añade la partida actual a la base temporal para edición (Ctrl+V para pegar)")
        self.btn_clip.clicked.connect(self.add_to_clipbase)
        
        self.btn_new = QPushButton(qta.icon('fa5s.file-alt', color='#555'), " Nueva")
        self.btn_new.setStyleSheet(STYLE_ACTION_BUTTON)
        self.btn_new.setToolTip("Nueva Partida: Limpia el tablero y empieza de cero")
        self.btn_new.clicked.connect(self.start_new_game)
        
        game_actions_layout.addWidget(self.btn_save)
        game_actions_layout.addWidget(self.btn_clip)
        game_actions_layout.addStretch()
        game_actions_layout.addWidget(self.btn_new)
        
        layout_notacion.addLayout(game_actions_layout)
        self.tabs_side.addTab(tab_notacion, qta.icon('fa5s.list-ol'), "Notación")
        
        tab_grafico = QWidget(); layout_grafico = QVBoxLayout(tab_grafico); layout_grafico.setContentsMargins(0,0,0,0)
        self.eval_graph = EvaluationGraph(); self.eval_graph.move_selected.connect(self.game.jump_to_move); layout_grafico.addWidget(self.eval_graph)
        self.tabs_side.addTab(tab_grafico, qta.icon('fa5s.chart-area'), "Gráfico")
        
        self.analysis_report = AnalysisReport(); self.tabs_side.addTab(self.analysis_report, qta.icon('fa5s.chart-pie'), "Informe")
        p_ana_layout.addWidget(self.tabs_side)
        
        btn_analyze = QPushButton(qta.icon('fa5s.magic', color='#673ab7'), " Analizar Partida Completa")
        btn_analyze.setStyleSheet(STYLE_ACTION_BUTTON)
        btn_analyze.clicked.connect(self.start_full_analysis)
        p_ana_layout.addWidget(btn_analyze)
        
        ana_layout.addWidget(panel_ana, 1)

        # --- TAB 2: GESTOR ---
        self.tab_db = QWidget(); self.tabs.addTab(self.tab_db, "Gestor Bases")
        db_layout = QHBoxLayout(self.tab_db)
        
        self.db_sidebar = DBSidebar()
        self.db_sidebar.add_db_item("Clipbase", is_clipbase=True)
        self.db_sidebar.new_db_requested.connect(self.create_new_db)
        self.db_sidebar.open_db_requested.connect(self.open_parquet_file)
        self.db_sidebar.search_requested.connect(self.open_search)
        self.db_sidebar.invert_filter_requested.connect(self.trigger_invert_filter)
        self.db_sidebar.clear_filter_requested.connect(self.reset_filters)
        self.db_sidebar.db_switched.connect(self.switch_database_with_feedback)
        self.db_sidebar.readonly_toggled.connect(self.toggle_db_readonly_logic)
        self.db_sidebar.context_menu_requested.connect(self.on_db_list_context_menu)
        db_layout.addWidget(self.db_sidebar, 1)
        
        db_content = QVBoxLayout()
        self.db_table = self.create_scid_table(["ID", "Fecha", "Blancas", "Elo B", "Negras", "Elo N", "Res"])
        self.db_table.itemDoubleClicked.connect(self.load_game_from_list)
        self.db_table.customContextMenuRequested.connect(self.on_db_table_context_menu)
        
        # CONECTAR SCROLL INFINITO
        self.db_table.verticalScrollBar().valueChanged.connect(self.on_db_scroll)
        
        db_content.addWidget(self.db_table); db_layout.addLayout(db_content, 4)
        
        # --- TAB 3: EJERCICIOS (Lichess DB) ---
        self.tab_puzzles = PuzzleBrowserWidget(self)
        self.tabs.addTab(self.tab_puzzles, "Ejercicios")
        
        self.progress = QProgressBar(); self.progress.setMaximumWidth(150); self.progress.setFixedHeight(14); self.progress.setTextVisible(True); self.progress.setVisible(False)
        self.statusBar().addPermanentWidget(self.progress)

        self.search_criteria = {"white": "", "black": "", "min_elo": "", "result": "Cualquiera"}
        pending = getattr(self, 'pending_dbs', [])
        for path in pending:
            if path and os.path.exists(path):
                self.load_parquet(path)
        
        self.refresh_reference_combo()

    def start_full_analysis(self):
        if self.action_engine.isChecked(): self.action_engine.toggle(); self.toggle_engine(False)
        self.board_ana.setEnabled(False); self.opening_tree.setEnabled(False)
        self.progress.setRange(0, 100); self.progress.setValue(0); self.progress.show()
        total_moves = len(self.game.full_mainline) + 1
        self.game_evals = [0] * total_moves; self.eval_graph.set_evaluations(self.game_evals)
        
        self.analysis_worker = FullAnalysisWorker(
            self.game.full_mainline,
            depth=self.engine_depth,
            engine_path=self.engine_path
        )
        
        self.analysis_worker.progress.connect(lambda curr, total: self.progress.setValue(int((curr/total)*100)))
        self.analysis_worker.analysis_result.connect(self.on_analysis_update)
        self.analysis_worker.finished.connect(self.on_analysis_finished)
        self.analysis_worker.error_occurred.connect(lambda msg: QMessageBox.critical(self, "Error de Análisis", msg))
        self.analysis_worker.start()
        self.statusBar().showMessage("Analizando partida completa...", 5000)

    def on_analysis_update(self, idx, cp_score):
        if 0 <= idx < len(self.game_evals):
            self.game_evals[idx] = cp_score; self.eval_graph.set_evaluations(self.game_evals)

    def on_analysis_finished(self):
        self.progress.hide(); self.board_ana.setEnabled(True); self.opening_tree.setEnabled(True)
        w_name = getattr(self, 'current_white', "Blancas"); b_name = getattr(self, 'current_black', "Negras")
        moves_uci = [m.uci() for m in self.game.full_mainline]
        self.analysis_report.update_stats(self.game_evals, moves_uci, w_name, b_name)
        self.statusBar().showMessage("Análisis completo finalizado", 3000)

    def toggle_engine(self, checked):
        self.eval_bar.setVisible(checked)
        if checked:
            self.engine_worker = EngineWorker(
                engine_path=self.engine_path,
                threads=self.engine_threads,
                hash_mb=self.engine_hash
            )
            self.engine_worker.info_updated.connect(self.on_engine_update)
            self.engine_worker.update_position(self.game.board.fen())
            self.engine_worker.start()
        else:
            if hasattr(self, 'engine_worker'): self.engine_worker.stop(); self.engine_worker.wait()
            self.label_eval.setText(""); self.board_ana.set_engine_move(None)

    def on_engine_update(self, eval_str, best_move, mainline):
        if not getattr(self, 'is_dragging', False): self.board_ana.set_engine_move(best_move if best_move else None)
        try:
            self.label_eval.setText(eval_str)
            if "M" in eval_str: val = 1000 if "+" in eval_str or eval_str[0].isdigit() or (eval_str.startswith("M") and not eval_str.startswith("-M")) else -1000; cp_val = 2000 if val > 0 else -2000
            else: val = int(float(eval_str) * 100); cp_val = val
            self.eval_bar.setValue(val)
            if 0 <= self.game.current_idx < len(self.game_evals): self.game_evals[self.game.current_idx] = cp_val; self.eval_graph.set_evaluations(self.game_evals)
        except: pass

    def closeEvent(self, event):
        if hasattr(self, 'engine_worker'): self.engine_worker.stop(); self.engine_worker.wait()
        super().closeEvent(event)

    def leaveEvent(self, event):
        if hasattr(self, 'board_ana') and not self.opening_tree.table.selectedItems(): self.board_ana.set_hover_move(None)
        super().leaveEvent(event)

    def create_scid_table(self, headers):
        table = QTableWidget(0, len(headers)); table.setHorizontalHeaderLabels(headers); table.setEditTriggers(QAbstractItemView.NoEditTriggers); table.setSelectionBehavior(QAbstractItemView.SelectRows); table.setContextMenuPolicy(Qt.CustomContextMenu); table.verticalHeader().setVisible(False); table.verticalHeader().setDefaultSectionSize(22); table.setStyleSheet(STYLE_TABLE_HEADER)
        table.setSortingEnabled(False); table.horizontalHeader().setSortIndicatorShown(True); table.horizontalHeader().sectionClicked.connect(self.sort_database)
        return table

    def setup_toolbar(self, toolbar):
        toolbar.setToolButtonStyle(Qt.ToolButtonIconOnly)
        
        left_spacer = QWidget(); left_spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred); toolbar.addWidget(left_spacer)
        self.action_search_pos = QAction(qta.icon('fa5s.crosshairs'), "", self); self.action_search_pos.setStatusTip("Buscar partidas con esta posición"); self.action_search_pos.triggered.connect(self.search_current_position); toolbar.addAction(self.action_search_pos); toolbar.addSeparator()
        actions = [(qta.icon('fa5s.step-backward'), self.game.go_start, "Inicio"), (qta.icon('fa5s.chevron-left'), self.game.step_back, "Anterior"), (qta.icon('fa5s.chevron-right'), self.game.step_forward, "Siguiente"), (qta.icon('fa5s.step-forward'), self.game.go_end, "Final"), (None, None, None), (qta.icon('fa5s.retweet'), self.flip_boards, "Girar Tablero")]
        for icon, func, tip in actions:
            if icon is None: toolbar.addSeparator()
            else: action = toolbar.addAction(icon, ""); action.triggered.connect(func); action.setStatusTip(tip)
        self.action_engine = QAction(qta.icon('fa5s.microchip', color='#444'), "", self); self.action_engine.setCheckable(True); self.action_engine.triggered.connect(self.toggle_engine); toolbar.addAction(self.action_engine)
        right_spacer = QWidget(); right_spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred); toolbar.addWidget(right_spacer)

    def toggle_engine_shortcut(self): self.action_engine.toggle(); self.toggle_engine(self.action_engine.isChecked())

    def start_new_game(self):
        self.last_pos_count = 1000000 # Resetear para permitir cálculo inicial
        self.game.load_uci_line(""); self.game_header.clear_info()

    def load_game_from_list(self, item):
        game_id = self.db_table.item(item.row(), 0).data(Qt.UserRole)
        row = self.db.get_game_by_id(self.db.active_db_name, game_id)
        if row:
            self.game.load_uci_line(row["full_line"]); self.game.go_start()
            self.game_header.update_info(row); self.tabs.setCurrentIndex(0)

    def change_reference_db(self, name):
        if self.db.set_reference_db(name):
            self.last_pos_count = 1000000 # Forzar que se calcule la posición actual con la nueva base
            self.update_stats()

    def refresh_reference_combo(self):
        """Actualiza la lista de bases disponibles en el combo del árbol"""
        if not hasattr(self, 'opening_tree'): return
        self.opening_tree.combo_ref.blockSignals(True)
        current = self.opening_tree.combo_ref.currentText()
        self.opening_tree.combo_ref.clear()
        self.opening_tree.combo_ref.addItem("Base Activa")
        
        # Añadir todas las bases abiertas (excepto Clipbase si ya es la activa, para no duplicar info mentalmente)
        for name in sorted(self.db.dbs.keys()):
            self.opening_tree.combo_ref.addItem(name)
            
        # Intentar restaurar la selección
        idx = self.opening_tree.combo_ref.findText(current)
        if idx >= 0: self.opening_tree.combo_ref.setCurrentIndex(idx)
        else: self.opening_tree.combo_ref.setCurrentIndex(0)
        
        self.opening_tree.combo_ref.blockSignals(False)

    def jump_to_move_link(self, url): self.game.jump_to_move(int(url.toString()))

    def update_ui(self):
        if hasattr(self, 'opening_tree'): self.opening_tree.clear_selection()
        self.board_ana.update_board(); self.update_stats()
        current_len = len(self.game.full_mainline)
        if len(self.game_evals) != current_len + 1:
            if abs(len(self.game_evals) - (current_len + 1)) > 1: self.game_evals = [None] * (current_len + 1)
            elif len(self.game_evals) < current_len + 1: self.game_evals.extend([None] * (current_len + 1 - len(self.game_evals)))
            else: self.game_evals = self.game_evals[:current_len + 1]
        self.eval_graph.set_evaluations(self.game_evals); self.eval_graph.set_current_move(self.game.current_idx)
        if hasattr(self, 'engine_worker') and self.engine_worker.isRunning(): self.engine_worker.update_position(self.game.board.fen())
        temp = chess.Board(); html = "<style>a { text-decoration: none; color: #222; } .active { background-color: #f6f669; }</style>"
        for i, m in enumerate(self.game.full_mainline):
            san_move = temp.san(m); num = (i//2)+1
            if i % 2 == 0: html += f"<b>{num}.</b> "
            st = "class='active'" if i == self.game.current_idx - 1 else ""
            html += f"<a {st} href='{i+1}'>{san_move}</a> "; temp.push(m)
        self.hist_ana.setHtml(html)

    def run_stats_worker(self):
        if not hasattr(self, '_active_workers'): self._active_workers = []
        self._active_workers = [w for w in self._active_workers if w.isRunning()]
        
        # --- ALGORITMO DE PARADA INTELIGENTE POR VOLUMEN ---
        # 1. La posición inicial SIEMPRE se calcula
        is_starting_pos = self.game.board.fen() == chess.STARTING_FEN
        
        if not is_starting_pos:
            # 2. Consultar si la posición PADRE tenía bajo volumen
            parent_board = self.game.board.copy()
            if parent_board.move_stack:
                parent_board.pop()
                p_hash = chess.polyglot.zobrist_hash(parent_board)
                ref_path = self.db.get_reference_path()
                
                # Buscamos en RAM o SQLite los datos del padre
                p_stats = self.db.get_cached_stats(p_hash)
                if p_stats is None and ref_path:
                    p_stats = self.app_db.get_opening_stats(ref_path, p_hash)
                
                # VALIDACIÓN: Si los datos del padre son antiguos (no tienen columna 'uci'), los ignoramos
                if p_stats is not None and "uci" not in p_stats.columns:
                    p_stats = None

                if p_stats is not None and not p_stats.is_empty() and "c" in p_stats.columns:
                    total_parent = p_stats["c"].sum()
                    if total_parent <= 10:
                        opening_name, _ = self.eco.get_opening_name(self.game.current_line_uci)
                        self.opening_tree.update_tree(None, self.game.board, opening_name)
                        
                        # PERSISTENCIA: Guardamos un DataFrame vacío en caché para esta posición
                        # para que los hijos sepan que aquí ya no había volumen.
                        empty_df = pl.DataFrame(schema=cached_res.schema if cached_res is not None else None)
                        self.db.cache_stats(current_hash, empty_df)
                        ref_path = self.db.get_reference_path()
                        if ref_path:
                            self.app_db.save_opening_stats(ref_path, current_hash, empty_df)
                            
                        self.statusBar().showMessage("Cálculo omitido: Variante sin volumen teórico", 2000)
                        return

        current_hash = chess.polyglot.zobrist_hash(self.game.board)
        
        # --- CONSULTA SÍNCRONA DE CACHÉ (INSTANTÁNEA) ---
        cached_res = self.db.get_cached_stats(current_hash)
        
        # Si los datos en caché son de la versión vieja (sin uci), forzamos recálculo
        if cached_res is not None and "uci" not in cached_res.columns:
            cached_res = None
            
        if cached_res is not None:
            # Si está en caché, actualizamos directamente sin spinner ni hilos
            self.on_stats_finished(cached_res)
            return

        if hasattr(self, 'stats_worker') and self.stats_worker.isRunning():
            try: self.stats_worker.finished.disconnect(); self._active_workers.append(self.stats_worker)
            except: pass
        
        # Solo mostramos el spinner si realmente vamos a calcular
        self.opening_tree.set_loading(True)
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.show()
        self.statusBar().showMessage("Calculando estadísticas...")
        
        self.stats_worker = StatsWorker(
            self.db, 
            self.game.current_line_uci, 
            self.game.board.turn == chess.WHITE, 
            current_hash, 
            app_db=self.app_db,
            min_games=self.min_games
        )
        self.stats_worker.progress.connect(self.progress.setValue)
        self.stats_worker.finished.connect(self.on_stats_finished); self.stats_worker.start()

    def update_stats(self):
        self.stats_timer.start(50)

    def on_stats_finished(self, res):
        self.progress.hide()
        is_filtered = self.db.current_filter_df is not None
        total_view = self.db.get_view_count()
        
        # get_opening_name ahora devuelve (nombre, profundidad)
        opening_name, _ = self.eco.get_opening_name(self.game.current_line_uci)
        
        # Identificar el siguiente movimiento en la partida actual para resaltarlo
        next_move_uci = None
        if self.game.current_idx < len(self.game.full_mainline):
            next_move_uci = self.game.full_mainline[self.game.current_idx].uci()

        # ACTUALIZAR EL CONTADOR DE VOLUMEN CON DATOS DE LA BASE DE REFERENCIA
        # Sumamos la columna 'c' (count) del resultado del árbol
        if res is not None and not res.is_empty() and "c" in res.columns:
            self.last_pos_count = res["c"].sum()
        else:
            self.last_pos_count = 0

        # Actualizar el árbol (él sí muestra datos de la posición)
        self.opening_tree.update_tree(res, self.game.board, opening_name, is_filtered, total_view, next_move_uci=next_move_uci)
        
        is_partial = False
        if res is not None and res.height > 0:
            is_partial = "_is_partial" in res.columns and res.row(0, named=True).get("_is_partial")
        
        if is_partial: 
            self.statusBar().showMessage("⚠️ Árbol parcial. El límite es de 1M de partidas.", 5000)
            # Solo en caso de árbol parcial mostramos error en el badge del árbol
            self.opening_tree.label_global_stats.setStyleSheet(STYLE_BADGE_ERROR)
        else: 
            self.statusBar().showMessage("Listo", 2000)

    def _create_action(self, text, icon_name, shortcut="", slot=None, tip="", color=None, is_checkable=False):
        """Helper factory para crear una QAction."""
        icon = qta.icon(icon_name, color=color) if icon_name else None
        action = QAction(icon, text, self)
        if shortcut:
            if isinstance(shortcut, str):
                action.setShortcut(QKeySequence.fromString(shortcut))
            else:
                action.setShortcut(shortcut)
            # Asegurar que el atajo funcione en toda la ventana
            action.setShortcutContext(Qt.WindowShortcut)
        if slot:
            action.triggered.connect(slot)
        if tip:
            action.setStatusTip(tip)
        if is_checkable:
            action.setCheckable(True)
        return action

    def _create_file_menu(self, menubar):
        file_menu = menubar.addMenu("&Archivo")
        
        file_menu.addAction(self._create_action(
            "&Nueva Base Vacía...", 'fa5s.plus-circle', "Ctrl+N", self.create_new_db,
            "Crear una nueva base de datos Parquet vacía"
        ))
        file_menu.addAction(self._create_action(
            "&Abrir Base de Datos...", 'fa5s.folder-open', "Ctrl+O", self.open_parquet_file,
            "Abrir una base de datos Parquet existente", color='#1976d2'
        ))
        
        self.save_action = self._create_action(
            "&Guardar Base Activa (Persistir)", 'fa5s.save', "Ctrl+S", self.save_to_active_db,
            "Persistir los cambios de la base activa en el disco", color='#1976d2'
        )
        file_menu.addAction(self.save_action)

        file_menu.addSeparator()
        file_menu.addAction(self._create_action(
            "&Configuración...", 'fa5s.cog', slot=self.open_settings
        ))
        file_menu.addSeparator()
        file_menu.addAction(self._create_action(
            "&Salir", 'fa5s.power-off', "Ctrl+Q", self.close
        ))

    def _create_database_menu(self, menubar):
        db_menu = menubar.addMenu("&Base de Datos")

        import_menu = db_menu.addMenu(qta.icon('fa5s.file-import'), "&Importar / Añadir")
        import_menu.addAction(self._create_action(
            "PGN a Nueva Base...", 'fa5s.file-code', "Ctrl+I", self.import_pgn,
            tip="Importar un archivo PGN a una nueva base Parquet", color='#2e7d32'
        ))
        import_menu.addAction(self._create_action(
            "Añadir PGN a Base Activa...", 'fa5s.file-medical', slot=self.append_pgn_to_current_db,
            tip="Añadir partidas de un PGN a la base de datos activa", color='#2e7d32'
        ))

        export_menu = db_menu.addMenu(qta.icon('fa5s.file-export'), "&Exportar")
        export_menu.addAction(self._create_action(
            "Base Activa a PGN...", 'fa5s.file-export', slot=self.export_full_db_to_pgn
        ))
        export_menu.addAction(self._create_action(
            "Filtro a PGN...", 'fa5s.filter', "Ctrl+E", self.export_filter_to_pgn
        ))
        export_menu.addAction(self._create_action(
            "Filtro a Parquet...", 'fa5s.file-download', slot=self.export_filter_to_parquet
        ))
        
        db_menu.addSeparator()
        
        db_menu.addAction(self._create_action(
            "&Filtrar Partidas...", 'fa5s.search', "Ctrl+F", self.open_search
        ))
        db_menu.addAction(self._create_action(
            "&Invertir Filtro", 'fa5s.exchange-alt', slot=self.trigger_invert_filter
        ))
        db_menu.addAction(self._create_action(
            "&Quitar Filtros", 'fa5s.eraser', "Ctrl+L", self.reset_filters, color='#c62828'
        ))
        
        db_menu.addSeparator()
        
        db_menu.addAction(self._create_action(
            "&Borrar Partidas Filtradas", 'fa5s.trash-alt', slot=self.delete_filtered_games_ui, color='#c62828'
        ))
        db_menu.addAction(self._create_action(
            "&Eliminar Archivo de Base...", 'fa5s.dumpster-fire', slot=self.delete_current_db_file, color='#c62828'
        ))

    def _create_edit_menu(self, menubar):
        edit_menu = menubar.addMenu("&Edición")
        edit_menu.addAction(self._create_action(
            "&Pegar PGN", 'fa5s.paste', QKeySequence.Paste, self.paste_pgn_to_clipbase,
            tip="Pegar partidas PGN desde el portapapeles a la Clipbase", color='#2e7d32'
        ))

    def _create_player_menu(self, menubar):
        player_menu = menubar.addMenu("&Jugador")
        player_menu.addAction(self._create_action(
            "Dossier de &Inteligencia...", 'fa5s.chart-bar', "Ctrl+D", self.prompt_player_report,
            tip="Generar un informe de rendimiento para un jugador", color='#673ab7'
        ))

    def _create_board_menu(self, menubar):
        board_menu = menubar.addMenu("&Tablero")
        board_menu.addAction(self._create_action(
            "&Girar Tablero", 'fa5s.retweet', "F", self.flip_boards
        ))
        
        # Esta acción se define en setup_toolbar, aquí solo la reutilizamos/sincronizamos
        # Le ponemos texto para que en el menú se vea bien
        self.action_engine.setText("&Análisis Infinito (Motor)")
        board_menu.addAction(self.action_engine)
        
        board_menu.addSeparator()
        board_menu.addAction(self._create_action(
            "&Analizar Partida Completa", 'fa5s.magic', slot=self.start_full_analysis, color='#673ab7'
        ))

    def _create_help_menu(self, menubar):
        help_menu = menubar.addMenu("&Ayuda")
        help_menu.addAction(self._create_action(
            "&Acerca de...", 'fa5s.info-circle', slot=self.show_about_dialog
        ))

    def init_menu(self):
        menubar = self.menuBar()
        self._create_file_menu(menubar)
        self._create_edit_menu(menubar)
        self._create_database_menu(menubar)
        self._create_player_menu(menubar)
        self._create_board_menu(menubar)
        self._create_help_menu(menubar)

    def show_about_dialog(self):
        about_text = "<h3>fa-chess</h3><p><b>Versión:</b> 1.0.0</p><p><b>Autor:</b> Fabio Rueda</p><hr><p>Un clon moderno y ligero de Scid vs. PC enfocado en el rendimiento masivo.</p><p><b>Tecnologías clave:</b><ul><li>Python & PySide6 (Qt)</li><li>Polars 1.x (Motor de datos ultra-rápido)</li><li>Python-Chess (Lógica de juego)</li></ul></p><p>© 2026 Fabio Rueda</p>"
        QMessageBox.about(self, "Acerca de fa-chess", about_text)

    def create_new_db(self):
        path, _ = QFileDialog.getSaveFileName(self, "Crear Nueva Base", "/data/chess", "Chess Parquet (*.parquet)")
        if path:
            if not path.endswith(".parquet"): path += ".parquet"
            self.db.create_new_database(path); self.load_parquet(path); self.statusBar().showMessage(f"Base creada y cargada: {os.path.basename(path)}", 3000)

    def delete_current_db_file(self):
        name = self.db.active_db_name
        if name == "Clipbase": QMessageBox.warning(self, "Acción no permitida", "No se puede eliminar el archivo de la Clipbase interna."); return
        ret = QMessageBox.question(self, "Eliminar Base", f"¿Estás seguro de que quieres eliminar FISICAMENTE el archivo de la base '{name}'?\n\nEsta acción no se puede deshacer.", QMessageBox.Yes | QMessageBox.No)
        if ret == QMessageBox.Yes:
            items = self.db_sidebar.list_widget.findItems(name, Qt.MatchExactly)
            if items:
                item = items[0]
                if self.db.delete_database_from_disk(name): self.db_sidebar.list_widget.takeItem(self.db_sidebar.list_widget.row(item)); self.save_config(); self.statusBar().showMessage(f"Archivo de base '{name}' eliminado", 3000)
                else: QMessageBox.critical(self, "Error", "No se pudo eliminar el archivo. Comprueba que no esté abierto por otro programa.")

    def add_to_clipbase(self):
        line_uci = self.game.current_line_uci
        import chess.polyglot; board = chess.Board(); hashes = [chess.polyglot.zobrist_hash(board)]
        for move in self.game.full_mainline: board.push(move); hashes.append(chess.polyglot.zobrist_hash(board))
        game_data = {"id": int(time.time()), "white": "Jugador Blanco", "black": "Jugador Negro", "w_elo": 2500, "b_elo": 2500, "result": "*", "date": datetime.now().strftime("%Y.%m.%d"), "event": "Análisis Local", "site": "", "line": line_uci, "full_line": line_uci, "fens": hashes}
        self.db.add_to_clipbase(game_data); self.statusBar().showMessage("Partida guardada en Clipbase", 3000)
        if self.db.active_db_name == "Clipbase": self.db.set_active_db("Clipbase")

    def paste_pgn_to_clipbase(self):
        """Lee el portapapeles y añade las partidas PGN encontradas a la Clipbase"""
        text = QApplication.clipboard().text()
        if not text:
            self.statusBar().showMessage("El portapapeles está vacío", 3000)
            return

        import chess.polyglot
        pgn_io = io.StringIO(text)
        games_added = 0
        
        while True:
            try:
                game = chess.pgn.read_game(pgn_io)
                if game is None:
                    break
                
                # Extraer datos básicos
                headers = game.headers
                white = headers.get("White", "Jugador Blanco")
                black = headers.get("Black", "Jugador Negro")
                result = headers.get("Result", "*")
                date = headers.get("Date", "????.??.??")
                event = headers.get("Event", "PGN Pegado")
                site = headers.get("Site", "")
                
                # Obtener Elos
                try: w_elo = int(headers.get("WhiteElo", 0))
                except: w_elo = 0
                try: b_elo = int(headers.get("BlackElo", 0))
                except: b_elo = 0
                
                # Generar línea UCI y hashes FEN
                board = game.board()
                moves_uci = []
                fens = [chess.polyglot.zobrist_hash(board)]
                
                for move in game.mainline_moves():
                    moves_uci.append(move.uci())
                    board.push(move)
                    fens.append(chess.polyglot.zobrist_hash(board))
                
                full_line = " ".join(moves_uci)
                # Línea corta para visualización rápida (primeros 12 movimientos)
                short_line = " ".join(moves_uci[:12])
                
                game_data = {
                    "id": int(time.time() * 1000) + games_added,
                    "white": white,
                    "black": black,
                    "w_elo": w_elo,
                    "b_elo": b_elo,
                    "result": result,
                    "date": date,
                    "event": event,
                    "site": site,
                    "line": short_line,
                    "full_line": full_line,
                    "fens": fens
                }
                
                self.db.add_to_clipbase(game_data)
                games_added += 1
            except Exception as e:
                logger.error(f"Error parseando PGN desde portapapeles: {e}")
                break

        if games_added > 0:
            self.statusBar().showMessage(f"Se han añadido {games_added} partidas a la Clipbase", 5000)
            if self.db.active_db_name == "Clipbase":
                self.refresh_db_list()
            else:
                # Opcional: preguntar si quiere cambiar a Clipbase
                ret = QMessageBox.question(self, "Partidas Añadidas", 
                                         f"Se han añadido {games_added} partidas a la Clipbase.\n¿Quieres ir a la Clipbase ahora?",
                                         QMessageBox.Yes | QMessageBox.No)
                if ret == QMessageBox.Yes:
                    self.db_sidebar.list_widget.setCurrentRow(0)
                    self.db.set_active_db("Clipbase")
        else:
            self.statusBar().showMessage("No se han encontrado partidas PGN válidas en el portapapeles", 3000)

    def delete_selected_game(self):
        row = self.db_table.currentRow()
        if row >= 0:
            game_id = self.db_table.item(row, 0).data(Qt.UserRole)
            if self.db.delete_game(self.db.active_db_name, game_id): self.db.set_active_db(self.db.active_db_name); self.statusBar().showMessage("Partida eliminada", 2000)

    def copy_selected_game(self):
        row = self.db_table.currentRow()
        if row < 0: return
        game_id = self.db_table.item(row, 0).data(Qt.UserRole); game_data = self.db.get_game_by_id(self.db.active_db_name, game_id)
        if not game_data: return
        writable_dbs = [name for name, meta in self.db.db_metadata.items() if not meta.get("read_only", True) or name == "Clipbase"]
        if not writable_dbs: QMessageBox.warning(self, "Copiar Partida", "No hay bases de datos con permiso de escritura abiertas."); return
        from PySide6.QtWidgets import QInputDialog
        target_db, ok = QInputDialog.getItem(self, "Copiar Partida", "Selecciona la base de datos destino:", writable_dbs, 0, False)
        if ok and target_db:
            from src.core.db_manager import GAME_SCHEMA
            game_data["id"] = int(time.time() * 1000)
            clean_data = {k: game_data[k] for k in GAME_SCHEMA.keys() if k in game_data}
            
            if target_db == "Clipbase": 
                self.db.add_to_clipbase(clean_data)
            else: 
                new_row = pl.DataFrame([clean_data], schema=GAME_SCHEMA).lazy()
                self.db.dbs[target_db] = pl.concat([self.db.dbs[target_db], new_row])
                
            self.statusBar().showMessage(f"Partida copiada a '{target_db}'", 3000)
            if self.db.active_db_name == target_db: self.db.set_active_db(target_db)

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

    def copy_to_clipbase(self):
        row = self.db_table.currentRow()
        if row >= 0:
            game_id = self.db_table.item(row, 0).data(Qt.UserRole); game_data = self.db.get_game_by_id(self.db.active_db_name, game_id)
            if game_data:
                game_data["id"] = int(time.time() * 1000); schema = self.db.dbs["Clipbase"].collect_schema(); clean_data = {k: game_data[k] for k in schema.names()}; self.db.add_to_clipbase(clean_data); self.statusBar().showMessage("Partida copiada a Clipbase", 2000)
                if self.db.active_db_name == "Clipbase": self.db.set_active_db("Clipbase")

    def on_db_table_context_menu(self, pos):
        selected_items = self.db_table.selectedItems()
        if not selected_items: return
        
        # Obtener IDs únicos de las filas seleccionadas
        selected_ids = sorted(list(set([self.db_table.item(item.row(), 0).data(Qt.UserRole) for item in selected_items])))
        count = len(selected_ids)
        
        # Si solo hay una fila, podemos ofrecer el informe del jugador
        white_player = ""
        black_player = ""
        if count == 1:
            row_idx = self.db_table.currentRow()
            white_player = self.db_table.item(row_idx, 2).text()
            black_player = self.db_table.item(row_idx, 4).text()

        menu = QMenu()
        
        if count == 1:
            report_white = QAction(qta.icon('fa5s.user', color='#1976d2'), f"Dossier de {white_player}", self)
            report_white.triggered.connect(lambda: self.show_player_report(white_player))
            menu.addAction(report_white)
            
            report_black = QAction(qta.icon('fa5s.user', color='#555'), f"Dossier de {black_player}", self)
            report_black.triggered.connect(lambda: self.show_player_report(black_player))
            menu.addAction(report_black)
            menu.addSeparator()

        # COPIAR (Soporta múltiple)
        copy_text = f"📋 Copiar {count} partidas a..." if count > 1 else "📋 Copiar Partida a..."
        copy_action = QAction(qta.icon('fa5s.copy'), copy_text, self)
        copy_action.triggered.connect(lambda: self.copy_selected_games_logic(selected_ids))
        menu.addAction(copy_action)

        if count == 1:
            edit_action = QAction(qta.icon('fa5s.edit'), "📝 Editar Datos Partida", self)
            edit_action.triggered.connect(self.edit_selected_game)
            menu.addAction(edit_action)

        # SECCIÓN DE BORRADO
        is_readonly = self.db.db_metadata.get(self.db.active_db_name, {}).get("read_only", True)
        if not is_readonly or self.db.active_db_name == "Clipbase":
            menu.addSeparator()
            del_text = f"❌ Eliminar {count} partidas seleccionadas" if count > 1 else "❌ Eliminar partida seleccionada"
            del_action = QAction(qta.icon('fa5s.trash-alt', color='#c62828'), del_text, self)
            del_action.triggered.connect(lambda: self.delete_selected_games_logic(selected_ids))
            menu.addAction(del_action)
        else:
            menu.addSeparator()
            locked_act = QAction(qta.icon('fa5s.lock', color='#888'), "Base de solo lectura (No se puede borrar)", self)
            locked_act.setEnabled(False)
            menu.addAction(locked_act)

        menu.exec(self.db_table.viewport().mapToGlobal(pos))

    def delete_selected_games_logic(self, ids):
        count = len(ids)
        ret = QMessageBox.warning(self, "Eliminar Partidas", 
                                f"¿Estás seguro de que quieres eliminar {count} partidas seleccionadas?\n\nEste cambio es reversible hasta que pulses 'Persistir Base'.",
                                QMessageBox.Yes | QMessageBox.No)
        
        if ret == QMessageBox.Yes:
            self.statusBar().showMessage(f"Eliminando {count} partidas...")
            for game_id in ids:
                self.db.delete_game(self.db.active_db_name, game_id)
            
            self.statusBar().showMessage(f"Eliminadas {count} partidas.", 3000)
            self.refresh_db_list()

    def copy_selected_games_logic(self, ids):
        active_db = self.db.active_db_name
        # Filtramos: bases RW que NO sean la base activa
        writable_dbs = [name for name, meta in self.db.db_metadata.items() 
                       if (not meta.get("read_only", True) or name == "Clipbase") and name != active_db]
        
        if not writable_dbs: 
            QMessageBox.warning(self, "Copiar Partidas", "No hay otras bases de datos con permiso de escritura abiertas.")
            return
            
        from PySide6.QtWidgets import QInputDialog
        target_db, ok = QInputDialog.getItem(self, "Copiar Partidas", f"Selecciona destino para las {len(ids)} partidas:", writable_dbs, 0, False)
        if ok and target_db:
            self.statusBar().showMessage(f"Copiando {len(ids)} partidas...")
            for game_id in ids:
                game_data = self.db.get_game_by_id(self.db.active_db_name, game_id)
                if game_data:
                    # Generar ID nuevo
                    game_data["id"] = int(time.time() * 1000) + ids.index(game_id)
                    from src.core.db_manager import GAME_SCHEMA
                    clean_data = {k: game_data[k] for k in GAME_SCHEMA.keys() if k in game_data}
                    
                    if target_db == "Clipbase":
                        self.db.add_to_clipbase(clean_data)
                    else:
                        new_row = pl.DataFrame([clean_data], schema=GAME_SCHEMA).lazy()
                        self.db.dbs[target_db] = pl.concat([self.db.dbs[target_db], new_row])
            
            self.statusBar().showMessage(f"Copiadas {len(ids)} partidas a {target_db}.", 3000)
            if self.db.active_db_name == target_db:
                self.refresh_db_list()

    def _fix_tab_buttons(self):
        """Elimina físicamente los botones de cierre de las pestañas fijas"""
        for i in [0, 1, 2]: # Tablero, Gestor, Ejercicios
            for side in [QTabBar.LeftSide, QTabBar.RightSide]:
                btn = self.tabs.tabBar().tabButton(i, side)
                if btn:
                    btn.hide()
                    self.tabs.tabBar().setTabButton(i, side, None)

    def prompt_player_report(self):
        from PySide6.QtWidgets import QInputDialog
        name, ok = QInputDialog.getText(self, "Dossier de Jugador", "Introduce el nombre exacto del jugador:")
        if ok and name:
            self.show_player_report(name)

    def close_tab(self, index):
        # Impedir cerrar las dos pestañas principales (Tablero y Gestor)
        if index > 1:
            # Primero cerramos la base de datos si es una base abierta en una pestaña
            widget = self.tabs.widget(index)
            if isinstance(widget, PlayerReportWidget): # Asumiendo que es el único tipo de pestaña dinámica
                db_name_to_close = widget.property("db_name") # Necesitaríamos guardar esto
                # self.db.close_db(db_name_to_close) # Necesitaríamos una función así
            self.tabs.removeTab(index)

    def show_player_report(self, player_name):
        if not player_name: return
        self.statusBar().showMessage(f"Generando dossier de inteligencia para {player_name}...")
        
        # Mostrar barra de progreso en modo pulso
        self.progress.setRange(0, 0)
        self.progress.show()
        
        QApplication.setOverrideCursor(Qt.WaitCursor)
        QApplication.processEvents() 
        try:
            stats = self.db.get_player_report(player_name, eco_manager=self.eco)
            
            # Ocultar barra y restaurar cursor
            self.progress.hide()
            QApplication.restoreOverrideCursor()
            
            if stats:
                report_w = PlayerReportWidget(stats, self)
                idx = self.tabs.addTab(report_w, qta.icon('fa5s.chart-bar'), f"Dossier: {player_name}")
                self._fix_tab_buttons()
                self.tabs.setCurrentIndex(idx)
                self.statusBar().showMessage(f"Dossier de {player_name} listo.", 3000)
            else:
                self.statusBar().showMessage("Informe cancelado: datos insuficientes.", 3000)
                QMessageBox.information(self, "Informe de Jugador", f"No se han encontrado suficientes datos para {player_name}")
        except Exception as e:
            self.progress.hide()
            QApplication.restoreOverrideCursor()
            self.statusBar().showMessage(f"Error al generar informe: {e}", 5000)
            QMessageBox.critical(self, "Error", f"Error al generar el informe: {e}")

    def toggle_db_readonly_logic(self, item):
        if not item: return
        name = item.text().replace("*", "").strip()
        if name == "Clipbase": return
        
        current_ro = self.db.db_metadata.get(name, {}).get("read_only", True)
        new_status = not current_ro
        
        if self.db.set_readonly(name, new_status):
            if new_status: 
                item.setIcon(qta.icon('fa5s.lock', color='#888888'))
                item.setForeground(QColor("#888888"))
            else: 
                item.setIcon(qta.icon('fa5s.unlock', color='#2e7d32'))
                item.setForeground(QColor("#000000"))
            
            self.statusBar().showMessage(f"Base '{name}': {'Solo Lectura' if new_status else 'Edición Habilitada'}", 3000)
            self.refresh_db_list()

    def on_db_list_context_menu(self, pos):
        item = self.db_sidebar.list_widget.itemAt(pos)
        if not item or item.text() == "Clipbase": return
        name = item.text().replace("*", "").strip()
        is_readonly = self.db.db_metadata.get(name, {}).get("read_only", True)
        menu = QMenu()
        
        if not is_readonly: 
            persist_action = QAction(qta.icon('fa5s.save', color='#1976d2'), " Persistir Base (Guardar en Disco)", self)
            persist_action.triggered.connect(self.save_to_active_db)
            menu.addAction(persist_action)

        menu.addSeparator()
        remove_action = QAction(qta.icon('fa5s.times', color='red'), " Quitar de la lista", self)
        remove_action.triggered.connect(lambda: self.remove_database(item))
        menu.addAction(remove_action)
        menu.exec(self.db_sidebar.list_widget.mapToGlobal(pos))

    def switch_database_with_feedback(self, name):
        """Cambia la base de datos activa mostrando una barra de progreso"""
        if self.db.active_db_name == name: return
        
        self.statusBar().showMessage(f"Cargando base de datos: {name}...")
        self.progress.setRange(0, 0) # Modo indeterminado
        self.progress.show()
        QApplication.setOverrideCursor(Qt.WaitCursor)
        QApplication.processEvents() # Forzar refresco
        
        try:
            self.db.set_active_db(name)
            self.statusBar().showMessage(f"Base '{name}' cargada.", 3000)
        finally:
            self.progress.hide()
            QApplication.restoreOverrideCursor()

    def remove_database(self, item):
        if not item: return
        name = item.text().replace("*", "").strip()
        if name in self.db.dbs:
            del self.db.dbs[name]
            if name in self.db.db_metadata: del self.db.db_metadata[name]
            
            # Quitar de la lista visual
            self.db_sidebar.list_widget.takeItem(self.db_sidebar.list_widget.row(item))
            
            if self.db.active_db_name == name: 
                self.db.set_active_db("Clipbase") # Volver a la base por defecto
            
            self.refresh_reference_combo()
            self.save_config()
            self.statusBar().showMessage(f"Base '{name}' quitada de la lista", 3000)

    def save_to_active_db(self):
        """Persiste los cambios actuales de la base activa en el disco"""
        db_name = self.db.active_db_name
        is_readonly = self.db.db_metadata.get(db_name, {}).get("read_only", True)
        
        if is_readonly and db_name != "Clipbase": 
            QMessageBox.warning(self, "Base de Solo Lectura", f"La base '{db_name}' es de solo lectura. Desbloquéala para guardar.")
            return
            
        if db_name == "Clipbase":
            self.statusBar().showMessage("Clipbase es volátil (se mantiene en memoria)", 3000)
        else:
            self.progress.setRange(0, 0); self.progress.show()
            self.statusBar().showMessage(f"Persistiendo cambios en {db_name}...")
            QApplication.processEvents()
            try:
                if self.db.save_active_db():
                    self.db.set_dirty(db_name, False)
                    self.refresh_db_list()
                    self.statusBar().showMessage(f"Base '{db_name}' guardada físicamente.", 5000)
                else:
                    self.statusBar().showMessage("No hay cambios que persistir.", 5000)
            finally:
                self.progress.hide()

    def add_current_game_to_db(self):
        """Añade la partida actual del tablero a la base de datos activa (en memoria)"""
        db_name = self.db.active_db_name
        is_readonly = self.db.db_metadata.get(db_name, {}).get("read_only", True)
        
        if is_readonly and db_name != "Clipbase":
            QMessageBox.warning(self, "Base de Solo Lectura", "No se puede añadir la partida a una base de solo lectura.")
            return

        # 1. Extraer datos de la partida actual
        line_uci = self.game.current_line_uci
        import chess.polyglot; board = chess.Board(); hashes = [chess.polyglot.zobrist_hash(board)]
        for move in self.game.full_mainline: 
            board.push(move)
            hashes.append(chess.polyglot.zobrist_hash(board))
            
        game_data = {
            "id": int(time.time() * 1000), 
            "white": "Jugador Blanco", "black": "Jugador Negro", 
            "w_elo": 2500, "b_elo": 2500, "result": "*", 
            "date": datetime.now().strftime("%Y.%m.%d"), 
            "event": "Análisis Local", "site": "", 
            "line": " ".join([m.uci() for m in self.game.full_mainline[:12]]), 
            "full_line": line_uci, "fens": hashes
        }

        # 2. Añadir vía DBManager
        if self.db.add_game(db_name, game_data):
            self.statusBar().showMessage(f"Partida añadida a {db_name} (pendiente de persistir)", 3000)
            self.refresh_db_list()
        else:
            self.statusBar().showMessage("Error al añadir partida", 5000)

    def export_filter_to_pgn(self):
        df = self.db.current_filter_df if self.db.current_filter_df is not None else self.db.get_active_df()
        if df is None or df.is_empty(): self.statusBar().showMessage("No hay partidas para exportar", 3000); return
        self._start_pgn_export(df)

    def export_full_db_to_pgn(self):
        """Exporta TODA la base de datos activa a PGN"""
        df = self.db.get_active_df()
        if df is None or df.is_empty(): self.statusBar().showMessage("La base de datos está vacía", 3000); return
        self._start_pgn_export(df)

    def _start_pgn_export(self, df):
        path, _ = QFileDialog.getSaveFileName(self, "Exportar a PGN", "/data/chess", "Chess PGN (*.pgn)")
        if not path: return
        if not path.endswith(".pgn"): path += ".pgn"
        self.progress.setRange(0, 100); self.progress.setValue(0); self.progress.show()
        self.export_worker = PGNExportWorker(df, path); self.export_worker.progress.connect(self.progress.setValue); self.export_worker.status.connect(self.statusBar().showMessage); self.export_worker.finished.connect(self.on_export_finished); self.export_worker.start()

    def on_export_finished(self, path):
        self.progress.hide(); self.statusBar().showMessage(f"Exportadas partidas a {os.path.basename(path)}", 5000); QMessageBox.information(self, "Exportación Completada", f"Se han exportado las partidas correctamente a:\n{path}")

    def export_filter_to_parquet(self):
        q = self.db.current_filter_query
        if q is None: self.statusBar().showMessage("No hay filtro activo para exportar", 3000); return
        
        path, _ = QFileDialog.getSaveFileName(self, "Exportar Filtro a Parquet", "/data/chess", "Chess Parquet (*.parquet)")
        if not path: return
        if not path.endswith(".parquet"): path += ".parquet"
        
        try:
            self.statusBar().showMessage("Exportando a Parquet...")
            q.collect().write_parquet(path)
            self.statusBar().showMessage(f"Base Parquet generada: {os.path.basename(path)}", 5000)
            QMessageBox.information(self, "Exportación Completada", f"Se ha creado la base Parquet en:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "Error de Exportación", f"No se pudo crear el archivo Parquet: {e}")

    def delete_filtered_games_ui(self):
        active_db = self.db.active_db_name
        is_readonly = self.db.db_metadata.get(active_db, {}).get("read_only", True)
        
        if is_readonly and active_db != "Clipbase":
            QMessageBox.warning(self, "Acción no permitida", f"La base '{active_db}' es de solo lectura. Debes permitir la escritura para borrar partidas.")
            return

        count = self.db.get_view_count()
        if count == 0: self.statusBar().showMessage("No hay partidas filtradas para borrar", 3000); return
        
        ret = QMessageBox.warning(self, "Borrado Masivo", 
                                f"¿Estás seguro de que quieres borrar las {count} partidas del filtro actual?\n\nEsta acción modificará la base en memoria. Deberás guardar para que sea permanente.",
                                QMessageBox.Yes | QMessageBox.No)
        if ret == QMessageBox.Yes:
            self.statusBar().showMessage(f"Borrando {count} partidas...")
            self.progress.setRange(0, 0) # Modo indeterminado
            self.progress.show()
            QApplication.processEvents() # Forzar visualización
            
            try:
                if self.db.delete_filtered_games():
                    self.statusBar().showMessage(f"Borradas {count} partidas.", 3000)
                    self.refresh_db_list()
            finally:
                self.progress.hide()

    def import_pgn(self):
        path, _ = QFileDialog.getOpenFileName(self, "Importar PGN", "/data/chess", "Chess PGN (*.pgn)")
        if path:
            size_mb = os.path.getsize(path) / (1024 * 1024)
            msg = f"Importando PGN ({size_mb:.1f} MB)..."
            if size_mb > 50: msg += " (Archivo grande, por favor ten paciencia)"
            
            self.progress.setRange(0, 100); self.progress.setValue(0); self.progress.show(); 
            QApplication.processEvents(); 
            self.statusBar().showMessage(msg)
            
            self.worker = PGNWorker(path)
            self.worker.progress.connect(self.progress.setValue)
            self.worker.status.connect(self.statusBar().showMessage)
            self.worker.finished.connect(self.load_parquet)
            self.worker.start()

    def append_pgn_to_current_db(self):
        active_db = self.db.active_db_name
        if active_db == "Clipbase":
            QMessageBox.warning(self, "Acción no permitida", "No se puede hacer 'Append' directamente en la Clipbase interna. Importa el PGN como una base nueva o añade partidas una a una.")
            return
            
        path, _ = QFileDialog.getOpenFileName(self, "Seleccionar PGN para añadir", "/data/chess", "Chess PGN (*.pgn)")
        if path:
            target_path = self.db.db_metadata[active_db]["path"]
            size_mb = os.path.getsize(path) / (1024 * 1024)
            
            msg = f"Añadiendo partidas a {active_db}..."
            if size_mb > 50: msg += " (Archivo grande, por favor ten paciencia)"
            
            self.progress.setRange(0, 100)
            self.progress.setValue(0)
            self.progress.show()
            self.statusBar().showMessage(msg)
            
            self.append_worker = PGNAppendWorker(path, target_path)
            self.append_worker.status.connect(self.statusBar().showMessage)
            self.append_worker.progress.connect(self.progress.setValue)
            # USAR RELOAD_DB PARA REFRESCAR EL PUNTERO AL ARCHIVO
            self.append_worker.finished.connect(lambda: self.db.reload_db(active_db)) 
            self.append_worker.finished.connect(lambda: self.db.set_dirty(active_db, True))
            self.append_worker.finished.connect(self.refresh_db_list)
            self.append_worker.finished.connect(lambda: self.progress.hide())
            self.append_worker.start()

    def open_parquet_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Abrir Parquet", "/data/chess", "Chess Parquet (*.parquet)")
        if path: self.load_parquet(path)

    def load_parquet(self, path):
        if not path or not os.path.exists(path):
            return None
            
        self.progress.hide()
        name = self.db.load_parquet(path)
        if not name: return None # Error en carga interna
        
        items = self.db_sidebar.list_widget.findItems(name, Qt.MatchExactly)
        if not items:
            item = self.db_sidebar.add_db_item(name)
            # Por defecto las externas son solo lectura
            item.setForeground(QColor("#888888")); item.setIcon(qta.icon('fa5s.lock', color='#888888'))
        else: 
            item = items[0]
            
        self.db_sidebar.list_widget.setCurrentItem(item)
        self.refresh_reference_combo()
        self.save_config()
        return name

    def open_settings(self):
        current_config = {
            "color_light": self.board_ana.color_light,
            "color_dark": self.board_ana.color_dark,
            "perf_threshold": self.perf_threshold,
            "engine_path": self.engine_path,
            "engine_threads": self.engine_threads,
            "engine_hash": self.engine_hash,
            "engine_depth": self.engine_depth,
            "min_games": self.min_games
        }
        
        dialog = SettingsDialog(current_config, self)
        if dialog.exec_():
            new_cfg = dialog.get_config()
            
            # 1. Actualizar apariencia
            self.board_ana.color_light = new_cfg["color_light"]
            self.board_ana.color_dark = new_cfg["color_dark"]
            self.board_ana.update_board()
            
            # 2. Actualizar umbral y motor
            self.perf_threshold = new_cfg["perf_threshold"]
            self.min_games = new_cfg["min_games"]
            self.engine_path = new_cfg["engine_path"]
            self.engine_threads = new_cfg["engine_threads"]
            self.engine_hash = new_cfg["engine_hash"]
            self.engine_depth = new_cfg["engine_depth"]
            
            # Sincronizar widgets
            self.opening_tree.perf_threshold = self.perf_threshold
            
            # 3. Actualizar tablero de ejercicios (si existe)
            if hasattr(self, 'tab_puzzles'):
                self.tab_puzzles.chess_board.color_light = self.board_ana.color_light
                self.tab_puzzles.chess_board.color_dark = self.board_ana.color_dark
                self.tab_puzzles.chess_board.update_board()
            
            self.save_config()
            self.statusBar().showMessage("Configuración actualizada", 3000)
            
            # 4. Reiniciar el motor si está activo para aplicar cambios (Threads, Hash, etc.)
            if hasattr(self, 'action_engine') and self.action_engine.isChecked():
                self.toggle_engine(False) # Apaga el actual
                self.toggle_engine(True)  # Arranca el nuevo con los nuevos parámetros

    def reset_filters(self):
        self.search_criteria = {"white": "", "black": "", "min_elo": "", "result": "Cualquiera"}; self.db.set_active_db(self.db.active_db_name)

    def resizeEvent(self, event): h = self.centralWidget().height() - 40; self.board_ana.setFixedWidth(h); super().resizeEvent(event)

    def trigger_invert_filter(self): self._just_inverted = True; self.db.invert_filter()

    def sort_database(self, logical_index):
        col_name = self.col_mapping.get(logical_index)
        if not col_name: return
        
        self.progress.setRange(0, 0); self.progress.show()
        self.statusBar().showMessage(f"Ordenando por {col_name}...")
        
        # Cambiar lógica: primer clic Ascendente (False), segundo Descendente (True)
        if self.sort_col == col_name:
            self.sort_desc = not self.sort_desc
        else:
            self.sort_col = col_name
            self.sort_desc = False # Empezar por Ascendente
            
        # Ajuste de inversión: forzamos que la flecha coincida con la lógica de Polars
        # Si Polars es DESC (True), usamos el indicador que en este sistema apunta abajo
        order = Qt.AscendingOrder if self.sort_desc else Qt.DescendingOrder
        self.db_table.horizontalHeader().setSortIndicator(logical_index, order)
        
        QApplication.processEvents()
        self.db.sort_active_db(col_name, self.sort_desc)
        self.progress.hide()
        self.statusBar().showMessage("Listo", 2000)

    def open_search(self):
        dialog = SearchDialog(self)
        dialog.white_input.setText(self.search_criteria.get("white", ""))
        dialog.black_input.setText(self.search_criteria.get("black", ""))
        dialog.min_elo_input.setText(str(self.search_criteria.get("min_elo", "")))
        dialog.date_from.setText(self.search_criteria.get("date_from", ""))
        dialog.date_to.setText(self.search_criteria.get("date_to", ""))
        dialog.result_combo.setCurrentText(self.search_criteria.get("result", "Cualquiera"))
        dialog.pos_check.setChecked(self.search_criteria.get("use_position", False))
        
        if dialog.exec_():
            criteria = dialog.get_criteria()
            is_empty = not any([criteria.get("white"), criteria.get("black"), criteria.get("min_elo"), criteria.get("date_from"), criteria.get("date_to"), criteria.get("use_position")])
            if criteria.get("result") != "Cualquiera": is_empty = False
            if is_empty: self.reset_filters(); return
            if criteria.get("use_position"):
                criteria["position_hash"] = chess.polyglot.zobrist_hash(self.game.board)
            self.search_criteria = criteria; self.db.filter_db(self.search_criteria)

    def refresh_db_list(self, df_to_show=None):
        lazy_active = self.db.dbs.get(self.db.active_db_name)
        if lazy_active is None: return
        
        # --- ACTUALIZAR INDICADORES "DIRTY" ---
        for i in range(self.db_sidebar.list_widget.count()):
            it = self.db_sidebar.list_widget.item(i)
            db_name = it.data(Qt.UserRole + 1) or it.text().replace("*", "").strip()
            if not it.data(Qt.UserRole + 1): it.setData(Qt.UserRole + 1, db_name)
            
            is_dirty = self.db.is_dirty(db_name)
            if is_dirty:
                it.setText(f"{db_name} *")
                if not self.db.db_metadata.get(db_name, {}).get("read_only", True):
                    it.setForeground(QColor("#1976d2"))
            else:
                it.setText(db_name)
                if self.db.db_metadata.get(db_name, {}).get("read_only", True): it.setForeground(QColor("#888888"))
                else: it.setForeground(QColor("#000000"))

        # Actualizar estado del botón Guardar
        is_readonly = self.db.db_metadata.get(self.db.active_db_name, {}).get("read_only", True)
        can_save = (not is_readonly) or (self.db.active_db_name == "Clipbase")
        self.btn_save.setEnabled(can_save)

        # Determinar DataFrame de origen
        total_db = self.db.get_active_count()
        is_filtered = self.db.current_filter_df is not None
        is_inverted = getattr(self, '_just_inverted', False)
        
        count = total_db
        state = "normal"
        
        if is_filtered:
            count = self.db.get_view_count()
            state = "error" if is_inverted else "success"
            if is_inverted: self._just_inverted = False 
            df = df_to_show if df_to_show is not None else self.db.current_filter_df
        else:
            self.search_criteria = {"white": "", "black": "", "min_elo": "", "result": "Cualquiera", "use_position": False}
            df = self.db.get_active_df()

        # ACTUALIZACIÓN SINCRONIZADA DEL BADGE GLOBAL
        f_count = format_qty(count); f_total = format_qty(total_db)
        self.opening_tree.label_global_stats.setText(f"{f_count} / {f_total}")
        self.opening_tree.label_global_stats.setStyleSheet(STYLE_BADGE_ERROR if state == "error" else (STYLE_BADGE_SUCCESS if is_filtered else STYLE_BADGE_NORMAL))
        self.db_sidebar.update_stats(f_count, f_total, state)
        
        if df is None: return
        
        # --- RESETEAR SCROLL INFINITO ---
        self.current_db_df = df
        self.db_loaded_count = 0
        self.db_table.setRowCount(0)
        self.load_more_db_rows()

    def on_db_scroll(self, value):
        scrollbar = self.db_table.verticalScrollBar()
        if value > scrollbar.maximum() - 20:
            self.load_more_db_rows()

    def load_more_db_rows(self):
        if self.current_db_df is None: return
        
        start = self.db_loaded_count
        end = min(start + self.db_batch_size, self.current_db_df.height)
        if start >= end: return
        
        self.db_table.setSortingEnabled(False)
        current_rows = self.db_table.rowCount()
        self.db_table.setRowCount(current_rows + (end - start))
        
        # Solo procesamos el nuevo bloque
        chunk = self.current_db_df.slice(start, end - start)
        for i, r in enumerate(chunk.rows(named=True)):
            row_idx = current_rows + i
            self.db_table.setItem(row_idx, 0, QTableWidgetItem(str(r["id"])))
            self.db_table.setItem(row_idx, 1, QTableWidgetItem(r["date"]))
            self.db_table.setItem(row_idx, 2, QTableWidgetItem(r["white"]))
            self.db_table.setItem(row_idx, 3, QTableWidgetItem(str(r["w_elo"])))
            self.db_table.setItem(row_idx, 4, QTableWidgetItem(r["black"]))
            self.db_table.setItem(row_idx, 5, QTableWidgetItem(str(r["b_elo"])))
            self.db_table.setItem(row_idx, 6, QTableWidgetItem(r["result"]))
            self.db_table.item(row_idx, 0).setData(Qt.UserRole, r["id"])
            
        self.db_loaded_count = end
        self.db_table.resizeColumnsToContents()

    def save_config(self):
        # 1. Rutas de bases abiertas
        dbs_info = [m["path"] for m in self.db.db_metadata.values() if m.get("path")]
        self.app_db.set_config("open_dbs", dbs_info)
        
        # 2. Colores
        colors = {"light": self.board_ana.color_light, "dark": self.board_ana.color_dark}
        self.app_db.set_config("colors", colors)
        
        # 3. Motor
        self.app_db.set_config("engine_path", self.engine_path)
        self.app_db.set_config("engine_threads", self.engine_threads)
        self.app_db.set_config("engine_hash", self.engine_hash)
        self.app_db.set_config("engine_depth", self.engine_depth)
        self.app_db.set_config("min_games", self.min_games)
        
        # 4. Otros ajustes
        self.app_db.set_config("perf_threshold", getattr(self, 'perf_threshold', 25))

    def load_config(self):
        self.def_light, self.def_dark = "#eeeed2", "#8ca2ad"
        
        # --- CARGA DESDE SQLITE ---
        self.perf_threshold = self.app_db.get_config("perf_threshold", 25)
        
        # Cargar colores
        colors = self.app_db.get_config("colors")
        if colors:
            self.def_light = colors.get("light", self.def_light)
            self.def_dark = colors.get("dark", self.def_dark)
            
        # Cargar configuración del motor
        self.engine_path = self.app_db.get_config("engine_path", "/usr/bin/stockfish")
        self.engine_threads = self.app_db.get_config("engine_threads", 1)
        self.engine_hash = self.app_db.get_config("engine_hash", 64)
        self.engine_depth = self.app_db.get_config("engine_depth", 10)
        self.min_games = self.app_db.get_config("min_games", 20)
            
        # Cargar bases de datos abiertas
        self.pending_dbs = self.app_db.get_config("open_dbs", [])
        
        # Sincronizar ajustes con widgets
        if hasattr(self, 'opening_tree'):
            self.opening_tree.perf_threshold = self.perf_threshold
