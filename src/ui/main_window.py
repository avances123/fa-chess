import os
import json
import time
from datetime import datetime
import chess
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

from src.config import CONFIG_FILE, LIGHT_STYLE, ECO_FILE
from src.core.workers import PGNWorker, StatsWorker, PGNExportWorker, PGNAppendWorker
from src.core.eco import ECOManager
from src.core.db_manager import DBManager
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
        
        # Gestores y Controladores
        self.db = DBManager()
        self.game = GameController()
        self.eco = ECOManager(ECO_FILE)
        
        # Estado de Ordenación
        self.sort_col = None
        self.sort_desc = False
        self.col_mapping = {0: "id", 1: "date", 2: "white", 3: "w_elo", 4: "black", 5: "b_elo", 6: "result"}
        
        self.game_evals = [] # Almacenar evaluaciones de la partida actual
        
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

    def flip_boards(self):
        self.board_ana.flip()

    def search_current_position(self):
        import chess.polyglot
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
        self.btn_save.setToolTip("Añadir esta partida a la base activa")
        self.btn_save.clicked.connect(self.add_current_game_to_db)
        
        self.btn_clip = QPushButton(qta.icon('fa5s.clipboard', color='#2e7d32'), " a Clipbase")
        self.btn_clip.setStyleSheet(STYLE_ACTION_BUTTON)
        self.btn_clip.setToolTip("Copiar esta partida a la Clipbase")
        self.btn_clip.clicked.connect(self.add_to_clipbase)
        
        self.btn_new = QPushButton(qta.icon('fa5s.file-alt', color='#555'), " Nueva")
        self.btn_new.setStyleSheet(STYLE_ACTION_BUTTON)
        self.btn_new.setToolTip("Empezar una nueva partida")
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
        db_content.addWidget(self.db_table); db_layout.addLayout(db_content, 4)
        
        self.progress = QProgressBar(); self.progress.setMaximumWidth(150); self.progress.setFixedHeight(14); self.progress.setTextVisible(True); self.progress.setVisible(False)
        self.statusBar().addPermanentWidget(self.progress)

        self.search_criteria = {"white": "", "black": "", "min_elo": "", "result": "Cualquiera"}
        for path in getattr(self, 'pending_dbs', []):
            if os.path.exists(path): self.load_parquet(path)

    def start_full_analysis(self):
        if self.action_engine.isChecked(): self.action_engine.toggle(); self.toggle_engine(False)
        self.board_ana.setEnabled(False); self.opening_tree.setEnabled(False)
        self.progress.setRange(0, 100); self.progress.setValue(0); self.progress.show()
        total_moves = len(self.game.full_mainline) + 1
        self.game_evals = [0] * total_moves; self.eval_graph.set_evaluations(self.game_evals)
        self.analysis_worker = FullAnalysisWorker(self.game.full_mainline)
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
            self.engine_worker = EngineWorker(); self.engine_worker.info_updated.connect(self.on_engine_update); self.engine_worker.update_position(self.game.board.fen()); self.engine_worker.start()
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
        self.game.load_uci_line(""); self.game_header.clear_info()

    def load_game_from_list(self, item):
        game_id = self.db_table.item(item.row(), 0).data(Qt.UserRole)
        row = self.db.get_game_by_id(self.db.active_db_name, game_id)
        if row:
            self.game.load_uci_line(row["full_line"]); self.game.go_start()
            self.game_header.update_info(row); self.tabs.setCurrentIndex(0)

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
        if hasattr(self, 'stats_worker') and self.stats_worker.isRunning():
            try: self.stats_worker.finished.disconnect(); self._active_workers.append(self.stats_worker)
            except: pass
        
        # MOSTRAR SPINNER
        self.opening_tree.set_loading(True)
        
        self.progress.setRange(0, 0); self.progress.show(); self.statusBar().showMessage("Calculando estadísticas...")
        import chess.polyglot
        current_hash = chess.polyglot.zobrist_hash(self.game.board)
        self.stats_worker = StatsWorker(self.db, self.game.current_line_uci, self.game.board.turn == chess.WHITE, current_hash)
        self.stats_worker.finished.connect(self.on_stats_finished); self.stats_worker.start()

    def update_stats(self):
        self.stats_timer.start(50)

    def on_stats_finished(self, res):
        self.progress.hide()
        is_filtered = self.db.current_filter_df is not None
        total_view = self.db.get_view_count()
        
        # get_opening_name ahora devuelve (nombre, profundidad)
        opening_name, _ = self.eco.get_opening_name(self.game.current_line_uci)
        
        # Actualizar el árbol (él sí muestra datos de la posición)
        self.opening_tree.update_tree(res, self.game.board, opening_name, is_filtered, total_view)
        
        is_partial = False
        if res is not None and res.height > 0:
            is_partial = "_is_partial" in res.columns and res.row(0, named=True).get("_is_partial")
        
        if is_partial: 
            self.statusBar().showMessage("⚠️ Árbol parcial. El límite es de 1M de partidas.", 5000)
            # Solo en caso de árbol parcial mostramos error en el badge del árbol
            self.opening_tree.label_global_stats.setStyleSheet(STYLE_BADGE_ERROR)
        else: 
            self.statusBar().showMessage("Listo", 2000)

    def init_menu(self):
        menubar = self.menuBar()
        
        # --- MENÚ ARCHIVO ---
        file_menu = menubar.addMenu("&Archivo")
        
        # Abrir
        open_parquet = QAction(qta.icon('fa5s.folder-open', color='#1976d2'), "&Abrir Base Parquet...", self)
        open_parquet.setShortcut("Ctrl+O")
        open_parquet.triggered.connect(self.open_parquet_file)
        file_menu.addAction(open_parquet)
        
        # Importar
        import_menu = file_menu.addMenu(qta.icon('fa5s.file-import'), "&Importar")
        import_pgn = QAction(qta.icon('fa5s.file-code', color='#2e7d32'), "Archivo &PGN...", self)
        import_pgn.setShortcut("Ctrl+I")
        import_pgn.triggered.connect(self.import_pgn)
        import_menu.addAction(import_pgn)
        
        # Exportar
        export_menu = file_menu.addMenu(qta.icon('fa5s.file-export'), "&Exportar")
        export_full_pgn = QAction(qta.icon('fa5s.file-export'), "Base Activa a &PGN...", self)
        export_full_pgn.triggered.connect(self.export_full_db_to_pgn)
        export_menu.addAction(export_full_pgn)
        
        export_filtered_pgn = QAction(qta.icon('fa5s.filter'), "Filtro a P&GN...", self)
        export_filtered_pgn.setShortcut("Ctrl+E")
        export_filtered_pgn.triggered.connect(self.export_filter_to_pgn)
        export_menu.addAction(export_filtered_pgn)
        
        file_menu.addSeparator()
        
        # --- ACCIÓN GUARDAR (PERSISTIR) ---
        self.save_action = QAction(qta.icon('fa5s.save', color='#1976d2'), "&Guardar Base (Persistir)", self)
        self.save_action.setShortcut("Ctrl+S")
        self.save_action.triggered.connect(self.save_to_active_db)
        file_menu.addAction(self.save_action)
        
        file_menu.addSeparator()
        settings_action = QAction(qta.icon('fa5s.cog'), "&Configuración...", self)
        settings_action.triggered.connect(self.open_settings)
        file_menu.addAction(settings_action)
        
        file_menu.addSeparator()
        exit_action = QAction(qta.icon('fa5s.power-off'), "&Salir", self); exit_action.setShortcut("Ctrl+Q"); exit_action.triggered.connect(self.close); file_menu.addAction(exit_action)

        # --- MENÚ EDITAR (Después de Archivo) ---
        edit_menu = menubar.addMenu("&Editar")
        invert_action = QAction(qta.icon('fa5s.exchange-alt'), "&Invertir Filtro", self)
        invert_action.triggered.connect(self.trigger_invert_filter)
        edit_menu.addAction(invert_action)
        
        clear_action = QAction(qta.icon('fa5s.eraser', color='#c62828'), "&Quitar Filtros", self)
        clear_action.setShortcut("Ctrl+L"); clear_action.triggered.connect(self.reset_filters); edit_menu.addAction(clear_action)
        
        edit_menu.addSeparator()
        delete_filtered_action = QAction(qta.icon('fa5s.trash-alt', color='#c62828'), "&Borrar Partidas Filtradas", self)
        delete_filtered_action.triggered.connect(self.delete_filtered_games_ui)
        edit_menu.addAction(delete_filtered_action)

        # --- MENÚ JUGADOR ---
        player_menu = menubar.addMenu("&Jugador")
        report_action = QAction(qta.icon('fa5s.chart-bar', color='#673ab7'), "Dossier de &Inteligencia...", self)
        report_action.setShortcut("Ctrl+D"); report_action.triggered.connect(self.prompt_player_report); player_menu.addAction(report_action)

        # --- MENÚ BASES DE DATOS ---
        db_menu = menubar.addMenu("&Bases de Datos")
        
        # Submenú Crear
        create_menu = db_menu.addMenu(qta.icon('fa5s.plus-circle'), "&Crear")
        
        new_empty_db = QAction("Base &Vacía...", self)
        new_empty_db.setShortcut("Ctrl+N")
        new_empty_db.triggered.connect(self.create_new_db)
        create_menu.addAction(new_empty_db)
        
        new_from_filter = QAction("Desde Filas &Filtradas (Parquet)...", self)
        new_from_filter.triggered.connect(self.export_filter_to_parquet)
        create_menu.addAction(new_from_filter)
        
        append_pgn_action = QAction(qta.icon('fa5s.file-medical', color='#2e7d32'), "&Añadir PGN a la base activa...", self)
        append_pgn_action.triggered.connect(self.append_pgn_to_current_db)
        db_menu.addAction(append_pgn_action)
        
        db_menu.addSeparator()
        filter_action = QAction(qta.icon('fa5s.search'), "&Filtrar Partidas...", self)
        filter_action.setShortcut("Ctrl+F"); filter_action.triggered.connect(self.open_search); db_menu.addAction(filter_action)
        
        db_menu.addSeparator()
        delete_db_action = QAction(qta.icon('fa5s.trash-alt'), "&Eliminar Archivo de Base...", self)
        delete_db_action.triggered.connect(self.delete_current_db_file); db_menu.addAction(delete_db_action)

        # --- MENÚ TABLERO ---
        board_menu = menubar.addMenu("&Tablero")
        
        flip_action = QAction(qta.icon('fa5s.retweet'), "&Girar Tablero", self)
        flip_action.setShortcut("F")
        flip_action.triggered.connect(self.flip_boards)
        board_menu.addAction(flip_action)
        
        engine_action = QAction(qta.icon('fa5s.microchip'), "&Activar Motor", self)
        engine_action.setShortcut("E")
        engine_action.setCheckable(True)
        engine_action.triggered.connect(self.toggle_engine_shortcut)
        board_menu.addAction(engine_action)
        
        board_menu.addSeparator()
        
        analyze_action = QAction(qta.icon('fa5s.magic', color='#673ab7'), "&Analizar Partida Completa", self)
        analyze_action.triggered.connect(self.start_full_analysis)
        board_menu.addAction(analyze_action)
        
        # --- MENÚ AYUDA ---
        help_menu = menubar.addMenu("&Ayuda")
        about_action = QAction(qta.icon('fa5s.info-circle'), "&Acerca de...", self)
        about_action.triggered.connect(self.show_about_dialog)
        help_menu.addAction(about_action)

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
        for i in [0, 1]:
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
        name = item.text()
        if name == "Clipbase": return
        
        # Obtener estado actual y hacer toggle
        current_ro = self.db.db_metadata.get(name, {}).get("read_only", True)
        new_status = not current_ro
        
        # Aplicar cambio en el motor
        if self.db.set_readonly(name, new_status):
            # Cambiar visualmente
            if new_status: 
                item.setIcon(qta.icon('fa5s.lock', color='#888888'))
                item.setForeground(QColor("#888888"))
            else: 
                item.setIcon(qta.icon('fa5s.unlock', color='#2e7d32'))
                item.setForeground(QColor("#000000"))
            
            self.statusBar().showMessage(f"Base '{name}': {'Solo Lectura' if new_status else 'Edición Habilitada'}", 3000)
            self.refresh_db_list()

    def toggle_db_readonly(self, item, status):
        # Mantenemos este método por compatibilidad con el menú contextual antiguo
        self.toggle_db_readonly_logic(item, status)

    def on_db_list_context_menu(self, pos):
        item = self.db_sidebar.list_widget.itemAt(pos)
        if not item or item.text() == "Clipbase": return
        name = item.text(); is_readonly = self.db.db_metadata.get(name, {}).get("read_only", True); menu = QMenu()
        
        if not is_readonly: 
            # Solo ofrecemos persistir si ya está en modo escritura
            persist_action = QAction(qta.icon('fa5s.save', color='#1976d2'), " Persistir Base (Guardar en Disco)", self)
            persist_action.triggered.connect(self.save_to_active_db)
            menu.addAction(persist_action)

        menu.addSeparator(); remove_action = QAction(qta.icon('fa5s.times', color='red'), " Quitar de la lista", self); remove_action.triggered.connect(lambda: self.remove_database(item)); menu.addAction(remove_action); menu.exec(self.db_sidebar.list_widget.mapToGlobal(pos))

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
        name = item.text()
        if name in self.db.dbs:
            del self.db.dbs[name]
            if name in self.db.db_metadata: del self.db.db_metadata[name]
            self.db_sidebar.list_widget.takeItem(self.db_sidebar.list_widget.row(item))
            if self.db.active_db_name == name: self.db.set_active_db("Clipbase")
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
        self.progress.hide(); name = self.db.load_parquet(path)
        items = self.db_sidebar.list_widget.findItems(name, Qt.MatchExactly)
        if not items:
            item = self.db_sidebar.add_db_item(name)
            item.setForeground(QColor("#888888")); item.setIcon(qta.icon('fa5s.lock', color='#888888'))
        else: item = items[0]
        self.db_sidebar.list_widget.setCurrentItem(item); self.save_config()

    def open_settings(self):
        dialog = SettingsDialog(self.board_ana.color_light, self.board_ana.color_dark, self)
        if dialog.exec_(): light, dark = dialog.get_colors(); self.board_ana.color_light = light; self.board_ana.color_dark = dark; self.board_ana.update_board(); self.save_config()

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
                import chess.polyglot; criteria["position_hash"] = chess.polyglot.zobrist_hash(self.game.board)
            self.search_criteria = criteria; self.db.filter_db(self.search_criteria)

    def refresh_db_list(self, df_to_show=None):
        lazy_active = self.db.dbs.get(self.db.active_db_name)
        if lazy_active is None: return
        
        # --- ACTUALIZAR INDICADORES "DIRTY" EN EL SIDEBAR ---
        for i in range(self.db_sidebar.list_widget.count()):
            it = self.db_sidebar.list_widget.item(i)
            db_name = it.data(Qt.UserRole + 1) or it.text().replace("*", "").strip()
            
            # Guardamos el nombre original sin asterisco en un rol oculto si no está
            if not it.data(Qt.UserRole + 1):
                it.setData(Qt.UserRole + 1, db_name)
            
            is_dirty = self.db.is_dirty(db_name)
            if is_dirty:
                it.setText(f"{db_name} *")
                if not self.db.db_metadata.get(db_name, {}).get("read_only", True):
                    it.setForeground(QColor("#1976d2")) # Azul si es dirty y RW
            else:
                it.setText(db_name)
                # Restaurar color según RO/RW
                if self.db.db_metadata.get(db_name, {}).get("read_only", True):
                    it.setForeground(QColor("#888888"))
                else:
                    it.setForeground(QColor("#000000"))

        # Actualizar estado del botón Guardar
        is_readonly = self.db.db_metadata.get(self.db.active_db_name, {}).get("read_only", True)
        can_save = (not is_readonly) or (self.db.active_db_name == "Clipbase")
        self.btn_save.setEnabled(can_save)

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
        f_count = format_qty(count)
        f_total = format_qty(total_db)
        self.opening_tree.label_global_stats.setText(f"{f_count} / {f_total}")
        self.opening_tree.label_global_stats.setStyleSheet(STYLE_BADGE_ERROR if state == "error" else (STYLE_BADGE_SUCCESS if is_filtered else STYLE_BADGE_NORMAL))
        
        # También actualizamos el sidebar para mantener coherencia total
        self.db_sidebar.update_stats(f_count, f_total, state)
        
        if df is None: return
        disp = df.head(1000); self.db_table.setRowCount(disp.height)
        for i, r in enumerate(disp.rows(named=True)):
            self.db_table.setItem(i, 0, QTableWidgetItem(str(r["id"]))); self.db_table.setItem(i, 1, QTableWidgetItem(r["date"])); self.db_table.setItem(i, 2, QTableWidgetItem(r["white"])); self.db_table.setItem(i, 3, QTableWidgetItem(str(r["w_elo"]))); self.db_table.setItem(i, 4, QTableWidgetItem(r["black"])); self.db_table.setItem(i, 5, QTableWidgetItem(str(r["b_elo"]))); self.db_table.setItem(i, 6, QTableWidgetItem(r["result"])); self.db_table.item(i, 0).setData(Qt.UserRole, r["id"])
        self.db_table.resizeColumnsToContents()

    def save_config(self):
        dbs_info = [{"path": m["path"]} for m in self.db.db_metadata.values() if m.get("path")]
        colors = {"light": self.board_ana.color_light, "dark": self.board_ana.color_dark}
        
        config = {
            "perf_threshold": getattr(self, 'perf_threshold', 25),
            "colors": colors,
            "dbs": dbs_info
        }
        
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4, ensure_ascii=False)

    def load_config(self):
        self.def_light, self.def_dark = "#eeeed2", "#8ca2ad"
        self.perf_threshold = 25
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self.pending_dbs = [info["path"] if isinstance(info, dict) else info for info in data.get("dbs", [])]
                colors = data.get("colors")
                if colors:
                    self.def_light = colors.get("light", self.def_light)
                    self.def_dark = colors.get("dark", self.def_dark)
                
                self.perf_threshold = data.get("perf_threshold", 25)
                # Sincronizar con el widget si ya existe
                if hasattr(self, 'opening_tree'):
                    self.opening_tree.perf_threshold = self.perf_threshold
            except: pass
