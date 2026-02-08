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
                             QStyle, QSizePolicy, QMessageBox, QApplication)
from PySide6.QtCore import Qt, QPointF, QTimer, QSize
from PySide6.QtGui import QAction, QFont, QShortcut, QKeySequence, QPainter, QColor, QBrush
import qtawesome as qta

class SortableWidgetItem(QTableWidgetItem):
    """Permite ordenar por un valor numérico oculto en lugar del texto formateado"""
    def __lt__(self, other):
        try:
            v1 = self.data(Qt.UserRole)
            v2 = other.data(Qt.UserRole)
            if v1 is not None and v2 is not None:
                return float(v1) < float(v2)
        except: pass
        return super().__lt__(other)

from config import CONFIG_FILE, LIGHT_STYLE, ECO_FILE
from core.workers import PGNWorker, StatsWorker, PGNExportWorker
from core.eco import ECOManager
from core.db_manager import DBManager
from core.game_controller import GameController
from ui.board import ChessBoard
from ui.settings_dialog import SettingsDialog
from ui.search_dialog import SearchDialog
from ui.edit_game_dialog import EditGameDialog
from ui.widgets.results_bar import ResultsWidget
from ui.widgets.eval_graph import EvaluationGraph
from ui.widgets.analysis_report import AnalysisReport
from ui.styles import (STYLE_EVAL_BAR, STYLE_LABEL_EVAL, STYLE_TABLE_HEADER, 
                       STYLE_PROGRESS_BAR, STYLE_BADGE_NORMAL, STYLE_BADGE_SUCCESS, 
                       STYLE_BADGE_ERROR)
from core.engine_worker import EngineWorker, FullAnalysisWorker

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
        self.col_mapping = {0: "date", 1: "white", 2: "w_elo", 3: "black", 4: "b_elo", 5: "result"}
        
        self.game_evals = [] # Almacenar evaluaciones de la partida actual
        
        # Conectar señales del controlador de juego
        self.game.position_changed.connect(self.update_ui)
        
        # Conectar señales del gestor de base de datos
        self.db.active_db_changed.connect(self.refresh_db_list)
        self.db.active_db_changed.connect(self.update_stats) # Refrescar vía timer (seguro)
        
        self.db.filter_updated.connect(self.refresh_db_list)
        self.db.filter_updated.connect(self.update_stats) # Refrescar vía timer (seguro)
        
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
        
        # Feedback visual de inicio
        QApplication.setOverrideCursor(Qt.WaitCursor)
        self.progress.setRange(0, 0)
        self.progress.show()
        self.statusBar().showMessage("Buscando posición en la base de datos...")
        QApplication.processEvents() # Forzar dibujado antes del cálculo pesado

        try:
            # Guardamos el estado completo para que la Lupa lo reconozca
            self.search_criteria = {
                "white": "", "black": "", "min_elo": "", "result": "Cualquiera", 
                "position_hash": pos_hash,
                "use_position": True
            }
            
            filtered = self.db.filter_db(self.search_criteria)
            self.refresh_db_list(filtered)
            self.tabs.setCurrentIndex(1)
        finally:
            QApplication.restoreOverrideCursor()
            # La barra de progreso se ocultará cuando on_stats_finished termine su trabajo
            # No la ocultamos aquí para que el flujo sea continuo del filtro al árbol

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
        self.eval_bar = QProgressBar(); self.eval_bar.setOrientation(Qt.Vertical); self.eval_bar.setRange(-1000, 1000); self.eval_bar.setValue(0); self.eval_bar.setTextVisible(False); self.eval_bar.setFixedWidth(15); self.eval_bar.setStyleSheet(STYLE_EVAL_BAR); self.eval_bar.setVisible(False)
        self.board_ana = ChessBoard(self.game.board, self); self.board_ana.color_light, self.board_ana.color_dark = self.def_light, self.def_dark
        # Conectar señales de arrastre para evitar crashes con el motor
        self.board_ana.piece_drag_started.connect(lambda: setattr(self, 'is_dragging', True))
        self.board_ana.piece_drag_finished.connect(lambda: setattr(self, 'is_dragging', False))
        
        board_eval_layout.addWidget(self.eval_bar); board_eval_layout.addWidget(self.board_ana); board_container_layout.addLayout(board_eval_layout)
        ana_layout.addWidget(board_container)
        
        panel_ana = QWidget(); p_ana_layout = QVBoxLayout(panel_ana)
        info_box = QWidget(); info_layout = QHBoxLayout(info_box); info_layout.setContentsMargins(0, 0, 0, 0)
        
        # Etiqueta de Apertura (Estilizada)
        self.label_eco = QLabel("Apertura: Inicial")
        self.label_eco.setStyleSheet(STYLE_BADGE_NORMAL)
        self.label_eco.setTextInteractionFlags(Qt.LinksAccessibleByMouse)
        self.label_eco.linkActivated.connect(self.on_opening_label_link)
        
        # Etiqueta de Estadísticas de Posición (Estilizada)
        self.label_pos_stats = QLabel("Partidas: 0")
        self.label_pos_stats.setStyleSheet(STYLE_BADGE_NORMAL)
        self.label_pos_stats.setAlignment(Qt.AlignCenter)
        
        self.label_eval = QLabel("")
        self.label_eval.setStyleSheet(STYLE_LABEL_EVAL)
        
        info_layout.addWidget(self.label_eco, 1)
        info_layout.addWidget(self.label_pos_stats, 1)
        info_layout.addWidget(self.label_eval)
        p_ana_layout.addWidget(info_box)
        
        self.tree_ana = self.create_scid_table(["Movim.", "Frec.", "Barra", "Win %", "AvElo", "Perf"])
        self.tree_ana.setFont(table_font); self.tree_ana.itemDoubleClicked.connect(self.on_tree_cell_double_click); self.tree_ana.itemClicked.connect(self.on_tree_cell_click)
        self.tree_ana.setMouseTracking(True); self.tree_ana.cellEntered.connect(self.on_tree_cell_hover); p_ana_layout.addWidget(self.tree_ana)
        
        # Panel de Pestañas (Notación / Gráfico)
        self.tabs_side = QTabWidget()
        
        # Pestaña 1: Notación
        # Panel de Pestañas (Notación / Gráfico)
        self.tabs_side = QTabWidget()
        
        # Pestaña 1: Notación
        tab_notacion = QWidget()
        layout_notacion = QVBoxLayout(tab_notacion)
        layout_notacion.setContentsMargins(0,0,0,0)
        
        self.hist_ana = QTextBrowser(); self.hist_ana.setOpenLinks(False); self.hist_ana.anchorClicked.connect(self.jump_to_move_link)
        layout_notacion.addWidget(self.hist_ana)
        
        # Barra de Herramientas de Partida (Acciones sobre la partida actual)
        game_actions_toolbar = QToolBar()
        game_actions_toolbar.setIconSize(QSize(16, 16))
        game_actions_toolbar.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        game_actions_toolbar.setStyleSheet("QToolBar { spacing: 10px; background: #f5f5f5; border-top: 1px solid #ddd; padding: 2px; }")
        
        act_save_active = QAction(qta.icon('fa5s.save', color='#1976d2'), "Guardar", self)
        act_save_active.setStatusTip("Guardar la partida actual en la base de datos seleccionada")
        act_save_active.setToolTip("Guardar en la base de datos activa")
        act_save_active.triggered.connect(self.save_to_active_db)
        game_actions_toolbar.addAction(act_save_active)
        
        act_save_clip = QAction(qta.icon('fa5s.clipboard', color='#2e7d32'), "a Clipbase", self)
        act_save_clip.setStatusTip("Copiar la partida actual al portapapeles interno (Clipbase)")
        act_save_clip.setToolTip("Añadir esta partida a la Clipbase (borrador)")
        act_save_clip.triggered.connect(self.add_to_clipbase)
        game_actions_toolbar.addAction(act_save_clip)
        
        spacer = QWidget(); spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        game_actions_toolbar.addWidget(spacer)
        
        act_new_game = QAction(qta.icon('fa5s.file'), "Nueva", self)
        act_new_game.setStatusTip("Limpiar el tablero y los datos para empezar una nueva partida desde cero")
        act_new_game.setToolTip("Limpiar tablero y empezar nueva partida")
        act_new_game.triggered.connect(lambda: self.game.load_uci_line(""))
        game_actions_toolbar.addAction(act_new_game)
        
        layout_notacion.addWidget(game_actions_toolbar)
        
        self.tabs_side.addTab(tab_notacion, qta.icon('fa5s.list-ol'), "Notación")
        
        # Pestaña 3: Gráfico
        tab_grafico = QWidget()
        layout_grafico = QVBoxLayout(tab_grafico)
        layout_grafico.setContentsMargins(0,0,0,0)
        
        self.eval_graph = EvaluationGraph()
        self.eval_graph.move_selected.connect(self.game.jump_to_move) 
        layout_grafico.addWidget(self.eval_graph)
        
        # btn_analyze movido fuera
        
        self.tabs_side.addTab(tab_grafico, qta.icon('fa5s.chart-area'), "Gráfico")
        
        # Pestaña 3: Informe
        self.analysis_report = AnalysisReport()
        self.tabs_side.addTab(self.analysis_report, qta.icon('fa5s.chart-pie'), "Informe")
        
        p_ana_layout.addWidget(self.tabs_side)
        
        # Botonera Común Inferior del Panel Lateral
        common_btns = QHBoxLayout()
        btn_analyze = QPushButton(qta.icon('fa5s.magic'), " Analizar Partida Completa")
        btn_analyze.clicked.connect(self.start_full_analysis)
        common_btns.addWidget(btn_analyze)
        p_ana_layout.addLayout(common_btns)
        
        ana_layout.addWidget(panel_ana, 1)

        # --- TAB 2: GESTOR ---
        self.tab_db = QWidget(); self.tabs.addTab(self.tab_db, "Gestor Bases")
        db_layout = QHBoxLayout(self.tab_db)
        
        # Sidebar de Bases
        db_sidebar_container = QWidget()
        db_sidebar = QVBoxLayout(db_sidebar_container)
        db_sidebar.setContentsMargins(5, 5, 5, 5)
        
        # Título y Estadísticas (Badge elegante)
        stats_header = QHBoxLayout()
        stats_header.addWidget(QLabel("<b>Partidas:</b>"))
        self.label_db_stats = QLabel("[0/0]")
        self.label_db_stats.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.label_db_stats.setStyleSheet(STYLE_BADGE_NORMAL)
        stats_header.addWidget(self.label_db_stats)
        db_sidebar.addLayout(stats_header)

        # Barra de Herramientas de Bases (Acciones rápidas sobre la lista)
        db_actions_toolbar = QToolBar()
        db_actions_toolbar.setIconSize(QSize(16, 16))
        db_actions_toolbar.setToolButtonStyle(Qt.ToolButtonIconOnly)
        db_actions_toolbar.setStyleSheet("QToolBar { spacing: 5px; background: transparent; border: none; }")
        
        act_new = QAction(qta.icon('fa5s.plus-circle', color='#2e7d32'), "Nueva Base", self)
        act_new.setStatusTip("Crear un nuevo archivo de base de datos vacío (.parquet)")
        act_new.triggered.connect(self.create_new_db)
        db_actions_toolbar.addAction(act_new)
        
        act_import = QAction(qta.icon('fa5s.file-import', color='#1976d2'), "Importar PGN", self)
        act_import.setStatusTip("Importar partidas desde un archivo PGN externo")
        act_import.triggered.connect(self.import_pgn)
        db_actions_toolbar.addAction(act_import)
        
        db_actions_toolbar.addSeparator()
        
        act_filter = QAction(qta.icon('fa5s.search'), "Filtrar Partidas", self)
        act_filter.setStatusTip("Buscar partidas por nombres, ELO o posición actual")
        act_filter.triggered.connect(self.open_search)
        db_actions_toolbar.addAction(act_filter)
        
        act_invert = QAction(qta.icon('fa5s.exchange-alt'), "Invertir Filtro", self)
        act_invert.setStatusTip("Ver las partidas que NO coinciden con el filtro actual")
        act_invert.triggered.connect(self.trigger_invert_filter)
        db_actions_toolbar.addAction(act_invert)
        
        act_clear = QAction(qta.icon('fa5s.eraser', color='#c62828'), "Quitar Filtros", self)
        act_clear.setStatusTip("Eliminar todos los filtros y volver a ver la base completa")
        act_clear.triggered.connect(self.reset_filters)
        db_actions_toolbar.addAction(act_clear)
        
        db_sidebar.addWidget(db_actions_toolbar)

        # Lista de Bases
        db_sidebar.addWidget(QLabel("<b>Bases Abiertas</b>"))
        self.db_list_widget = QListWidget()
        self.db_list_widget.setIconSize(QSize(14, 14))
        
        # Inicializar Clipbase con icono de portapapeles
        clip_item = QListWidgetItem(qta.icon('fa5s.clipboard', color='#2e7d32'), "Clipbase")
        self.db_list_widget.addItem(clip_item)
        
        self.db_list_widget.setContextMenuPolicy(Qt.CustomContextMenu)
        self.db_list_widget.currentRowChanged.connect(self.switch_db)
        self.db_list_widget.customContextMenuRequested.connect(self.on_db_list_context_menu)
        db_sidebar.addWidget(self.db_list_widget)
        
        db_layout.addWidget(db_sidebar_container, 1)
        
        self.search_criteria = {"white": "", "black": "", "min_elo": "", "result": "Cualquiera"}
        
        # Contenido de la Tabla
        db_content = QVBoxLayout()
        self.db_table = self.create_scid_table(["Fecha", "Blancas", "Elo B", "Negras", "Elo N", "Res"])
        self.db_table.setFont(table_font)
        self.db_table.itemDoubleClicked.connect(self.load_game_from_list)
        self.db_table.customContextMenuRequested.connect(self.on_db_table_context_menu)
        db_content.addWidget(self.db_table)
        db_layout.addLayout(db_content, 4)
        
        # Barra de progreso global en la StatusBar
        self.progress = QProgressBar()
        self.progress.setMaximumWidth(150)
        self.progress.setFixedHeight(12)
        self.progress.setTextVisible(False)
        self.progress.setVisible(False)
        self.progress.setStyleSheet("QProgressBar { border: 1px solid #aaa; background: #eee; } QProgressBar::chunk { background: #4caf50; }")
        self.statusBar().addPermanentWidget(self.progress)

        for path in getattr(self, 'pending_dbs', []):
            if os.path.exists(path): self.load_parquet(path)

    def start_full_analysis(self):
        # Detener motor principal si está corriendo
        if self.action_engine.isChecked():
            self.action_engine.toggle()
            self.toggle_engine(False)
            
        # Bloquear interacción para evitar crashes
        self.board_ana.setEnabled(False)
        self.tree_ana.setEnabled(False)
            
        # Preparar UI
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.show()
        
        # Inicializar array de evaluaciones si es necesario
        total_moves = len(self.game.full_mainline) + 1
        self.game_evals = [0] * total_moves
        self.eval_graph.set_evaluations(self.game_evals)
        
        # Iniciar Worker
        self.analysis_worker = FullAnalysisWorker(self.game.full_mainline)
        self.analysis_worker.progress.connect(lambda curr, total: self.progress.setValue(int((curr/total)*100)))
        self.analysis_worker.analysis_result.connect(self.on_analysis_update)
        self.analysis_worker.finished.connect(self.on_analysis_finished)
        self.analysis_worker.error_occurred.connect(lambda msg: QMessageBox.critical(self, "Error de Análisis", msg))
        self.analysis_worker.start()
        self.statusBar().showMessage("Analizando partida completa...", 5000)

    def on_analysis_update(self, idx, cp_score):
        if 0 <= idx < len(self.game_evals):
            self.game_evals[idx] = cp_score
            self.eval_graph.set_evaluations(self.game_evals)

    def on_analysis_finished(self):
        self.progress.hide()
        self.board_ana.setEnabled(True)
        self.tree_ana.setEnabled(True)
        
        # Obtener nombres de jugadores
        w_name = getattr(self, 'current_white', "Blancas")
        b_name = getattr(self, 'current_black', "Negras")
        
        # Actualizar Informe
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
        self.label_eval.setText(eval_str)
        # Solo actualizar el tablero (flechas) si NO estamos arrastrando una pieza
        if not getattr(self, 'is_dragging', False):
            self.board_ana.set_engine_move(best_move if best_move else None)
            
        try:
            if "M" in eval_str: 
                val = 1000 if "+" in eval_str or eval_str[0].isdigit() or (eval_str.startswith("M") and not eval_str.startswith("-M")) else -1000
                cp_val = 2000 if val > 0 else -2000 # Valor alto para mate en gráfica
            else: 
                val = int(float(eval_str) * 100)
                cp_val = int(float(eval_str) * 100)
            
            self.eval_bar.setValue(val)
            
            # Guardar en el histórico para la gráfica
            if 0 <= self.game.current_idx < len(self.game_evals):
                self.game_evals[self.game.current_idx] = cp_val
                self.eval_graph.set_evaluations(self.game_evals) # Redibujar gráfica en vivo
                
        except: pass

    def closeEvent(self, event):
        # Guardar Clipbase al cerrar
        self.db.save_clipbase()
        
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

    def create_scid_table(self, headers):
        table = QTableWidget(0, len(headers)); table.setHorizontalHeaderLabels(headers); table.setEditTriggers(QAbstractItemView.NoEditTriggers); table.setSelectionBehavior(QAbstractItemView.SelectRows); table.setContextMenuPolicy(Qt.CustomContextMenu); table.verticalHeader().setVisible(False); table.verticalHeader().setDefaultSectionSize(22); table.horizontalHeader().setHighlightSections(True); table.setShowGrid(True); table.setStyleSheet(STYLE_TABLE_HEADER)
        # Desactivar ordenación nativa y usar nuestra lógica
        table.setSortingEnabled(False)
        table.horizontalHeader().setSortIndicatorShown(True) # Mostrar flechita aunque sea manual
        table.horizontalHeader().sectionClicked.connect(self.sort_database)
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

    def copy_selected_game(self):
        row = self.db_table.currentRow()
        if row < 0: return
        
        game_id = self.db_table.item(row, 0).data(Qt.UserRole)
        game_data = self.db.get_game_by_id(self.db.active_db_name, game_id)
        if not game_data: return

        # Buscar bases que NO son de solo lectura
        writable_dbs = []
        for name, meta in self.db.db_metadata.items():
            if not meta.get("read_only", True) or name == "Clipbase":
                writable_dbs.append(name)

        if not writable_dbs:
            QMessageBox.warning(self, "Copiar Partida", "No hay bases de datos con permiso de escritura abiertas.")
            return

        # Diálogo para elegir destino
        from PySide6.QtWidgets import QInputDialog
        target_db, ok = QInputDialog.getItem(self, "Copiar Partida", 
                                           "Selecciona la base de datos destino:", 
                                           writable_dbs, 0, False)
        
        if ok and target_db:
            game_data["id"] = int(time.time() * 1000) # Nuevo ID único
            schema = self.db.dbs[target_db].schema
            # Asegurar consistencia de columnas
            clean_data = {k: game_data[k] for k in schema.keys()}
            
            if target_db == "Clipbase":
                self.db.add_to_clipbase(clean_data)
            else:
                self.db.dbs[target_db] = pl.concat([self.db.dbs[target_db], pl.DataFrame([clean_data], schema=schema)])
            
            self.statusBar().showMessage(f"Partida copiada a '{target_db}'", 3000)
            if self.db.active_db_name == target_db:
                self.db.set_active_db(target_db) # Refresca si estamos viéndola

    def on_db_table_context_menu(self, pos):
        menu = QMenu()
        copy_action = QAction(qta.icon('fa5s.copy'), "📋 Copiar Partida a...", self)
        copy_action.triggered.connect(self.copy_selected_game)
        menu.addAction(copy_action)
        
        edit_action = QAction(qta.icon('fa5s.edit'), "📝 Editar Datos Partida", self)
        edit_action.triggered.connect(self.edit_selected_game)
        menu.addAction(edit_action)
        
        is_readonly = self.db.db_metadata.get(self.db.active_db_name, {}).get("read_only", True)
        if not is_readonly or self.db.active_db_name == "Clipbase":
            menu.addSeparator()
            del_action = QAction(qta.icon('fa5s.trash-alt'), "❌ Eliminar Partida", self)
            del_action.triggered.connect(self.delete_selected_game)
            menu.addAction(del_action)
        menu.exec(self.db_table.viewport().mapToGlobal(pos))

    def on_db_list_context_menu(self, pos):
        item = self.db_list_widget.itemAt(pos)
        if not item or item.text() == "Clipbase": return
        
        name = item.text()
        is_readonly = self.db.db_metadata.get(name, {}).get("read_only", True)
        
        menu = QMenu()
        
        # Opción para alternar Solo Lectura
        if is_readonly:
            unlock_action = QAction(qta.icon('fa5s.unlock'), " Permitir Escritura", self)
            unlock_action.triggered.connect(lambda: self.toggle_db_readonly(item, False))
            menu.addAction(unlock_action)
        else:
            lock_action = QAction(qta.icon('fa5s.lock'), " Poner Solo Lectura", self)
            lock_action.triggered.connect(lambda: self.toggle_db_readonly(item, True))
            menu.addAction(lock_action)
            
        menu.addSeparator()
        remove_action = QAction(qta.icon('fa5s.times', color='red'), " Quitar de la lista", self)
        remove_action.triggered.connect(lambda: self.remove_database(item))
        menu.addAction(remove_action)
        menu.exec(self.db_list_widget.mapToGlobal(pos))

    def toggle_db_readonly(self, item, status):
        name = item.text()
        if self.db.set_readonly(name, status):
            if status:
                item.setForeground(QColor("#888888"))
                item.setIcon(qta.icon('fa5s.lock', color='#888888'))
            else:
                item.setForeground(QColor("#000000"))
                item.setIcon(qta.icon('fa5s.unlock', color='#2e7d32'))
            state = "Solo Lectura" if status else "Lectura/Escritura"
            self.statusBar().showMessage(f"Base '{name}' ahora en modo {state}", 3000)

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
        self.progress.hide(); name = self.db.load_parquet(path)
        items = self.db_list_widget.findItems(name, Qt.MatchExactly)
        if not items:
            item = QListWidgetItem(name)
            item.setForeground(QColor("#888888"))
            item.setIcon(qta.icon('fa5s.lock', color='#888888'))
            self.db_list_widget.addItem(item)
        else: item = items[0]
        self.db_list_widget.setCurrentItem(item); self.save_config()

    def init_menu(self):
        menubar = self.menuBar()
        
        # Menú Archivo
        file_menu = menubar.addMenu("&Archivo")
        
        settings_action = QAction(qta.icon('fa5s.cog'), "&Configuración...", self)
        settings_action.triggered.connect(self.open_settings)
        file_menu.addAction(settings_action)
        
        file_menu.addSeparator()
        
        exit_action = QAction(qta.icon('fa5s.power-off'), "&Salir", self)
        exit_action.setShortcut("Ctrl+Q")
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # Menú Bases de Datos
        db_menu = menubar.addMenu("&Bases de Datos")
        
        new_db_action = QAction(qta.icon('fa5s.plus-circle', color='#2e7d32'), "Nueva Base &Vacía...", self)
        new_db_action.setShortcut("Ctrl+N")
        new_db_action.triggered.connect(self.create_new_db)
        db_menu.addAction(new_db_action)
        
        open_parquet_action = QAction(qta.icon('fa5s.folder-open'), "Abrir Base &Parquet...", self)
        open_parquet_action.setShortcut("Ctrl+O")
        open_parquet_action.triggered.connect(self.open_parquet_file)
        db_menu.addAction(open_parquet_action)

        open_pgn_action = QAction(qta.icon('fa5s.file-import', color='#1976d2'), "Importar &PGN...", self)
        open_pgn_action.setShortcut("Ctrl+I")
        open_pgn_action.triggered.connect(self.import_pgn)
        db_menu.addAction(open_pgn_action)
        
        db_menu.addSeparator()
        
        # Acciones de Filtro en el Menú (Coherencia con la Toolbar)
        filter_action = QAction(qta.icon('fa5s.search'), "&Filtrar Partidas...", self)
        filter_action.setShortcut("Ctrl+F")
        filter_action.triggered.connect(self.open_search)
        db_menu.addAction(filter_action)
        
        invert_action = QAction(qta.icon('fa5s.exchange-alt'), "&Invertir Filtro", self)
        invert_action.triggered.connect(self.trigger_invert_filter)
        db_menu.addAction(invert_action)
        
        clear_action = QAction(qta.icon('fa5s.eraser', color='#c62828'), "&Quitar Filtros", self)
        clear_action.setShortcut("Ctrl+L")
        clear_action.triggered.connect(self.reset_filters)
        db_menu.addAction(clear_action)
        
        db_menu.addSeparator()

        export_pgn_action = QAction(qta.icon('fa5s.file-export'), "Exportar &Filtro a PGN...", self)
        export_pgn_action.setShortcut("Ctrl+E")
        export_pgn_action.triggered.connect(self.export_filter_to_pgn)
        db_menu.addAction(export_pgn_action)

        db_menu.addSeparator()
        
        delete_db_action = QAction(qta.icon('fa5s.trash-alt'), "&Eliminar Archivo de Base...", self)
        delete_db_action.triggered.connect(self.delete_current_db_file)
        db_menu.addAction(delete_db_action)
        
        # Menú Tablero
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

        # Menú Ayuda
        help_menu = menubar.addMenu("&Ayuda")
        about_action = QAction(qta.icon('fa5s.info-circle'), "&Acerca de...", self)
        about_action.triggered.connect(self.show_about_dialog)
        help_menu.addAction(about_action)

    def show_about_dialog(self):
        """Muestra el diálogo informativo del programa"""
        about_text = """
            <h3>fa-chess</h3>
            <p><b>Versión:</b> 1.0.0</p>
            <p><b>Autor:</b> Fabio Rueda</p>
            <hr>
            <p>Un clon moderno y ligero de Scid vs. PC enfocado en el rendimiento masivo.</p>
            <p><b>Tecnologías clave:</b>
            <ul>
                <li>Python & PySide6 (Qt)</li>
                <li>Polars 1.x (Motor de datos ultra-rápido)</li>
                <li>Python-Chess (Lógica de juego)</li>
            </ul>
            </p>
            <p>© 2026 Fabio Rueda</p>
        """
        QMessageBox.about(self, "Acerca de fa-chess", about_text)

    def create_new_db(self):
        path, _ = QFileDialog.getSaveFileName(self, "Crear Nueva Base", "/data/chess", "Chess Parquet (*.parquet)")
        if path:
            if not path.endswith(".parquet"): path += ".parquet"
            self.db.create_new_database(path)
            self.load_parquet(path) # Refrescar UI cargando la base recién creada
            self.statusBar().showMessage(f"Base creada y cargada: {os.path.basename(path)}", 3000)

    def delete_current_db_file(self):
        name = self.db.active_db_name
        if name == "Clipbase":
            QMessageBox.warning(self, "Acción no permitida", "No se puede eliminar el archivo de la Clipbase interna.")
            return
            
        ret = QMessageBox.question(self, "Eliminar Base", 
            f"¿Estás seguro de que quieres eliminar FISICAMENTE el archivo de la base '{name}'?\n\nEsta acción no se puede deshacer.",
            QMessageBox.Yes | QMessageBox.No)
            
        if ret == QMessageBox.Yes:
            # Necesitamos el item de la lista para llamar a remove_database
            items = self.db_list_widget.findItems(name, Qt.MatchExactly)
            if items:
                item = items[0]
                if self.db.delete_database_from_disk(name):
                    # El DBManager ya borró el archivo y el dato, 
                    # ahora quitamos el item de la lista visual
                    self.db_list_widget.takeItem(self.db_list_widget.row(item))
                    if self.db.active_db_name == "Clipbase": 
                        self.db_list_widget.setCurrentRow(0)
                    self.save_config()
                    self.statusBar().showMessage(f"Archivo de base '{name}' eliminado", 3000)
                else:
                    QMessageBox.critical(self, "Error", "No se pudo eliminar el archivo. Comprueba que no esté abierto por otro programa.")

    def on_opening_label_link(self, link):
        pass

    def export_filter_to_pgn(self):
        # Exportar partidas visibles (filtradas) a un archivo PGN
        df = self.db.current_filter_df if self.db.current_filter_df is not None else self.db.get_active_df()
        if df is None or df.is_empty():
            self.statusBar().showMessage("No hay partidas para exportar", 3000)
            return

        path, _ = QFileDialog.getSaveFileName(self, "Exportar Filtro a PGN", "/data/chess", "Chess PGN (*.pgn)")
        if not path: return
        if not path.endswith(".pgn"): path += ".pgn"

        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.show()
        
        self.export_worker = PGNExportWorker(df, path)
        self.export_worker.progress.connect(self.progress.setValue)
        self.export_worker.status.connect(self.statusBar().showMessage)
        self.export_worker.finished.connect(self.on_export_finished)
        self.export_worker.start()

    def on_export_finished(self, path):
        self.progress.hide()
        self.statusBar().showMessage(f"Exportadas partidas a {os.path.basename(path)}", 5000)
        QMessageBox.information(self, "Exportación Completada", f"Se han exportado las partidas correctamente a:\n{path}")

    def import_pgn(self):
        path, _ = QFileDialog.getOpenFileName(self, "Importar PGN", "/data/chess", "Chess PGN (*.pgn)")
        if path:
            self.progress.setRange(0, 100)
            self.progress.setValue(0)
            self.progress.show()
            QApplication.processEvents() # Forzar dibujado inicial
            
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

    def reset_filters(self):
        """Limpia el filtro actual y refresca toda la UI"""
        self.search_criteria = {"white": "", "black": "", "min_elo": "", "result": "Cualquiera"}
        self.db.set_active_db(self.db.active_db_name)
        # La señal active_db_changed ya disparará update_stats y refresh_db_list

    def resizeEvent(self, event): h = self.centralWidget().height() - 40; self.board_ana.setFixedWidth(h); super().resizeEvent(event)

    def trigger_invert_filter(self):
        self._just_inverted = True
        self.db.invert_filter()

    def switch_db(self, row):
        if row < 0: return
        self.db.set_active_db(self.db_list_widget.item(row).text())

    def sort_database(self, logical_index):
        col_name = self.col_mapping.get(logical_index)
        if not col_name: return
        
        self.progress.setRange(0, 0)
        self.progress.show()
        self.statusBar().showMessage(f"Ordenando base completa por {col_name}...")
        
        # Alternar orden
        if self.sort_col == col_name:
            self.sort_desc = not self.sort_desc
        else:
            self.sort_col = col_name
            self.sort_desc = True 
            
        # Actualizar flechita en cabecera
        order = Qt.DescendingOrder if self.sort_desc else Qt.AscendingOrder
        self.db_table.horizontalHeader().setSortIndicator(logical_index, order)
        QApplication.processEvents()
            
        # Ordenación Lazy Real
        self.db.sort_active_db(col_name, self.sort_desc)
        
        self.progress.hide()
        self.statusBar().showMessage("Listo", 2000)

    def open_search(self):
        dialog = SearchDialog(self)
        dialog.white_input.setText(self.search_criteria.get("white", ""))
        dialog.black_input.setText(self.search_criteria.get("black", ""))
        dialog.min_elo_input.setText(str(self.search_criteria.get("min_elo", "")))
        dialog.result_combo.setCurrentText(self.search_criteria.get("result", "Cualquiera"))
        
        # Sincronizar el checkbox con el estado actual
        dialog.pos_check.setChecked(self.search_criteria.get("use_position", False))
        
        if dialog.exec_():
            criteria = dialog.get_criteria()
            
            # Si todos los campos están vacíos, es equivalente a resetear filtros
            is_empty = not any([criteria.get("white"), criteria.get("black"), 
                              criteria.get("min_elo"), criteria.get("use_position")])
            if criteria.get("result") != "Cualquiera": is_empty = False

            if is_empty:
                self.reset_filters()
                return

            # Integrar búsqueda por posición si se ha marcado
            if criteria.get("use_position"):
                import chess.polyglot
                criteria["position_hash"] = chess.polyglot.zobrist_hash(self.game.board)
            
            self.search_criteria = criteria
            self.db.filter_db(self.search_criteria)
            # Cambiar a la pestaña del gestor para ver los resultados
            self.tabs.setCurrentIndex(1)

    def refresh_db_list(self, df_to_show=None):
        lazy_active = self.db.dbs.get(self.db.active_db_name)
        if lazy_active is None: return
        
        # Obtenemos el total real de la base activa (muy rápido en Lazy)
        total_db = self.db.get_active_count()
        is_filtered = self.db.current_filter_df is not None
        is_inverted = getattr(self, '_just_inverted', False)

        if not is_filtered:
            f_total = self.format_qty(total_db)
            self.label_db_stats.setText(f"[{f_total}/{f_total}]")
            self.label_db_stats.setStyleSheet(STYLE_BADGE_NORMAL)
            self.search_criteria = {"white": "", "black": "", "min_elo": "", "result": "Cualquiera", "use_position": False}
            df = self.db.get_active_df() # Head 1000
        else:
            # Obtenemos el total real del filtro (muy rápido en Lazy)
            count = self.db.get_view_count()
            f_count = self.format_qty(count)
            f_total = self.format_qty(total_db)
            self.label_db_stats.setText(f"[{f_count}/{f_total}]")
            
            if is_inverted:
                self.label_db_stats.setStyleSheet(STYLE_BADGE_ERROR)
                self._just_inverted = False 
            else:
                self.label_db_stats.setStyleSheet(STYLE_BADGE_SUCCESS)
            
            # Usamos la vista previa para la tabla
            df = df_to_show if df_to_show is not None else self.db.current_filter_df
        
        if df is None: return
        disp = df.head(1000); self.db_table.setRowCount(disp.height)
        for i, r in enumerate(disp.rows(named=True)):
            self.db_table.setItem(i, 0, QTableWidgetItem(r["date"])); self.db_table.setItem(i, 1, QTableWidgetItem(r["white"])); self.db_table.setItem(i, 2, QTableWidgetItem(str(r["w_elo"]))); self.db_table.setItem(i, 3, QTableWidgetItem(r["black"])); self.db_table.setItem(i, 4, QTableWidgetItem(str(r["b_elo"]))); self.db_table.setItem(i, 5, QTableWidgetItem(r["result"])); self.db_table.item(i, 0).setData(Qt.UserRole, r["id"])
        self.db_table.resizeColumnsToContents()

    def load_game_from_list(self, item):
        game_id = self.db_table.item(item.row(), 0).data(Qt.UserRole); row = self.db.get_game_by_id(self.db.active_db_name, game_id)
        if row: self.game.load_uci_line(row["full_line"]); self.tabs.setCurrentIndex(0)

    def jump_to_move_link(self, url):
        self.game.jump_to_move(int(url.toString()))

    def update_ui(self):
        self.tree_ana.clearSelection(); self.board_ana.set_hover_move(None); self.board_ana.update_board(); self.update_stats()
        
        # Sincronizar evaluaciones con la longitud de la partida
        current_len = len(self.game.full_mainline)
        if len(self.game_evals) != current_len + 1: # +1 por posición inicial
            # Si cambia la longitud (nueva partida o variante), reseteamos o ajustamos
            # Por simplicidad, si es muy distinto, reseteamos. Idealmente conservaríamos lo común.
            if abs(len(self.game_evals) - (current_len + 1)) > 1:
                self.game_evals = [None] * (current_len + 1)
            elif len(self.game_evals) < current_len + 1:
                self.game_evals.extend([None] * (current_len + 1 - len(self.game_evals)))
            else:
                self.game_evals = self.game_evals[:current_len + 1]
        
        self.eval_graph.set_evaluations(self.game_evals)
        self.eval_graph.set_current_move(self.game.current_idx)

        if hasattr(self, 'engine_worker') and self.engine_worker.isRunning(): self.engine_worker.update_position(self.game.board.fen())
        temp = chess.Board(); html = "<style>a { text-decoration: none; color: #222; } .active { background-color: #f6f669; }</style>"
        for i, m in enumerate(self.game.full_mainline):
            san_move = temp.san(m); num = (i//2)+1
            if i % 2 == 0: html += f"<b>{num}.</b> "
            st = "class='active'" if i == self.game.current_idx - 1 else ""
            html += f"<a {st} href='{i+1}'>{san_move}</a> "; temp.push(m)
        self.hist_ana.setHtml(html)

    def save_to_active_db(self):
        # Guardar partida actual en la base seleccionada
        db_name = self.db.active_db_name
        is_readonly = self.db.db_metadata.get(db_name, {}).get("read_only", True)
        
        if is_readonly and db_name != "Clipbase":
            QMessageBox.warning(self, "Base de Solo Lectura", 
                f"La base '{db_name}' es de solo lectura. Prueba a guardar en Clipbase.")
            return

        line_uci = self.game.current_line_uci
        import chess.polyglot
        board = chess.Board()
        hashes = [chess.polyglot.zobrist_hash(board)]
        for move in self.game.full_mainline:
            board.push(move)
            hashes.append(chess.polyglot.zobrist_hash(board))

        game_data = {
            "id": int(time.time() * 1000),
            "white": "Jugador Blanco", "black": "Jugador Negro",
            "w_elo": 2500, "b_elo": 2500, "result": "*", 
            "date": datetime.now().strftime("%Y.%m.%d"),
            "event": "Análisis Local", 
            "line": " ".join([m.uci() for m in self.game.full_mainline[:12]]),
            "full_line": line_uci,
            "fens": hashes
        }
        
        if db_name == "Clipbase":
            self.db.add_to_clipbase(game_data)
        else:
            # Para bases Parquet, añadimos al DataFrame en memoria
            df = self.db.get_active_df()
            self.db.dbs[db_name] = pl.concat([df, pl.DataFrame([game_data], schema=df.schema)])
            # Aquí podríamos guardar a disco físicamente si quisiéramos persistencia real
            
        self.db.set_active_db(db_name) # Refresca vista
        self.statusBar().showMessage(f"Partida guardada en {db_name}", 3000)

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
        # 1. Gestionar hilos anteriores para evitar el error "Destroyed while running"
        if not hasattr(self, '_active_workers'):
            self._active_workers = []
            
        # Limpiar referencias a hilos que ya terminaron
        self._active_workers = [w for w in self._active_workers if w.isRunning()]
        
        # Si el hilo principal está corriendo, lo desconectamos del resultado
        if hasattr(self, 'stats_worker') and self.stats_worker.isRunning():
            try:
                self.stats_worker.finished.disconnect()
                # Lo movemos a la lista de huérfanos para que no sea destruido por Python
                self._active_workers.append(self.stats_worker)
            except: pass
        
        # 2. Preparar el nuevo cálculo
        self.progress.setRange(0, 0)
        self.progress.show()
        self.statusBar().showMessage("Calculando estadísticas...")
        
        import chess.polyglot
        current_hash = chess.polyglot.zobrist_hash(self.game.board)
        
        # 3. Lanzar el nuevo Worker
        self.stats_worker = StatsWorker(self.db, self.game.current_line_uci, self.game.board.turn == chess.WHITE, current_hash)
        self.stats_worker.finished.connect(self.on_stats_finished)
        self.stats_worker.start()

    def update_stats(self):
        is_filtered = self.db.current_filter_df is not None
        status_text = " (Filtrado)" if is_filtered else ""
        self.label_eco.setText(f"Apertura: {self.eco.get_opening_name(self.game.current_line_uci)}{status_text}")
        # Solo ponemos verde si hay un filtro ACTIVO
        self.label_eco.setStyleSheet(STYLE_BADGE_SUCCESS if is_filtered else STYLE_BADGE_NORMAL)
        self.stats_timer.start(50) # Más rápido para sensación de respuesta, la caché ayuda

    def format_qty(self, n, precise=False):
        """Formatea números grandes: 4900000 -> 4.9M o 4.900.000"""
        if precise:
            return f"{n:,}".replace(",", ".")
        if n >= 1_000_000:
            return f"{n/1_000_000:.2f}M".replace(".00M", "M")
        if n >= 1_000:
            return f"{n/1_000:.1f}k".replace(".0k", "k")
        return str(n)

    def on_stats_finished(self, res):
        self.progress.hide()
        
        # Calcular total de partidas en la posición
        total_pos = 0
        is_partial = False
        if res is not None and res.height > 0:
            total_pos = res.select(pl.sum("c")).item()
            is_partial = "_is_partial" in res.columns and res.row(0, named=True).get("_is_partial")
        
        # Obtener total de la VISTA actual (consulta completa)
        total_view = self.db.get_view_count()
        
        # Formatear números
        txt_pos = self.format_qty(total_pos)
        txt_total = self.format_qty(total_view)
        
        # Actualizar Etiqueta de Estadísticas (Badge)
        is_filtered = self.db.current_filter_df is not None
        if is_partial:
            self.label_pos_stats.setText(f"Muestra: {txt_pos} de {txt_total}")
            self.label_pos_stats.setStyleSheet(STYLE_BADGE_ERROR)
            self.label_pos_stats.setToolTip("Haz clic para generar el índice completo (Árbol)")
        else:
            self.label_pos_stats.setText(f"Partidas: {txt_pos} de {txt_total}")
            # Verde si hay filtro, Gris si no
            self.label_pos_stats.setStyleSheet(STYLE_BADGE_SUCCESS if is_filtered else STYLE_BADGE_NORMAL)
            self.label_pos_stats.setToolTip("")

        if is_partial:
            self.statusBar().showMessage("⚠️ Árbol parcial. Haz clic en 'Muestra' para generar el índice completo.", 5000)
        else:
            self.statusBar().showMessage("Listo", 2000)
            
        if res is None or total_pos <= 1: 
            self.tree_ana.setRowCount(0)
            if total_pos == 1:
                self.statusBar().showMessage("Posición única: no hay más variantes en la base", 3000)
            return
            
        table = self.tree_ana; table.setSortingEnabled(False); table.setRowCount(res.height); is_white = self.game.board.turn == chess.WHITE
        
        valid_rows = 0
        for i, r in enumerate(res.rows(named=True)):
            try:
                mv = chess.Move.from_uci(r["uci"])
                if mv not in self.game.board.legal_moves:
                    continue # Saltar jugadas que ya no son legales
                
                san = self.game.board.san(mv)
                it_move = QTableWidgetItem(san); it_move.setData(Qt.UserRole, r["uci"]); table.setItem(valid_rows, 0, it_move)
                
                # Frecuencia: Mostrar con puntos, ordenar por número real
                f_count = self.format_qty(r["c"], precise=True)
                it_count = SortableWidgetItem(f_count)
                it_count.setData(Qt.UserRole, r["c"])
                table.setItem(valid_rows, 1, it_count)
                
                table.setCellWidget(valid_rows, 2, ResultsWidget(r["w"], r["d"], r["b"], r["c"], is_white))
                
                # Win %: Mostrar con %, ordenar por número real
                win_rate = ((r["w"] + 0.5 * r["d"]) / r["c"] if is_white else (r["b"] + 0.5 * r["d"]) / r["c"]) * 100
                it_win = SortableWidgetItem(f"{win_rate:.1f}%")
                it_win.setData(Qt.UserRole, win_rate)
                it_win.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                table.setItem(valid_rows, 3, it_win)
                
                score = (r["w"] + 0.5 * r["d"]) / r["c"] if is_white else (r["b"] + 0.5 * r["d"]) / r["c"]
                perf = int(r["avg_b_elo" if is_white else "avg_w_elo"] + (score - 0.5) * 800)
                
                # Elos y Perf: Orden numérico
                w_elo_val = int(r["avg_w_elo" if is_white else "avg_b_elo"])
                it_elo = SortableWidgetItem(str(w_elo_val))
                it_elo.setData(Qt.UserRole, w_elo_val)
                table.setItem(valid_rows, 4, it_elo)
                
                it_perf = SortableWidgetItem(str(perf))
                it_perf.setData(Qt.UserRole, perf)
                table.setItem(valid_rows, 5, it_perf)
                
                valid_rows += 1
            except: continue
            
        table.setRowCount(valid_rows)
        table.setSortingEnabled(True)
        table.sortByColumn(1, Qt.DescendingOrder)
        table.resizeColumnsToContents()