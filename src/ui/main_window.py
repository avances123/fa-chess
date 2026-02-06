import os
import json
import time
from datetime import datetime
import chess
import polars as pl
from PySide6.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                             QTableWidget, QTableWidgetItem, QLabel, QPushButton, 
                             QFileDialog, QProgressBar, QHeaderView, QTextBrowser, 
                             QStatusBar, QTabWidget, QLineEdit, QListWidget, QMenu, 
                             QCheckBox, QColorDialog, QListWidgetItem, QMenuBar, 
                             QAbstractItemView, QToolBar, QStyle, QSizePolicy)
from PySide6.QtCore import Qt
from PySide6.QtGui import QAction, QColor, QFont, QShortcut, QKeySequence

from config import CONFIG_FILE, LIGHT_STYLE, ECO_FILE
from core.workers import PGNWorker, MaskWorker
from core.eco import ECOManager
from ui.board import ChessBoard
from ui.settings_dialog import SettingsDialog
from ui.search_dialog import SearchDialog
from ui.edit_game_dialog import EditGameDialog
from PySide6.QtWidgets import QMessageBox

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("fa-chess")
        self.setStyleSheet(LIGHT_STYLE)
        
        self.board = chess.Board()
        self.full_mainline = []
        self.current_idx = 0
        self.dbs = {} 
        self.db_metadata = {}
        self.active_db_name = "Clipbase"
        self.masks = {}
        
        # Cargar gestor ECO desde la configuración
        self.eco = ECOManager(ECO_FILE)
        
        self.init_clipbase()
        self.load_config() # Cargar antes de init_ui
        self.init_ui()
        self.init_menu()
        self.init_shortcuts()

        # Inicializar barra de estado
        self.statusBar().showMessage("Listo")

    def init_shortcuts(self):
        # Atajos de flechas para navegación global
        QShortcut(QKeySequence(Qt.Key_Left), self, self.step_back)
        QShortcut(QKeySequence(Qt.Key_Right), self, self.step_forward)
        QShortcut(QKeySequence(Qt.Key_Home), self, self.go_start)
        QShortcut(QKeySequence(Qt.Key_End), self, self.go_end)
        QShortcut(QKeySequence("F"), self, self.flip_boards)

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
        
        # Contenedor para Tablero + Toolbar
        board_container = QWidget()
        board_container_layout = QVBoxLayout(board_container)
        board_container_layout.setContentsMargins(0,0,0,0)
        board_container_layout.setSpacing(0)
        
        self.toolbar_ana = QToolBar()
        self.toolbar_ana.setMovable(False)
        self.setup_toolbar(self.toolbar_ana)
        board_container_layout.addWidget(self.toolbar_ana)
        
        self.board_ana = ChessBoard(self.board, self)
        self.board_ana.color_light = self.def_light
        self.board_ana.color_dark = self.def_dark
        board_container_layout.addWidget(self.board_ana)
        ana_layout.addWidget(board_container)
        
        panel_ana = QWidget(); p_ana_layout = QVBoxLayout(panel_ana)
        self.label_apertura = QLabel("<b>Apertura:</b> Inicial")
        p_ana_layout.addWidget(self.label_apertura)
        
        self.tree_ana = self.create_scid_table(["Movim.", "Frec.", "Win %", "Tablas", "AvElo", "Perf"])
        self.tree_ana.setFont(table_font)
        self.tree_ana.cellClicked.connect(self.on_tree_cell_click)
        p_ana_layout.addWidget(self.tree_ana)
        
        self.hist_ana = QTextBrowser(); self.hist_ana.setOpenLinks(False)
        self.hist_ana.anchorClicked.connect(self.jump_to_move); p_ana_layout.addWidget(self.hist_ana)
        
        btn_save = QPushButton("💾 Guardar Partida en Clipbase")
        btn_save.clicked.connect(self.add_to_clipbase)
        p_ana_layout.addWidget(btn_save)

        ana_layout.addWidget(panel_ana, 1)

        # --- TAB 2: GESTOR ---
        self.tab_db = QWidget()
        self.tabs.addTab(self.tab_db, "Gestor Bases")
        db_layout = QHBoxLayout(self.tab_db); db_sidebar = QVBoxLayout()
        self.db_list_widget = QListWidget(); self.db_list_widget.addItem("Clipbase")
        self.db_list_widget.setContextMenuPolicy(Qt.CustomContextMenu)
        self.db_list_widget.currentRowChanged.connect(self.switch_db)
        self.db_list_widget.customContextMenuRequested.connect(self.on_db_list_context_menu)
        self.progress = QProgressBar(); self.progress.setVisible(False)
        db_sidebar.addWidget(self.db_list_widget)
        
        self.btn_search = QPushButton("🔍 Filtrar Partidas")
        self.btn_search.clicked.connect(self.open_search)
        db_sidebar.addWidget(self.btn_search)
        
        self.label_db_stats = QLabel("[0/0]")
        self.label_db_stats.setAlignment(Qt.AlignCenter)
        db_sidebar.addWidget(self.label_db_stats)
        
        db_sidebar.addWidget(self.progress)
        db_layout.addLayout(db_sidebar, 1)
        
        self.search_criteria = {"white": "", "black": "", "min_elo": "", "result": "Cualquiera"}
        
        db_content = QVBoxLayout()
        self.db_table = self.create_scid_table(["Fecha", "Blancas", "Elo B", "Negras", "Elo N", "Res"])
        self.db_table.setFont(table_font)
        self.db_table.itemDoubleClicked.connect(self.load_game_from_list)
        self.db_table.customContextMenuRequested.connect(self.on_db_table_context_menu)
        db_content.addWidget(self.db_table); db_layout.addLayout(db_content, 4)

        # Cargar DBs que quedaron pendientes del config
        for path in getattr(self, 'pending_dbs', []):
            if os.path.exists(path): self.load_parquet(path)

    def create_scid_table(self, headers):
        table = QTableWidget(0, len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        table.setContextMenuPolicy(Qt.CustomContextMenu)
        table.verticalHeader().setVisible(False)
        table.verticalHeader().setDefaultSectionSize(22)
        table.horizontalHeader().setHighlightSections(False)
        table.setShowGrid(True)
        table.setStyleSheet("QHeaderView::section { font-weight: bold; }")
        return table

    def setup_toolbar(self, toolbar):
        style = self.style()
        
        # Espaciador izquierdo
        left_spacer = QWidget()
        left_spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        toolbar.addWidget(left_spacer)

        actions = [
            (style.standardIcon(QStyle.StandardPixmap.SP_MediaSkipBackward), self.go_start, "Inicio (Home)"),
            (style.standardIcon(QStyle.StandardPixmap.SP_ArrowBack), self.step_back, "Anterior (Izquierda)"),
            (style.standardIcon(QStyle.StandardPixmap.SP_ArrowForward), self.step_forward, "Siguiente (Derecha)"),
            (style.standardIcon(QStyle.StandardPixmap.SP_MediaSkipForward), self.go_end, "Final (End)"),
            (None, None, None), # Separador
            (style.standardIcon(QStyle.StandardPixmap.SP_BrowserReload), self.flip_boards, "Girar Tablero (F)"),
        ]
        
        for icon, func, tip in actions:
            if icon is None:
                toolbar.addSeparator()
            else:
                action = toolbar.addAction(icon, "")
                action.triggered.connect(func)
                action.setToolTip(tip)

        # Espaciador derecho
        right_spacer = QWidget()
        right_spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        toolbar.addWidget(right_spacer)

    def add_to_clipbase(self):
        line_uci = " ".join([m.uci() for m in self.full_mainline])
        game_data = {
            "id": int(time.time()), 
            "white": "Jugador Blanco", "black": "Jugador Negro",
            "w_elo": 2500, "b_elo": 2500, "result": "*", 
            "date": datetime.now().strftime("%Y.%m.%d"),
            "event": "Análisis Local", 
            "line": line_uci, "full_line": line_uci
        }
        self.dbs["Clipbase"] = pl.concat([self.dbs["Clipbase"], pl.DataFrame([game_data])])
        self.statusBar().showMessage("Partida guardada en Clipbase", 3000)
        if self.active_db_name == "Clipbase":
            self.refresh_db_list()

    def delete_selected_game(self):
        row = self.db_table.currentRow()
        if row >= 0:
            game_id = self.db_table.item(row, 0).data(Qt.UserRole)
            self.dbs[self.active_db_name] = self.dbs[self.active_db_name].filter(pl.col("id") != game_id)
            self.refresh_db_list()
            self.statusBar().showMessage("Partida eliminada", 2000)

    def on_db_table_context_menu(self, pos):
        menu = QMenu()
        
        # Opción para copiar a Clipbase
        copy_action = QAction("📋 Copiar a Clipbase", self)
        copy_action.triggered.connect(self.copy_to_clipbase)
        menu.addAction(copy_action)
        
        # Opción para editar (solo si hay una fila seleccionada)
        edit_action = QAction("📝 Editar Datos Partida", self)
        edit_action.triggered.connect(self.edit_selected_game)
        menu.addAction(edit_action)

        # Eliminar solo si no es de solo lectura o es Clipbase
        is_readonly = self.db_metadata.get(self.active_db_name, {}).get("read_only", True)
        if not is_readonly or self.active_db_name == "Clipbase":
            menu.addSeparator()
            del_action = QAction("❌ Eliminar Partida", self)
            del_action.triggered.connect(self.delete_selected_game)
            menu.addAction(del_action)
            
        menu.exec(self.db_table.viewport().mapToGlobal(pos))

    def on_db_list_context_menu(self, pos):
        item = self.db_list_widget.itemAt(pos)
        if not item or item.text() == "Clipbase": return
        
        menu = QMenu()
        remove_action = QAction("❌ Quitar de la lista", self)
        remove_action.triggered.connect(lambda: self.remove_database(item))
        menu.addAction(remove_action)
        menu.exec(self.db_list_widget.mapToGlobal(pos))

    def remove_database(self, item):
        name = item.text()
        if name in self.dbs: del self.dbs[name]
        if name in self.db_metadata: del self.db_metadata[name]
        
        row = self.db_list_widget.row(item)
        self.db_list_widget.takeItem(row)
        
        # Cambiar a Clipbase si borramos la activa
        if self.active_db_name == name:
            self.db_list_widget.setCurrentRow(0)
            
        self.save_config()
        self.statusBar().showMessage(f"Base '{name}' quitada", 2000)

    def copy_to_clipbase(self):
        row = self.db_table.currentRow()
        if row >= 0:
            game_id = self.db_table.item(row, 0).data(Qt.UserRole)
            game_df = self.dbs[self.active_db_name].filter(pl.col("id") == game_id)
            if not game_df.is_empty():
                # Forzar el esquema de Clipbase para evitar errores de tipo
                new_game = game_df.with_columns([
                    pl.lit(int(time.time() * 1000)).alias("id") # ID único basado en ms
                ]).select(self.dbs["Clipbase"].columns)
                
                self.dbs["Clipbase"] = pl.concat([self.dbs["Clipbase"], new_game])
                self.statusBar().showMessage("Partida copiada a Clipbase", 2000)
                if self.active_db_name == "Clipbase": self.refresh_db_list()

    def edit_selected_game(self):
        row = self.db_table.currentRow()
        if row < 0: return
        
        game_id = self.db_table.item(row, 0).data(Qt.UserRole)
        is_readonly = self.db_metadata.get(self.active_db_name, {}).get("read_only", True)
        
        # Si es de solo lectura, proponemos copiar a Clipbase
        target_db = self.active_db_name
        if is_readonly and self.active_db_name != "Clipbase":
            ret = QMessageBox.question(self, "Base de Solo Lectura", 
                "Esta base es de solo lectura. ¿Deseas copiar esta partida a la Clipbase para editarla?",
                QMessageBox.Yes | QMessageBox.No)
            if ret == QMessageBox.Yes:
                self.copy_to_clipbase()
                self.active_db_name = "Clipbase"
                # Actualizar selección a la última de Clipbase
                self.refresh_db_list()
                row = self.db_table.rowCount() - 1
                game_id = self.db_table.item(row, 0).data(Qt.UserRole)
                target_db = "Clipbase"
            else: return

        # Obtener datos actuales
        current_row = self.dbs[target_db].filter(pl.col("id") == game_id).row(0, named=True)
        dialog = EditGameDialog(current_row, self)
        if dialog.exec_():
            new_data = dialog.get_data()
            # Actualizar el DataFrame de Polars
            self.dbs[target_db] = self.dbs[target_db].with_columns([
                pl.when(pl.col("id") == game_id).then(pl.lit(new_data[k])).otherwise(pl.col(k)).alias(k)
                for k in new_data.keys()
            ])
            self.refresh_db_list()
            self.statusBar().showMessage("Partida actualizada", 2000)

    def load_parquet(self, path):
        self.progress.setVisible(False)
        name = os.path.basename(path)
        self.dbs[name] = pl.read_parquet(path)
        # Por defecto, bases cargadas son de solo lectura
        self.db_metadata[name] = {"read_only": True, "path": path}
        if not self.db_list_widget.findItems(name, Qt.MatchExactly):
            self.db_list_widget.addItem(name)
        self.db_list_widget.setCurrentRow(self.db_list_widget.count()-1)
        self.save_config()
        self.refresh_db_list()

    def init_menu(self):
        menubar = self.menuBar()
        file_menu = menubar.addMenu("&Archivo")
        open_action = QAction("&Abrir PGN...", self); open_action.setShortcut("Ctrl+O"); open_action.triggered.connect(self.open_file); file_menu.addAction(open_action)
        exit_action = QAction("&Salir", self); exit_action.setShortcut("Ctrl+Q"); exit_action.triggered.connect(self.close); file_menu.addAction(exit_action)
        board_menu = menubar.addMenu("&Tablero")
        settings_action = QAction("&Configuración...", self); settings_action.triggered.connect(self.open_settings); board_menu.addAction(settings_action)

    def open_settings(self):
        dialog = SettingsDialog(self.board_ana.color_light, self.board_ana.color_dark, self)
        if dialog.exec_():
            light, dark = dialog.get_colors()
            self.board_ana.color_light = light
            self.board_ana.color_dark = dark
            self.board_ana.update_board()
            self.save_config()

    def resizeEvent(self, event):
        h = self.centralWidget().height() - 40
        self.board_ana.setFixedWidth(h)
        super().resizeEvent(event)

    def init_clipbase(self):
        self.dbs["Clipbase"] = pl.DataFrame(schema={"id": pl.Int64, "white": pl.String, "black": pl.String, "w_elo": pl.Int64, "b_elo": pl.Int64, "result": pl.String, "date": pl.String, "event": pl.String, "line": pl.String, "full_line": pl.String})
        self.db_metadata["Clipbase"] = {"read_only": False, "path": None}

    def open_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Abrir", "/data/chess", "Chess (*.pgn *.parquet)")
        if path:
            p_path = path.replace(".pgn", ".parquet")
            if os.path.exists(p_path): self.load_parquet(p_path)
            else:
                self.progress.setVisible(True)
                self.worker = PGNWorker(path); self.worker.progress.connect(self.progress.setValue); self.worker.status.connect(self.statusBar().showMessage); self.worker.finished.connect(self.load_parquet); self.worker.start()

    def load_parquet(self, path):
        self.progress.setVisible(False); name = os.path.basename(path); self.dbs[name] = pl.read_parquet(path)
        self.db_metadata[name] = {"read_only": False, "path": path}
        if not self.db_list_widget.findItems(name, Qt.MatchExactly): self.db_list_widget.addItem(name)
        self.db_list_widget.setCurrentRow(self.db_list_widget.count()-1); self.save_config(); self.refresh_db_list()

    def switch_db(self, row):
        if row < 0: return
        self.active_db_name = self.db_list_widget.item(row).text(); self.refresh_db_list(); self.update_stats()

    def open_search(self):
        dialog = SearchDialog(self)
        # Cargar estado previo
        dialog.white_input.setText(self.search_criteria["white"])
        dialog.black_input.setText(self.search_criteria["black"])
        dialog.min_elo_input.setText(self.search_criteria["min_elo"])
        dialog.result_combo.setCurrentText(self.search_criteria["result"])
        
        if dialog.exec_():
            self.search_criteria = dialog.get_criteria()
            self.filter_db(self.search_criteria)

    def filter_db(self, c):
        df = self.dbs.get(self.active_db_name)
        if df is None: return
        
        total = df.height
        q = df.lazy()
        if c["white"]: q = q.filter(pl.col("white").str.contains(c["white"]))
        if c["black"]: q = q.filter(pl.col("black").str.contains(c["black"]))
        if c["min_elo"].isdigit():
            m = int(c["min_elo"])
            q = q.filter((pl.col("w_elo") >= m) | (pl.col("b_elo") >= m))
        if c["result"] != "Cualquiera":
            q = q.filter(pl.col("result") == c["result"])
        
        filtered_df = q.collect()
        count = filtered_df.height
        self.label_db_stats.setText(f"[{count}/{total}]")
        self.refresh_db_list(filtered_df)

    def refresh_db_list(self, df_to_show=None):
        df = df_to_show if df_to_show is not None else self.dbs.get(self.active_db_name)
        if df is None: return
        
        # Si no se pasa un DF filtrado (ej: al cambiar de DB), actualizar stats
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
        self.db_table.resizeColumnsToContents()

    def load_game_from_list(self, item):
        game_id = self.db_table.item(item.row(), 0).data(Qt.UserRole)
        row = self.dbs[self.active_db_name].filter(pl.col("id") == game_id).row(0, named=True)
        self.board.reset(); self.full_mainline = []
        for uci in row["full_line"].split():
            if uci: self.full_mainline.append(chess.Move.from_uci(uci))
        self.current_idx = 0; self.tabs.setCurrentIndex(0); self.update_ui()

    def make_move(self, move):
        if self.current_idx < len(self.full_mainline):
            if self.full_mainline[self.current_idx] == move:
                # Si es la misma jugada, solo avanzamos
                self.step_forward()
                return
            # Si es una jugada diferente, cortamos la línea
            self.full_mainline = self.full_mainline[:self.current_idx]
        
        self.board.push(move)
        self.full_mainline.append(move)
        self.current_idx += 1
        self.update_ui()

    def step_back(self):
        if self.current_idx > 0: self.current_idx -= 1; self.board.pop(); self.update_ui()

    def step_forward(self):
        if self.current_idx < len(self.full_mainline): self.board.push(self.full_mainline[self.current_idx]); self.current_idx += 1; self.update_ui()

    def go_start(self):
        while self.current_idx > 0: self.step_back()

    def go_end(self):
        while self.current_idx < len(self.full_mainline): self.step_forward()

    def jump_to_move(self, url):
        idx = int(url.toString())
        while self.current_idx > idx: self.step_back()
        while self.current_idx < idx: self.step_forward()

    def update_ui(self):
        self.board_ana.update_board(); self.update_stats()
        temp = chess.Board(); html = "<style>a { text-decoration: none; color: #222; } .active { background-color: #f6f669; }</style>"
        for i, m in enumerate(self.full_mainline[:len(self.full_mainline)]):
            san_move = temp.san(m); num = (i//2)+1
            if i % 2 == 0: html += f"<b>{num}.</b> "
            st = "class='active'" if i == self.current_idx - 1 else ""
            html += f"<a {st} href='{i+1}'>{san_move}</a> "; temp.push(m)
        self.hist_ana.setHtml(html)

    def on_tree_cell_click(self, row, column):
        table = self.sender()
        it = table.item(row, 0); uci = it.data(Qt.UserRole)
        if uci: self.make_move(chess.Move.from_uci(uci))

    def save_config(self):
        dbs_info = [{"path": m["path"]} for m in self.db_metadata.values() if m.get("path")]
        # Usar colores actuales de los tableros si existen, si no los por defecto
        colors = {
            "light": self.board_ana.color_light if hasattr(self, 'board_ana') else "#eeeed2",
            "dark": self.board_ana.color_dark if hasattr(self, 'board_ana') else "#8ca2ad"
        }
        config = {"dbs": dbs_info, "colors": colors}
        with open(CONFIG_FILE, "w") as f: json.dump(config, f)

    def load_config(self):
        # Valores por defecto
        self.def_light = "#eeeed2"
        self.def_dark = "#8ca2ad"
        
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r") as f:
                try:
                    data = json.load(f)
                    # Guardamos las rutas de DBs para cargarlas luego en init_ui o similar
                    self.pending_dbs = [info["path"] if isinstance(info, dict) else info for info in data.get("dbs", [])]
                    
                    colors = data.get("colors")
                    if colors:
                        self.def_light = colors.get("light", self.def_light)
                        self.def_dark = colors.get("dark", self.def_dark)
                except: pass

    def update_stats(self):
        moves = self.board.move_stack
        line_uci = " ".join([m.uci() for m in moves])
        
        # --- Obtener Nombre Humano de Apertura (ECO) ---
        ap_nombre = self.eco.get_opening_name(line_uci)
        self.label_apertura.setText(f"<b>Apertura:</b> {ap_nombre}")

        df = self.dbs.get(self.active_db_name)
        if df is None:
            self.tree_ana.setRowCount(0)
            return

        try:
            res = df.lazy().filter(pl.col("line").str.starts_with(line_uci)).select([
                pl.col("line").str.slice(len(line_uci)).str.strip_chars().str.split(" ").list.get(0).alias("uci"),
                pl.col("result"),
                pl.col("w_elo"),
                pl.col("b_elo")
            ]).filter(pl.col("uci").is_not_null() & (pl.col("uci") != "")).group_by("uci").agg([
                pl.count().alias("c"),
                pl.col("result").filter(pl.col("result") == "1-0").count().alias("w"),
                pl.col("result").filter(pl.col("result") == "0-1").count().alias("b"),
                pl.col("result").filter(pl.col("result") == "1/2-1/2").count().alias("d"),
                pl.col("w_elo").mean().alias("avg_w_elo"),
                pl.col("b_elo").mean().alias("avg_b_elo")
            ]).sort("c", descending=True).limit(15).collect()

            is_white = self.board.turn == chess.WHITE

            table = self.tree_ana
            table.setRowCount(res.height)
            for i, r in enumerate(res.rows(named=True)):
                mv = chess.Move.from_uci(r["uci"]); san = self.board.san(mv)
                it_move = QTableWidgetItem(san); it_move.setData(Qt.UserRole, r["uci"])
                table.setItem(i, 0, it_move)
                
                count = r["c"]; w, b, d = r["w"], r["b"], r["d"]
                if is_white:
                    score = (w + 0.5 * d) / count
                    perf = int(r["avg_b_elo"] + (score - 0.5) * 800)
                else:
                    score = (b + 0.5 * d) / count
                    perf = int(r["avg_w_elo"] + (score - 0.5) * 800)

                cols = [str(count), f"{(w/count)*100:.1f}%", f"{(d/count)*100:.1f}%", str(int(r["avg_w_elo" if is_white else "avg_b_elo"])), str(perf)]
                for col_idx, text in enumerate(cols, start=1):
                    it = QTableWidgetItem(text); it.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter); table.setItem(i, col_idx, it)

            table.resizeColumnsToContents()
        except: pass