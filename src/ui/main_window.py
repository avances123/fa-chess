import os
import json
import chess
import polars as pl
from PySide6.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                             QTableWidget, QTableWidgetItem, QLabel, QPushButton, 
                             QFileDialog, QProgressBar, QHeaderView, QTextBrowser, 
                             QStatusBar, QTabWidget, QLineEdit, QListWidget, QMenu, 
                             QCheckBox, QColorDialog, QListWidgetItem, QMenuBar, QAbstractItemView)
from PySide6.QtCore import Qt
from PySide6.QtGui import QAction, QColor, QFont, QShortcut, QKeySequence

from config import CONFIG_FILE, LIGHT_STYLE, ECO_FILE
from core.workers import PGNWorker, MaskWorker
from core.eco import ECOManager
from ui.board import ChessBoard
from ui.settings_dialog import SettingsDialog

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
        self.init_ui()
        self.init_menu()
        self.init_shortcuts()
        self.load_config()

    def init_shortcuts(self):
        # Atajos de flechas para navegación global
        QShortcut(QKeySequence(Qt.Key_Left), self, self.step_back)
        QShortcut(QKeySequence(Qt.Key_Right), self, self.step_forward)
        QShortcut(QKeySequence(Qt.Key_Home), self, self.go_start)
        QShortcut(QKeySequence(Qt.Key_End), self, self.go_end)

    def init_ui(self):
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)

        table_font = QFont("monospace", 9)

        # --- TAB 1: ANÁLISIS ---
        self.tab_analysis = QWidget()
        self.tabs.addTab(self.tab_analysis, "Tablero")
        ana_layout = QHBoxLayout(self.tab_analysis); ana_layout.setContentsMargins(0, 0, 0, 0)
        self.board_ana = ChessBoard(self.board, self); ana_layout.addWidget(self.board_ana)
        
        panel_ana = QWidget(); p_ana_layout = QVBoxLayout(panel_ana)
        self.label_apertura = QLabel("<b>Apertura:</b> Inicial")
        p_ana_layout.addWidget(self.label_apertura)
        
        self.tree_ana = self.create_scid_table(["Movim.", "Frec.", "Win %", "Tablas", "AvElo", "Perf"])
        self.tree_ana.setFont(table_font)
        self.tree_ana.cellClicked.connect(self.on_tree_cell_click)
        p_ana_layout.addWidget(self.tree_ana)
        
        nav_box = QHBoxLayout()
        for label, func in [("|<", self.go_start), ("<", self.step_back), (">", self.step_forward), (">|", self.go_end)]:
            btn = QPushButton(label); btn.clicked.connect(func); nav_box.addWidget(btn)
        p_ana_layout.addLayout(nav_box)
        
        self.hist_ana = QTextBrowser(); self.hist_ana.setOpenLinks(False)
        self.hist_ana.anchorClicked.connect(self.jump_to_move); p_ana_layout.addWidget(self.hist_ana)
        ana_layout.addWidget(panel_ana, 1)

        # --- TAB 2: GESTOR ---
        self.tab_db = QWidget()
        self.tabs.addTab(self.tab_db, "Gestor Bases")
        db_layout = QHBoxLayout(self.tab_db); db_sidebar = QVBoxLayout()
        self.db_list_widget = QListWidget(); self.db_list_widget.addItem("Clipbase")
        self.db_list_widget.currentRowChanged.connect(self.switch_db)
        self.progress = QProgressBar(); self.progress.setVisible(False)
        db_sidebar.addWidget(self.db_list_widget); db_sidebar.addWidget(self.progress); db_layout.addLayout(db_sidebar, 1)
        
        db_content = QVBoxLayout()
        self.db_table = self.create_scid_table(["Fecha", "Blancas", "Elo B", "Negras", "Elo N", "Res"])
        self.db_table.setFont(table_font)
        self.db_table.itemDoubleClicked.connect(self.load_game_from_list)
        db_content.addWidget(self.db_table); db_layout.addLayout(db_content, 4)

        # --- TAB 3: LABORATORIO ---
        self.tab_lab = QWidget()
        self.tabs.addTab(self.tab_lab, "Laboratorio")
        lab_layout = QHBoxLayout(self.tab_lab); lab_layout.setContentsMargins(0, 0, 0, 0)
        self.board_lab = ChessBoard(self.board, self); lab_layout.addWidget(self.board_lab)
        panel_lab = QWidget(); p_lab_layout = QVBoxLayout(panel_lab)
        
        self.label_apertura_lab = QLabel("<b>Apertura:</b> Inicial")
        p_lab_layout.addWidget(self.label_apertura_lab)
        
        self.tree_lab = self.create_scid_table(["Movim.", "Frec.", "Win %", "Tablas", "AvElo", "Perf", "Masc."])
        self.tree_lab.setFont(table_font)
        self.tree_lab.cellClicked.connect(self.on_tree_cell_click)
        p_lab_layout.addWidget(self.tree_lab)
        
        p_lab_layout.addWidget(QLabel("<b>Gestión de Máscaras</b>"))
        self.mask_list = QListWidget(); p_lab_layout.addWidget(self.mask_list)
        mask_btns = QHBoxLayout()
        btn_new = QPushButton("Nueva"); btn_new.clicked.connect(self.create_mask)
        btn_fill = QPushButton("Cargar en Máscara"); btn_fill.clicked.connect(self.fill_mask_from_db)
        btn_del = QPushButton("Borrar"); btn_del.clicked.connect(self.delete_mask)
        mask_btns.addWidget(btn_new); mask_btns.addWidget(btn_fill); mask_btns.addWidget(btn_del)
        p_lab_layout.addLayout(mask_btns)
        lab_layout.addWidget(panel_lab, 1)

    def create_scid_table(self, headers):
        table = QTableWidget(0, len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        table.verticalHeader().setVisible(False)
        table.verticalHeader().setDefaultSectionSize(22)
        table.horizontalHeader().setHighlightSections(False)
        table.setShowGrid(True)
        table.setStyleSheet("QHeaderView::section { font-weight: bold; }")
        return table

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
            self.board_ana.color_light = self.board_lab.color_light = light
            self.board_ana.color_dark = self.board_lab.color_dark = dark
            self.board_ana.update_board()
            self.board_lab.update_board()
            self.save_config()

    def resizeEvent(self, event):
        h = self.centralWidget().height() - 40
        self.board_ana.setFixedWidth(h); self.board_lab.setFixedWidth(h)
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

    def refresh_db_list(self):
        df = self.dbs.get(self.active_db_name)
        if df is None: return
        disp = df.head(1000)
        self.db_table.setRowCount(disp.height)
        for i, r in enumerate(disp.rows(named=True)):
            self.db_table.setItem(i, 0, QTableWidgetItem(r["date"])); self.db_table.setItem(i, 1, QTableWidgetItem(r["white"])); self.db_table.setItem(i, 2, QTableWidgetItem(str(r["w_elo"])))
            self.db_table.setItem(i, 3, QTableWidgetItem(r["black"])); self.db_table.setItem(i, 4, QTableWidgetItem(str(r["b_elo"]))); self.db_table.setItem(i, 5, QTableWidgetItem(r["result"])); self.db_table.item(i, 0).setData(Qt.UserRole, r["id"])
        self.db_table.resizeColumnsToContents()

    def load_game_from_list(self, item):
        game_id = self.db_table.item(item.row(), 0).data(Qt.UserRole)
        row = self.dbs[self.active_db_name].filter(pl.col("id") == game_id).row(0, named=True)
        self.board.reset(); self.full_mainline = []
        for uci in row["full_line"].split():
            if uci: self.full_mainline.append(chess.Move.from_uci(uci))
        self.current_idx = 0; self.tabs.setCurrentIndex(0); self.update_ui()

    def make_move(self, move):
        if self.current_idx < len(self.full_mainline): self.full_mainline = self.full_mainline[:self.current_idx]
        self.board.push(move); self.full_mainline.append(move); self.current_idx += 1; self.update_ui()

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
        self.board_ana.update_board(); self.board_lab.update_board(); self.update_stats()
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

    def create_mask(self):
        color = QColorDialog.getColor(Qt.blue, self, "Color")
        if color.isValid():
            name = f"Máscara {len(self.masks)+1}"
            self.masks[name] = {"positions": set(), "color": color}
            it = QListWidgetItem(name); it.setBackground(color); it.setForeground(Qt.white); self.mask_list.addItem(it)

    def fill_mask_from_db(self):
        curr = self.mask_list.currentItem()
        df = self.dbs.get(self.active_db_name)
        if curr and df is not None:
            self.progress.setVisible(True); self.mask_worker = MaskWorker(df)
            self.mask_worker.progress.connect(self.progress.setValue); self.mask_worker.finished.connect(lambda p: self.on_mask_filled(curr.text(), p)); self.mask_worker.start()

    def on_mask_filled(self, name, positions):
        self.masks[name]["positions"] = positions; self.progress.setVisible(False); self.update_stats()

    def delete_mask(self):
        curr = self.mask_list.currentItem()
        if curr:
            name = curr.text()
            if name in self.masks: del self.masks[name]
            self.mask_list.takeItem(self.mask_list.row(curr)); self.update_stats()

    def save_config(self):
        dbs_info = [m for m in self.db_metadata.values() if m.get("path")]
        colors = {"light": self.board_ana.color_light, "dark": self.board_ana.color_dark}
        config = {"dbs": dbs_info, "colors": colors}
        with open(CONFIG_FILE, "w") as f: json.dump(config, f)

    def load_config(self):
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r") as f:
                try:
                    data = json.load(f)
                    for info in data.get("dbs", []):
                        path = info["path"] if isinstance(info, dict) else info
                        if os.path.exists(path): self.load_parquet(path)
                    colors = data.get("colors")
                    if colors:
                        self.board_ana.color_light = self.board_lab.color_light = colors["light"]
                        self.board_ana.color_dark = self.board_lab.color_dark = colors["dark"]
                        self.board_ana.update_board(); self.board_lab.update_board()
                except: pass

    def update_stats(self):
        moves = self.board.move_stack
        line_uci = " ".join([m.uci() for m in moves])
        
        # --- Obtener Nombre Humano de Apertura (ECO) ---
        ap_nombre = self.eco.get_opening_name(line_uci)
        self.label_apertura.setText(f"<b>Apertura:</b> {ap_nombre}")
        self.label_apertura_lab.setText(f"<b>Apertura:</b> {ap_nombre}")

        df = self.dbs.get(self.active_db_name)
        if df is None:
            self.tree_ana.setRowCount(0)
            self.tree_lab.setRowCount(0)
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

            for table in [self.tree_ana, self.tree_lab]:
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

                    if table == self.tree_lab:
                        self.board.push(mv); epd = self.board.epd(); mask_str = ""
                        for m_n, m_i in self.masks.items():
                            if epd in m_i["positions"]: mask_str += "● "; it_move.setForeground(m_i["color"])
                        self.board.pop(); table.setItem(i, 6, QTableWidgetItem(mask_str))
                table.resizeColumnsToContents()
        except: pass