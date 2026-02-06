import sys
import os
import json
import chess
import chess.pgn
import chess.svg
import polars as pl
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QGraphicsView, QGraphicsScene, QTableWidget, 
                             QTableWidgetItem, QLabel, QPushButton, QFileDialog, 
                             QProgressBar, QHeaderView, QTextBrowser, QStatusBar, QTabWidget, 
                             QLineEdit, QListWidget, QMenu, QCheckBox, QColorDialog, QListWidgetItem)
from PySide6.QtSvgWidgets import QGraphicsSvgItem
from PySide6.QtSvg import QSvgRenderer
from PySide6.QtCore import Qt, QPointF, QThread, Signal
from PySide6.QtGui import QPainter, QBrush, QColor, QAction

os.environ["QT_QPA_PLATFORM"] = "wayland;xcb"
CONFIG_FILE = os.path.expanduser("~/.config/fa-chess.json")

LIGHT_STYLE = """
QMainWindow { background-color: #f5f5f5; }
QWidget { color: #222; font-family: 'Inter', sans-serif; font-size: 13px; }
QTableWidget, QListWidget { background-color: #ffffff; border: 1px solid #ddd; color: #000; }
QHeaderView::section { background-color: #f0f0f0; color: #333; padding: 6px; border: 1px solid #ddd; }
QPushButton { background-color: #e0e0e0; border: 1px solid #ccc; padding: 6px; border-radius: 3px; font-weight: bold;}
QLineEdit { background-color: #fff; border: 1px solid #ddd; padding: 4px; }
QTabWidget::pane { border: 1px solid #ccc; }
QTextBrowser { background-color: #fff; border: 1px solid #ddd; color: #000; font-size: 16px; }
"""

class PGNWorker(QThread):
    progress = Signal(int); finished = Signal(str); status = Signal(str)
    def __init__(self, path): super().__init__(); self.path = path
    def run(self):
        games = []
        try:
            with open(self.path, encoding="utf-8", errors="ignore") as pgn:
                for i in range(1000000):
                    game = chess.pgn.read_game(pgn)
                    if not game: break
                    def safe_int(val):
                        clean = "".join(filter(str.isdigit, str(val)))
                        return int(clean) if clean else 0
                    full_line = [node.move.uci() for node in game.mainline()]
                    games.append({"id": i, "white": game.headers.get("White", "?"), "black": game.headers.get("Black", "?"), "w_elo": safe_int(game.headers.get("WhiteElo")), "b_elo": safe_int(game.headers.get("BlackElo")), "result": game.headers.get("Result", "*"), "date": game.headers.get("Date", "????.??.??"), "event": game.headers.get("Event", "?"), "line": " ".join(full_line[:12]), "full_line": " ".join(full_line)})
                    if i % 1000 == 0: self.progress.emit(i); self.status.emit(f"Cargando {i}...")
            out = self.path.replace(".pgn", ".parquet")
            pl.DataFrame(games).write_parquet(out); self.finished.emit(out)
        except Exception as e: self.status.emit(f"Error: {e}")

class MaskWorker(QThread):
    progress = Signal(int); finished = Signal(set)
    def __init__(self, df): super().__init__(); self.df = df
    def run(self):
        positions = set(); total = len(self.df)
        for idx, row in enumerate(self.df.iter_rows(named=True)):
            line = row["full_line"].split(); board = chess.Board(); positions.add(board.epd())
            for uci in line:
                try: board.push_uci(uci); positions.add(board.epd())
                except: break
            if idx % 500 == 0: self.progress.emit(int((idx/total)*100))
        self.finished.emit(positions)

class ChessBoard(QGraphicsView):
    def __init__(self, board, parent_main):
        super().__init__()
        self.board = board; self.parent_main = parent_main
        self.scene = QGraphicsScene(self); self.setScene(self.scene)
        self.setRenderHint(QPainter.Antialiasing)
        self.setViewportUpdateMode(QGraphicsView.FullViewportUpdate)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.renderers = {}; self.piece_items = []
        self.square_size = 100; self.color_light = "#eeeed2"; self.color_dark = "#8ca2ad"

    def resizeEvent(self, event):
        side = min(self.width(), self.height())
        self.square_size = side / 8
        self.scene.setSceneRect(0, 0, side, side)
        self.update_board()
        super().resizeEvent(event)

    def wheelEvent(self, event):
        if event.angleDelta().y() > 0: self.parent_main.step_back()
        else: self.parent_main.step_forward()

    def get_square(self, pos):
        c, r = int(pos.x() // self.square_size), 7 - int(pos.y() // self.square_size)
        return chess.square(c, r) if 0 <= c < 8 and 0 <= r < 8 else None

    def mousePressEvent(self, event):
        pos = self.mapToScene(event.position().toPoint()); sq = self.get_square(pos)
        if sq is not None:
            piece = self.board.piece_at(sq)
            if piece and piece.color == self.board.turn:
                self.selected_square = sq
                for item in self.piece_items:
                    r, c = chess.square_rank(sq), chess.square_file(sq)
                    target = QPointF(c * self.square_size, (7 - r) * self.square_size)
                    if (item.pos() - target).manhattanLength() < 5:
                        item.setZValue(100); self.drag_item = item; break
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event):
        if self.drag_item:
            pos = self.mapToScene(event.position().toPoint())
            self.drag_item.setPos(pos - QPointF(self.square_size/2, self.square_size/2))
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event):
        if self.selected_square is not None:
            pos = self.mapToScene(event.position().toPoint()); target_sq = self.get_square(pos)
            if target_sq is not None:
                move = chess.Move(self.selected_square, target_sq)
                if move in self.board.legal_moves: self.parent_main.make_move(move)
            self.selected_square = None; self.drag_item = None; self.update_board()
        super().mouseReleaseEvent(event)

    def update_board(self):
        self.scene.clear(); self.piece_items.clear()
        for r in range(8):
            for c in range(8):
                color = self.color_light if (r + c) % 2 == 0 else self.color_dark
                self.scene.addRect(c * self.square_size, (7 - r) * self.square_size, self.square_size, self.square_size, Qt.NoPen, QBrush(QColor(color)))
        for square, piece in self.board.piece_map().items():
            r, c = chess.square_rank(square), chess.square_file(square)
            key = f"{piece.symbol()}_{int(self.square_size)}"
            if key not in self.renderers:
                svg = chess.svg.piece(piece, size=int(self.square_size)).encode("utf-8")
                self.renderers[key] = QSvgRenderer(svg)
            item = QGraphicsSvgItem()
            item.setSharedRenderer(self.renderers[key])
            item.setPos(c * self.square_size, (7 - r) * self.square_size)
            self.scene.addItem(item); self.piece_items.append(item)

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("fa-chess"); self.setStyleSheet(LIGHT_STYLE)
        self.board = chess.Board(); self.full_mainline = []; self.current_idx = 0
        self.dbs = {}; self.db_metadata = {}; self.active_db_name = "Clipbase"
        self.masks = {}; self.init_clipbase()
        
        self.tabs = QTabWidget(); self.setCentralWidget(self.tabs)

        # TAB 1: ANALISIS
        self.tab_analysis = QWidget(); self.tabs.addTab(self.tab_analysis, "Tablero")
        ana_layout = QHBoxLayout(self.tab_analysis); ana_layout.setContentsMargins(0,0,0,0)
        self.board_ana = ChessBoard(self.board, self)
        ana_layout.addWidget(self.board_ana)
        
        panel_ana = QWidget(); p_ana_layout = QVBoxLayout(panel_ana)
        
        # Configuración Visual Restaurada
        p_ana_layout.addWidget(QLabel("<b>Colores Tablero</b>"))
        color_layout = QHBoxLayout()
        btn_l = QPushButton("Claras"); btn_l.clicked.connect(self.pick_light_color)
        btn_d = QPushButton("Oscuras"); btn_d.clicked.connect(self.pick_dark_color)
        color_layout.addWidget(btn_l); color_layout.addWidget(btn_d)
        p_ana_layout.addLayout(color_layout)

        p_ana_layout.addWidget(QLabel("<b>Explorador Lichess</b>"))
        self.tree_ana = QTableWidget(0, 4); self.tree_ana.setHorizontalHeaderLabels(["Mov.", "Cant.", "B%", "N%"])
        self.tree_ana.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.tree_ana.itemClicked.connect(self.on_tree_click); p_ana_layout.addWidget(self.tree_ana)
        
        nav_box = QHBoxLayout()
        for l, f in [("|<", self.go_start), ("<", self.step_back), (">", self.step_forward), (">|", self.go_end)]:
            btn = QPushButton(l); btn.clicked.connect(f); nav_box.addWidget(btn)
        p_layout = p_ana_layout # Alias para compatibilidad
        p_layout.addLayout(nav_box)
        self.hist_ana = QTextBrowser(); self.hist_ana.setOpenLinks(False); self.hist_ana.anchorClicked.connect(self.jump_to_move)
        p_layout.addWidget(self.hist_ana); ana_layout.addWidget(panel_ana, 1)

        # TAB 2: LABORATORIO
        self.tab_lab = QWidget(); self.tabs.addTab(self.tab_lab, "Laboratorio")
        lab_layout = QHBoxLayout(self.tab_lab); lab_layout.setContentsMargins(0,0,0,0)
        self.board_lab = ChessBoard(self.board, self)
        lab_layout.addWidget(self.board_lab)
        panel_lab = QWidget(); p_lab_layout = QVBoxLayout(panel_lab)
        self.tree_lab = QTableWidget(0, 5); self.tree_lab.setHorizontalHeaderLabels(["Mov.", "Cant.", "B%", "N%", "Masc."])
        self.tree_lab.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.tree_lab.itemClicked.connect(self.on_tree_click); p_lab_layout.addWidget(self.tree_lab)
        self.mask_list = QListWidget(); p_lab_layout.addWidget(self.mask_list)
        mask_btns = QHBoxLayout()
        btn_new = QPushButton("Nueva"); btn_new.clicked.connect(self.create_mask)
        btn_fill = QPushButton("Llenar"); btn_fill.clicked.connect(self.fill_mask_from_db)
        mask_btns.addWidget(btn_new); mask_btns.addWidget(btn_fill)
        p_lab_layout.addLayout(mask_btns); lab_layout.addWidget(panel_lab, 1)

        # TAB 3: GESTOR
        self.tab_db = QWidget(); self.tabs.addTab(self.tab_db, "Gestor Bases")
        db_layout = QHBoxLayout(self.tab_db)
        db_sidebar = QVBoxLayout(); self.db_list_widget = QListWidget(); self.db_list_widget.addItem("Clipbase")
        self.db_list_widget.currentRowChanged.connect(self.switch_db); btn_add = QPushButton("Añadir Base...")
        btn_add.clicked.connect(self.open_file); self.progress = QProgressBar(); self.progress.setVisible(False)
        db_sidebar.addWidget(self.db_list_widget); db_sidebar.addWidget(btn_add); db_sidebar.addWidget(self.progress); db_layout.addLayout(db_sidebar, 1)
        db_content = QVBoxLayout(); self.db_table = QTableWidget(0, 6); self.db_table.setHorizontalHeaderLabels(["Fecha", "Blancas", "Elo B", "Negras", "Elo N", "Res"])
        self.db_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch); self.db_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.db_table.itemDoubleClicked.connect(self.load_game_from_list); db_content.addWidget(self.db_table); db_layout.addLayout(db_content, 4)

        self.setStatusBar(QStatusBar()); self.load_config()

    def resizeEvent(self, event):
        h = self.centralWidget().height() - 40
        self.board_ana.setFixedWidth(h); self.board_lab.setFixedWidth(h)
        super().resizeEvent(event)

    def pick_light_color(self):
        c = QColorDialog.getColor(QColor(self.board_ana.color_light), self, "Claras")
        if c.isValid():
            self.board_ana.color_light = self.board_lab.color_light = c.name()
            self.board_ana.update_board(); self.board_lab.update_board(); self.save_config()

    def pick_dark_color(self):
        c = QColorDialog.getColor(QColor(self.board_ana.color_dark), self, "Oscuras")
        if c.isValid():
            self.board_ana.color_dark = self.board_lab.color_dark = c.name()
            self.board_ana.update_board(); self.board_lab.update_board(); self.save_config()

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
                        if os.path.exists(info["path"]): self.load_parquet(info["path"])
                    colors = data.get("colors")
                    if colors:
                        self.board_ana.color_light = self.board_lab.color_light = colors.get("light", "#eeeed2")
                        self.board_ana.color_dark = self.board_lab.color_dark = colors.get("dark", "#8ca2ad")
                        self.board_ana.update_board(); self.board_lab.update_board()
                except: pass

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
            self.mask_worker.progress.connect(self.progress.setValue)
            self.mask_worker.finished.connect(lambda p: self.on_mask_filled(curr.text(), p)); self.mask_worker.start()

    def on_mask_filled(self, name, positions):
        self.masks[name]["positions"] = positions; self.progress.setVisible(False); self.update_stats()

    def make_move(self, move):
        if self.current_idx < len(self.full_mainline): self.full_mainline = self.full_mainline[:self.current_idx]
        self.board.push(move); self.full_mainline.append(move); self.current_idx += 1; self.update_ui()

    def step_back(self):
        if self.current_idx > 0: self.current_idx -= 1; self.board.pop(); self.update_ui()

    def step_forward(self):
        if self.current_idx < len(self.full_mainline): self.board.push(self.full_mainline[self.current_idx]); self.current_idx += 1; self.update_ui()

    def jump_to_move(self, url):
        idx = int(url.toString())
        while self.current_idx > idx: self.step_back()
        while self.current_idx < idx: self.step_forward()

    def go_start(self):
        while self.current_idx > 0: self.step_back()

    def go_end(self):
        while self.current_idx < len(self.full_mainline): self.step_forward()

    def update_ui(self):
        self.board_ana.update_board(); self.board_lab.update_board(); self.update_stats()
        temp = chess.Board(); html = "<style>a { text-decoration: none; color: #222; } .active { background-color: #f6f669; }</style>"
        for i, m in enumerate(self.full_mainline[:len(self.full_mainline)]):
            san = temp.san(m); num = (i//2)+1
            if i % 2 == 0: html += f"<b>{num}.</b> "
            st = "class='active'" if i == self.current_idx - 1 else ""
            html += f"<a {st} href='{i+1}'>{san}</a> "; temp.push(m)
        self.hist_ana.setHtml(html)

    def on_tree_click(self, item):
        uci = item.data(Qt.UserRole); self.make_move(chess.Move.from_uci(uci))

    def init_clipbase(self):
        self.dbs["Clipbase"] = pl.DataFrame(schema={"id": pl.Int64, "white": pl.String, "black": pl.String, "w_elo": pl.Int64, "b_elo": pl.Int64, "result": pl.String, "date": pl.String, "event": pl.String, "line": pl.String, "full_line": pl.String})
        self.db_metadata["Clipbase"] = {"read_only": False, "path": None}

    def switch_db(self, row):
        if row < 0: return
        self.active_db_name = self.db_list_widget.item(row).text(); self.refresh_db_list(); self.update_stats()

    def open_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Abrir", "/data/chess", "Chess (*.pgn *.parquet)")
        if path:
            p_path = path.replace(".pgn", ".parquet")
            if os.path.exists(p_path): self.load_parquet(p_path)
            else:
                self.progress.setVisible(True); self.worker = PGNWorker(path)
                self.worker.progress.connect(self.progress.setValue); self.worker.status.connect(self.statusBar().showMessage); self.worker.finished.connect(self.load_parquet); self.worker.start()

    def load_parquet(self, path):
        self.progress.setVisible(False); name = os.path.basename(path)
        self.dbs[name] = pl.read_parquet(path); self.db_metadata[name] = {"read_only": False, "path": path}
        if not self.db_list_widget.findItems(name, Qt.MatchExactly): self.db_list_widget.addItem(name)
        self.db_list_widget.setCurrentRow(self.db_list_widget.count()-1); self.save_config(); self.refresh_db_list()

    def refresh_db_list(self):
        df = self.dbs.get(self.active_db_name)
        if df is None: return
        disp = df.head(1000)
        self.db_table.setRowCount(disp.height)
        for i, r in enumerate(disp.rows(named=True)):
            self.db_table.setItem(i, 0, QTableWidgetItem(r["date"]))
            self.db_table.setItem(i, 1, QTableWidgetItem(r["white"])); self.db_table.setItem(i, 2, QTableWidgetItem(str(r["w_elo"])))
            self.db_table.setItem(i, 3, QTableWidgetItem(r["black"])); self.db_table.setItem(i, 4, QTableWidgetItem(str(r["b_elo"])))
            self.db_table.setItem(i, 5, QTableWidgetItem(r["result"])); self.db_table.item(i, 0).setData(Qt.UserRole, r["id"])

    def load_game_from_list(self, item):
        game_id = self.db_table.item(item.row(), 0).data(Qt.UserRole)
        row = self.dbs[self.active_db_name].filter(pl.col("id") == game_id).row(0, named=True)
        self.board.reset(); self.full_mainline = []
        for uci in row["full_line"].split(): self.full_mainline.append(chess.Move.from_uci(uci))
        self.current_idx = 0; self.tabs.setCurrentIndex(0); self.update_ui()

    def update_stats(self):
        df = self.dbs.get(self.active_db_name)
        if df is None: return
        line = " ".join([m.uci() for m in self.board.move_stack[:self.current_idx]])
        try:
            res = df.lazy().filter(pl.col("line").str.starts_with(line)).select([
                pl.col("line").str.slice(len(line)).str.strip_chars().str.split(" ").list.get(0).alias("uci"),
                pl.col("result")
            ]).filter(pl.col("uci").is_not_null() & (pl.col("uci") != "")).group_by("uci").agg([
                pl.count().alias("c"),
                pl.col("result").filter(pl.col("result") == "1-0").count().alias("w"),
                pl.col("result").filter(pl.col("result") == "0-1").count().alias("b")
            ]).sort("c", descending=True).limit(15).collect()
            for table in [self.tree_ana, self.tree_lab]:
                table.setRowCount(res.height)
                for i, r in enumerate(res.rows(named=True)):
                    mv = chess.Move.from_uci(r["uci"]); san = self.board.san(mv)
                    it = QTableWidgetItem(san); it.setData(Qt.UserRole, r["uci"])
                    if table == self.tree_lab:
                        self.board.push(mv); epd = self.board.epd(); mask_str = ""
                        for m_n, m_i in self.masks.items():
                            if epd in m_i["positions"]: mask_str += "● "; it.setForeground(m_i["color"])
                        self.board.pop(); table.setItem(i, 4, QTableWidgetItem(mask_str))
                    table.setItem(i, 0, it); table.setItem(i, 1, QTableWidgetItem(str(r["c"])))
                    table.setItem(i, 2, QTableWidgetItem(f"{(r['w']/r['c'])*100:.1f}%"))
                    table.setItem(i, 3, QTableWidgetItem(f"{(r['b']/r['c'])*100:.1f}%"))
        except: pass

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow(); window.resize(1200, 800); window.show(); sys.exit(app.exec())