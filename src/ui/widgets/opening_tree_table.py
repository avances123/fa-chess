from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                             QTableWidget, QTableWidgetItem, QHeaderView, QAbstractItemView, QStackedWidget, QComboBox)
from PySide6.QtCore import Qt, Signal, QSize
from PySide6.QtGui import QColor, QFont
import polars as pl
import chess
import qtawesome as qta
from src.ui.styles import STYLE_TABLE_HEADER
from src.ui.widgets.results_bar import ResultsWidget
from src.ui.utils import SortableWidgetItem
from src.core.utils import uci_to_san

class OpeningTreeTable(QWidget):
    move_selected = Signal(str) # uci
    move_hovered = Signal(str)  # uci or None
    reference_changed = Signal(str) # nombre de la base

    def __init__(self, parent=None):
        super().__init__(parent)
        self.perf_threshold = 25 
        self.venom_eval = 0.5
        self.venom_win = 52
        self.practical_win = 60
        self.total_view_count = 0
        self.branch_evals_cache = {} # Memoria persistente de evaluaciones para la posición actual
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        header_layout = QHBoxLayout()
        header_layout.setContentsMargins(5, 2, 5, 2)
        
        self.label_eco = QLabel("...")
        self.label_eco.setStyleSheet("font-size: 11px; color: #666; font-weight: bold;")
        self.label_eco.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        header_layout.addWidget(self.label_eco, 1)
        
        self.combo_ref = QComboBox()
        self.combo_ref.addItem("Base Activa")
        self.combo_ref.setToolTip("Elegir base de datos para estadísticas")
        self.combo_ref.setStyleSheet("QComboBox { font-size: 10px; padding: 2px; color: #555; }")
        self.combo_ref.currentTextChanged.connect(self.reference_changed.emit)
        header_layout.addWidget(self.combo_ref)

        from src.ui.styles import STYLE_BADGE_NORMAL
        from src.ui.utils import ClickableBadge
        self.label_global_stats = ClickableBadge("0 / 0")
        self.label_global_stats.setMinimumWidth(80)
        self.label_global_stats.setAlignment(Qt.AlignCenter)
        self.label_global_stats.setStyleSheet(STYLE_BADGE_NORMAL + "border: 1px solid #ccc; padding-left: 8px; padding-right: 8px;")
        self.label_global_stats.setCursor(Qt.PointingHandCursor)
        self.label_global_stats.setToolTip("Haz clic para filtrar partidas")
        header_layout.addWidget(self.label_global_stats)

        layout.addLayout(header_layout)

        self.stack = QStackedWidget()
        self.table = QTableWidget(0, 8)
        self.table.setHorizontalHeaderLabels(["🧪", "Movim.", "Eval", "Frec.", "Barra", "Win %", "AvElo", "Perf"])
        
        self.table.horizontalHeaderItem(0).setToolTip("Buscador de Veneno: Jugadas con alta recompensa práctica a pesar de la evaluación.")
        self.table.horizontalHeaderItem(2).setToolTip("Evaluación del motor para este movimiento")
        
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.verticalHeader().setVisible(False)
        self.table.verticalHeader().setDefaultSectionSize(22)
        self.table.setStyleSheet(STYLE_TABLE_HEADER)
        
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        for i in range(3, 8):
            header.setSectionResizeMode(i, QHeaderView.ResizeToContents)
        
        self.table.itemDoubleClicked.connect(self._on_double_click)
        self.table.itemSelectionChanged.connect(self._on_selection_changed)
        self.stack.addWidget(self.table)
        
        self.loading_container = QWidget()
        loading_layout = QVBoxLayout(self.loading_container)
        from PySide6.QtWidgets import QPushButton as QPushBtn
        self.spinner_widget = QPushBtn()
        self.spinner_widget.setFlat(True); self.spinner_widget.setDisabled(True)
        self.spinner_widget.setStyleSheet("background: transparent; border: none;"); self.spinner_widget.setIconSize(QSize(32, 32))
        spinner_icon = qta.icon('fa5s.spinner', color='#999', animation=qta.Spin(self.spinner_widget))
        self.spinner_widget.setIcon(spinner_icon)
        loading_layout.addStretch(); loading_layout.addWidget(self.spinner_widget, 0, Qt.AlignCenter); loading_layout.addStretch()
        self.stack.addWidget(self.loading_container)
        
        self.insufficient_container = QWidget()
        insufficient_layout = QVBoxLayout(self.insufficient_container)
        self.icon_insufficient = QLabel(); self.icon_insufficient.setPixmap(qta.icon('fa5s.hand-paper', color='#ccc').pixmap(QSize(32, 32))); self.icon_insufficient.setAlignment(Qt.AlignCenter)
        self.label_insufficient = QLabel("Cálculo detenido")
        self.label_insufficient.setStyleSheet("color: #888; font-style: italic;"); self.label_insufficient.setAlignment(Qt.AlignCenter)
        insufficient_layout.addStretch(); insufficient_layout.addWidget(self.icon_insufficient); insufficient_layout.addWidget(self.label_insufficient); insufficient_layout.addStretch()
        self.stack.addWidget(self.insufficient_container)
        
        layout.addWidget(self.stack)

    def set_loading(self, loading=True):
        self.stack.setCurrentIndex(1 if loading else 0)

    def update_tree(self, stats_df, current_board, opening_name, is_filtered=False, total_view_count=0, next_move_uci=None, engine_eval=None):
        self.set_loading(False)
        self.total_view_count = total_view_count
        self.branch_evals_cache = {} # RESETEAR MEMORIA AL CAMBIAR DE POSICIÓN
        self.label_eco.setText(f"{opening_name} ({engine_eval})" if engine_eval else opening_name)
        self.table.setSortingEnabled(False); self.table.setRowCount(0)
        
        if stats_df is None or stats_df.is_empty():
            self.stack.setCurrentIndex(2); return

        self.stack.setCurrentIndex(0)
        is_white_turn = current_board.turn == chess.WHITE
        self.table.setRowCount(stats_df.height)
        
        for i, r in enumerate(stats_df.rows(named=True)):
            win_rate = ((r["w"] + 0.5 * r["d"]) / r["c"] if is_white_turn else (r["b"] + 0.5 * r["d"]) / r["c"]) * 100
            is_played = r["uci"] == next_move_uci
            
            it_venom = QTableWidgetItem(""); it_venom.setTextAlignment(Qt.AlignCenter)
            if is_played: it_venom.setBackground(QColor("#f6f669"))
            self.table.setItem(i, 0, it_venom)

            try: move_text = uci_to_san(current_board, r["uci"])
            except: move_text = r["uci"]
            it_move = QTableWidgetItem(move_text); it_move.setData(Qt.UserRole, r["uci"]); it_move.setTextAlignment(Qt.AlignCenter)
            font = it_move.font(); font.setBold(True); it_move.setFont(font)
            if is_played: it_move.setBackground(QColor("#f6f669"))
            self.table.setItem(i, 1, it_move)

            it_eval = QTableWidgetItem("-"); it_eval.setTextAlignment(Qt.AlignCenter)
            if is_played: it_eval.setBackground(QColor("#f6f669"))
            self.table.setItem(i, 2, it_eval)

            it_count = SortableWidgetItem(f"{r['c']:,}".replace(",", ".")); it_count.setData(Qt.UserRole, r["c"]); it_count.setTextAlignment(Qt.AlignCenter)
            if is_played: it_count.setBackground(QColor("#f6f669"))
            self.table.setItem(i, 3, it_count)

            res_w = ResultsWidget(r["w"], r["d"], r["b"], r["c"], is_white_turn)
            if is_played: res_w.setStyleSheet("background-color: #f6f669;")
            self.table.setCellWidget(i, 4, res_w)

            it_win = SortableWidgetItem(f"{win_rate:.1f}%"); it_win.setData(Qt.UserRole, win_rate); it_win.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            if is_played: it_win.setBackground(QColor("#f6f669"))
            self.table.setItem(i, 5, it_win)

            av_elo = int(r["avg_w_elo"] if is_white_turn else r["avg_b_elo"])
            it_elo = SortableWidgetItem(str(av_elo)); it_elo.setData(Qt.UserRole, av_elo); it_elo.setTextAlignment(Qt.AlignCenter)
            if is_played: it_elo.setBackground(QColor("#f6f669"))
            self.table.setItem(i, 6, it_elo)

            opp_elo = r["avg_b_elo"] if is_white_turn else r["avg_w_elo"]
            perf = int(opp_elo + ((r["w"] + 0.5 * r["d"]) / r["c"] - 0.5 if is_white_turn else (r["b"] + 0.5 * r["d"]) / r["c"] - 0.5) * 800)
            it_perf = SortableWidgetItem(str(perf)); it_perf.setData(Qt.UserRole, perf); it_perf.setTextAlignment(Qt.AlignCenter)
            if is_played: it_perf.setBackground(QColor("#f6f669"))
            if perf > av_elo + self.perf_threshold: it_perf.setForeground(QColor("#2e7d32"))
            elif perf < av_elo - self.perf_threshold: it_perf.setForeground(QColor("#c62828"))
            self.table.setItem(i, 7, it_perf)

        self.table.setSortingEnabled(True); self.table.sortByColumn(3, Qt.DescendingOrder)

    def update_branch_evals(self, multi_evals, is_white_turn):
        """Actualiza dinámicamente usando una memoria persistente de ramas"""
        if not multi_evals or self.stack.currentIndex() != 0: return
        
        # 1. Normalizar y actualizar memoria interna (convertir todo a numérico para cálculo relativo)
        for uci, val in multi_evals.items():
            if isinstance(val, str):
                try:
                    if "M" in val: num_val = 10.0 if "M" in val and not val.startswith("-") else -10.0
                    else: num_val = float(val)
                    self.branch_evals_cache[uci] = num_val
                except: continue
            else:
                self.branch_evals_cache[uci] = val
        
        # 2. Encontrar la mejor evaluación de entre todas las conocidas
        if not self.branch_evals_cache: return
        scores = list(self.branch_evals_cache.values())
        best_score = max(scores) if is_white_turn else min(scores)
        
        min_v_games = max(20, int(self.total_view_count * 0.00001))
        
        for row in range(self.table.rowCount()):
            it_move = self.table.item(row, 1)
            if not it_move: continue
            uci = it_move.data(Qt.UserRole)
            
            if uci in self.branch_evals_cache:
                score = self.branch_evals_cache[uci]
                
                # Actualizar celda de Eval (usar el string original si existe en multi_evals)
                it_eval = self.table.item(row, 2)
                if it_eval:
                    display_text = multi_evals.get(uci) if isinstance(multi_evals.get(uci), str) else f"{score:+.2f}"
                    it_eval.setText(display_text)
                    
                    # Color de eval (verde bueno, rojo malo)
                    if (is_white_turn and score > 0.3) or (not is_white_turn and score < -0.3): it_eval.setForeground(QColor("#2e7d32"))
                    elif (is_white_turn and score < -0.3) or (not is_white_turn and score > 0.3): it_eval.setForeground(QColor("#c62828"))
                    else: it_eval.setForeground(QColor("#000000"))
                
                # RECALCULAR VENENO RELATIVO
                it_win = self.table.item(row, 5); it_count = self.table.item(row, 3)
                if it_win and it_count:
                    win_rate = it_win.data(Qt.UserRole); count = it_count.data(Qt.UserRole)
                    icon = ""; tip = ""
                    if count >= min_v_games:
                        eval_loss = abs(best_score - score)
                        if eval_loss >= self.venom_eval and win_rate > self.venom_win:
                            icon = "🧪"; tip = f"Veneno: Pierdes {eval_loss:.2f} de eval pero ganas el {win_rate:.1f}%"
                        elif eval_loss < 0.2 and win_rate > self.practical_win:
                            icon = "🧪"; tip = f"Oro Práctico: Jugada sólida con éxito del {win_rate:.1f}%"
                    
                    it_venom = self.table.item(row, 0)
                    if it_venom: it_venom.setText(icon); it_venom.setToolTip(tip)

    def _on_double_click(self, item):
        uci = self.table.item(item.row(), 1).data(Qt.UserRole)
        if uci: self.move_selected.emit(uci)

    def _on_selection_changed(self):
        items = self.table.selectedItems()
        if items:
            it_move = self.table.item(items[0].row(), 1)
            if it_move: self.move_hovered.emit(it_move.data(Qt.UserRole))
        else: self.move_hovered.emit(None)

    def clear_selection(self): self.table.clearSelection()
