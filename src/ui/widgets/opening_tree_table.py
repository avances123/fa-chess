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
        self.perf_threshold = 25 # Valor por defecto, se actualizará desde MainWindow
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Cabecera con Nombre de Apertura y Badge Global
        header_layout = QHBoxLayout()
        header_layout.setContentsMargins(5, 2, 5, 2)
        
        self.label_eco = QLabel("...")
        self.label_eco.setStyleSheet("font-size: 11px; color: #666; font-weight: bold;")
        self.label_eco.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        
        from src.ui.styles import STYLE_BADGE_NORMAL
        from src.ui.utils import ClickableBadge
        self.label_global_stats = ClickableBadge("0 / 0")
        self.label_global_stats.setMinimumWidth(80)
        self.label_global_stats.setAlignment(Qt.AlignCenter)
        self.label_global_stats.setStyleSheet(STYLE_BADGE_NORMAL + "border: 1px solid #ccc; padding-left: 8px; padding-right: 8px;")
        self.label_global_stats.setCursor(Qt.PointingHandCursor)
        self.label_global_stats.setToolTip("Haz clic para filtrar partidas")
        
        header_layout.addWidget(self.label_eco, 1)
        
        # Selector de Base de Referencia
        self.combo_ref = QComboBox()
        self.combo_ref.addItem("Base Activa")
        self.combo_ref.setToolTip("Elegir base de datos para estadísticas")
        self.combo_ref.setStyleSheet("QComboBox { font-size: 10px; padding: 2px; color: #555; }")
        self.combo_ref.currentTextChanged.connect(self.reference_changed.emit)
        header_layout.addWidget(self.combo_ref)

        header_layout.addWidget(self.label_global_stats)
        layout.addLayout(header_layout)

        # Stacked Widget para alternar entre tabla y carga
        self.stack = QStackedWidget()
        
        # Página 1: La Tabla
        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(["Movim.", "Frec.", "Barra", "Win %", "AvElo", "Perf"])
        
        # Añadir tooltips a las cabeceras
        self.table.horizontalHeaderItem(4).setToolTip("Elo medio de los jugadores que realizaron este movimiento")
        self.table.horizontalHeaderItem(5).setToolTip("Rendimiento real (Performance). Se ilumina en verde si es muy superior al AvElo y en rojo si es inferior.")
        
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.verticalHeader().setVisible(False)
        self.table.verticalHeader().setDefaultSectionSize(22)
        self.table.setStyleSheet(STYLE_TABLE_HEADER)
        
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.Stretch)
        for i in range(1, 6):
            header.setSectionResizeMode(i, QHeaderView.ResizeToContents)
        
        self.table.itemDoubleClicked.connect(self._on_double_click)
        self.table.itemSelectionChanged.connect(self._on_selection_changed)
        
        self.stack.addWidget(self.table)
        
        # Página 2: Indicador de Carga
        self.loading_container = QWidget()
        loading_layout = QVBoxLayout(self.loading_container)
        
        # Usamos un QPushButton transparente para que la animación funcione (QLabel no anima iconos de fuente directamente)
        from PySide6.QtWidgets import QPushButton as QPushBtn
        self.spinner_widget = QPushBtn()
        self.spinner_widget.setFlat(True)
        self.spinner_widget.setDisabled(True)
        self.spinner_widget.setStyleSheet("background: transparent; border: none;")
        self.spinner_widget.setIconSize(QSize(32, 32))
        
        # Aplicar el icono con animación de giro
        spinner_icon = qta.icon('fa5s.spinner', color='#999', animation=qta.Spin(self.spinner_widget))
        self.spinner_widget.setIcon(spinner_icon)
        
        loading_text = QLabel("Calculando estadísticas...<br><span style='color: #aaa; font-size: 10px;'>(Se guardarán en caché para acceso instantáneo)</span>")
        loading_text.setStyleSheet("color: #888; font-size: 11px;")
        loading_text.setAlignment(Qt.AlignCenter)
        loading_text.setWordWrap(True)
        
        loading_layout.addStretch()
        loading_layout.addWidget(self.spinner_widget, 0, Qt.AlignCenter)
        loading_layout.addWidget(loading_text)
        loading_layout.addStretch()
        
        self.stack.addWidget(self.loading_container)
        
        # Página 3: Datos insuficientes / Parada de cálculo
        self.insufficient_container = QWidget()
        insufficient_layout = QVBoxLayout(self.insufficient_container)
        self.icon_insufficient = QLabel()
        self.icon_insufficient.setPixmap(qta.icon('fa5s.hand-paper', color='#ccc').pixmap(QSize(32, 32)))
        self.icon_insufficient.setAlignment(Qt.AlignCenter)
        self.label_insufficient = QLabel("Cálculo detenido: Volumen muy bajo")
        self.label_insufficient.setStyleSheet("color: #888; font-style: italic;")
        self.label_insufficient.setAlignment(Qt.AlignCenter)
        insufficient_layout.addStretch()
        insufficient_layout.addWidget(self.icon_insufficient)
        insufficient_layout.addWidget(self.label_insufficient)
        insufficient_layout.addStretch()
        self.stack.addWidget(self.insufficient_container)
        
        layout.addWidget(self.stack)

    def set_loading(self, loading=True):
        """Muestra u oculta el estado de carga"""
        self.stack.setCurrentIndex(1 if loading else 0)

    def update_tree(self, stats_df, current_board, opening_name, is_filtered=False, total_view_count=0, next_move_uci=None):
        self.set_loading(False) # Asegurar que mostramos la tabla al recibir datos
        self.label_eco.setText(opening_name if opening_name else "Posición desconocida")
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        
        # Si no hay datos o es un DataFrame de aviso de parada
        if stats_df is None or stats_df.is_empty():
            self.stack.setCurrentIndex(2) # Mostrar página de datos insuficientes/parada
            self.label_insufficient.setText(f"Cálculo detenido<br>(Variante con muy pocas partidas)")
            return

        # Limpieza de seguridad para columnas de versiones anteriores
        if "uci" not in stats_df.columns:
            return

        self.stack.setCurrentIndex(0) # Mostrar tabla
        is_white_turn = current_board.turn == chess.WHITE
        self.table.setRowCount(stats_df.height)
        for i, r in enumerate(stats_df.rows(named=True)):
            # 1. Movimiento: Forzar siempre conversión de UCI a SAN para que sea legible
            try:
                move_text = uci_to_san(current_board, r["uci"])
            except:
                move_text = r["uci"] # Fallback a UCI si algo falla

            it_move = QTableWidgetItem(move_text)
            it_move.setData(Qt.UserRole, r["uci"]) # Guardamos UCI para la lógica interna
            it_move.setTextAlignment(Qt.AlignCenter)
            font = it_move.font()
            font.setBold(True)
            it_move.setFont(font)
            
            # RESALTAR SI ES LA JUGADA DE LA PARTIDA
            is_played = r["uci"] == next_move_uci
            if is_played:
                it_move.setBackground(QColor("#f6f669"))
            
            self.table.setItem(i, 0, it_move)

            # 2. Frecuencia
            it_count = SortableWidgetItem(f"{r['c']:,}".replace(",", "."))
            it_count.setData(Qt.UserRole, r["c"])
            it_count.setTextAlignment(Qt.AlignCenter)
            if is_played: it_count.setBackground(QColor("#f6f669"))
            self.table.setItem(i, 1, it_count)

            # 3. Barra de resultados (Widget)
            res_widget = ResultsWidget(r["w"], r["d"], r["b"], r["c"], is_white_turn)
            if is_played: res_widget.setStyleSheet("background-color: #f6f669;")
            self.table.setCellWidget(i, 2, res_widget)

            # 4. Win %
            win_rate = ((r["w"] + 0.5 * r["d"]) / r["c"] if is_white_turn else (r["b"] + 0.5 * r["d"]) / r["c"]) * 100
            it_win = SortableWidgetItem(f"{win_rate:.1f}%")
            it_win.setData(Qt.UserRole, win_rate)
            it_win.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            if is_played: it_win.setBackground(QColor("#f6f669"))
            self.table.setItem(i, 3, it_win)

            # 5. AvElo (Elo medio del bando que mueve)
            av_elo = int(r["avg_w_elo"] if is_white_turn else r["avg_b_elo"])
            it_elo = SortableWidgetItem(str(av_elo))
            it_elo.setData(Qt.UserRole, av_elo)
            it_elo.setTextAlignment(Qt.AlignCenter)
            if is_played: it_elo.setBackground(QColor("#f6f669"))
            self.table.setItem(i, 4, it_elo)

            # 6. Perf (Performance y lógica de colores)
            score = (r["w"] + 0.5 * r["d"]) / r["c"] if is_white_turn else (r["b"] + 0.5 * r["d"]) / r["c"]
            opponent_elo = r["avg_b_elo"] if is_white_turn else r["avg_w_elo"]
            perf = int(opponent_elo + (score - 0.5) * 800)
            
            it_perf = SortableWidgetItem(str(perf))
            it_perf.setData(Qt.UserRole, perf)
            it_perf.setTextAlignment(Qt.AlignCenter)
            if is_played: it_perf.setBackground(QColor("#f6f669"))
            
            # Lógica de colores comparando Perf con AvElo usando el umbral configurable
            if perf > av_elo + self.perf_threshold:
                it_perf.setForeground(QColor("#2e7d32")) # Verde
            elif perf < av_elo - self.perf_threshold:
                it_perf.setForeground(QColor("#c62828")) # Rojo
            else:
                it_perf.setForeground(QColor("#000000")) # Negro
            
            self.table.setItem(i, 5, it_perf)

        self.table.setSortingEnabled(True)
        self.table.sortByColumn(1, Qt.DescendingOrder) # Ordenar por frecuencia por defecto

    def _on_double_click(self, item):
        uci = self.table.item(item.row(), 0).data(Qt.UserRole)
        if uci: self.move_selected.emit(uci)

    def _on_selection_changed(self):
        items = self.table.selectedItems()
        if items:
            uci = self.table.item(items[0].row(), 0).data(Qt.UserRole)
            self.move_hovered.emit(uci)
        else:
            self.move_hovered.emit(None)

    def clear_selection(self):
        self.table.clearSelection()