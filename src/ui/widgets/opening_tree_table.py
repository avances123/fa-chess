from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                             QTableWidget, QTableWidgetItem, QHeaderView, QAbstractItemView)
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QColor
import polars as pl
import chess
from src.ui.styles import STYLE_TABLE_HEADER

class OpeningTreeTable(QWidget):
    move_selected = Signal(str) # uci
    move_hovered = Signal(str)  # uci or None

    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # ECO Label
        self.label_eco = QLabel("...")
        self.label_eco.setStyleSheet("font-size: 11px; color: #666; font-weight: bold; padding: 2px;")
        self.label_eco.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.label_eco)

        # Table
        self.table = QTableWidget(0, 5)
        self.table.setHorizontalHeaderLabels(["Jugada", "Partidas", "1-0", "1/2", "0-1"])
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.verticalHeader().setVisible(False)
        self.table.verticalHeader().setDefaultSectionSize(20)
        self.table.setStyleSheet(STYLE_TABLE_HEADER)
        
        # Header behavior
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.Stretch)
        for i in range(1, 5):
            header.setSectionResizeMode(i, QHeaderView.ResizeToContents)
        
        self.table.itemDoubleClicked.connect(self._on_double_click)
        self.table.itemSelectionChanged.connect(self._on_selection_changed)
        
        layout.addWidget(self.table)

    def update_tree(self, stats_df, is_white_turn, opening_name, is_filtered=False, total_view_count=0):
        """Actualiza la tabla con los resultados del StatsWorker."""
        self.label_eco.setText(opening_name if opening_name else "Posición desconocida")
        self.table.setRowCount(0)
        
        if stats_df is None or stats_df.height == 0:
            return

        self.table.setRowCount(stats_df.height)
        for i, row in enumerate(stats_df.rows(named=True)):
            # Convertir UCI a SAN para mostrar (más legible)
            try:
                # Necesitamos un board temporal para SAN, pero el worker ya nos da UCI
                # Por ahora mostramos UCI para ser rápidos, o simplificamos
                move_str = row["uci"] 
            except:
                move_str = row["uci"]

            # Porcentajes
            c = row["c"]
            pct_w = (row["w"] / c * 100)
            pct_d = (row["d"] / c * 100)
            pct_b = (row["b"] / c * 100)

            self._set_item(i, 0, move_str, Qt.AlignCenter, bold=True)
            self._set_item(i, 1, f"{c}", Qt.AlignCenter)
            self._set_item(i, 2, f"{pct_w:.1f}%", Qt.AlignCenter, color="#2e7d32")
            self._set_item(i, 3, f"{pct_d:.1f}%", Qt.AlignCenter, color="#666")
            self._set_item(i, 4, f"{pct_b:.1f}%", Qt.AlignCenter, color="#c62828")
            
            # Guardar UCI en el primer item para recuperarlo al hacer click
            self.table.item(i, 0).setData(Qt.UserRole, row["uci"])

    def _set_item(self, row, col, text, align, bold=False, color=None):
        item = QTableWidgetItem(text)
        item.setTextAlignment(align)
        if bold:
            font = item.font()
            font.setBold(True)
            item.setFont(font)
        if color:
            item.setForeground(QColor(color))
        self.table.setItem(row, col, item)

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
