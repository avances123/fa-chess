from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                             QListWidget, QListWidgetItem, QToolBar)
from PySide6.QtCore import Qt, QSize, Signal
import qtawesome as qta
from src.ui.styles import STYLE_BADGE_NORMAL, STYLE_BADGE_SUCCESS, STYLE_BADGE_ERROR

class DBSidebar(QWidget):
    # Signals to communicate with MainWindow
    new_db_requested = Signal()
    open_db_requested = Signal()
    search_requested = Signal()
    invert_filter_requested = Signal()
    clear_filter_requested = Signal()
    db_switched = Signal(str)
    context_menu_requested = Signal(object, object) # pos, item

    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)
        
        # Stats Header
        stats_header = QHBoxLayout()
        stats_header.addWidget(QLabel("<b>Partidas:</b>"))
        self.label_stats = QLabel("[0/0]")
        self.label_stats.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.label_stats.setStyleSheet(STYLE_BADGE_NORMAL)
        stats_header.addWidget(self.label_stats)
        layout.addLayout(stats_header)

        # Toolbar
        self.toolbar = QToolBar()
        self.toolbar.setIconSize(QSize(16, 16))
        self.toolbar.setToolButtonStyle(Qt.ToolButtonIconOnly)
        self.toolbar.setStyleSheet("QToolBar { spacing: 5px; background: transparent; border: none; }")
        
        self._add_act('fa5s.plus-circle', '#2e7d32', "Nueva Base", self.new_db_requested.emit)
        self._add_act('fa5s.folder-open', '#1976d2', "Abrir Base Parquet", self.open_db_requested.emit)
        self.toolbar.addSeparator()
        self._add_act('fa5s.search', None, "Filtrar Partidas", self.search_requested.emit)
        self._add_act('fa5s.exchange-alt', None, "Invertir Filtro", self.invert_filter_requested.emit)
        self._add_act('fa5s.eraser', '#c62828', "Quitar Filtros", self.clear_filter_requested.emit)
        
        layout.addWidget(self.toolbar)

        # DB List
        layout.addWidget(QLabel("<b>Bases Abiertas</b>"))
        self.list_widget = QListWidget()
        self.list_widget.setIconSize(QSize(14, 14))
        self.list_widget.setContextMenuPolicy(Qt.CustomContextMenu)
        self.list_widget.currentRowChanged.connect(self._on_row_changed)
        self.list_widget.customContextMenuRequested.connect(
            lambda pos: self.context_menu_requested.emit(pos, self.list_widget.itemAt(pos))
        )
        layout.addWidget(self.list_widget)

    def _add_act(self, icon, color, tip, callback):
        act = self.toolbar.addAction(qta.icon(icon, color=color) if color else qta.icon(icon), "")
        act.setToolTip(tip); act.setStatusTip(tip); act.triggered.connect(callback)

    def add_db_item(self, name, is_clipbase=False):
        icon = qta.icon('fa5s.clipboard', color='#2e7d32') if is_clipbase else qta.icon('fa5s.database')
        item = QListWidgetItem(icon, name)
        self.list_widget.addItem(item)
        return item

    def update_stats(self, count, total, state="normal"):
        self.label_stats.setText(f"[{count}/{total}]")
        styles = {"success": STYLE_BADGE_SUCCESS, "error": STYLE_BADGE_ERROR, "normal": STYLE_BADGE_NORMAL}
        self.label_stats.setStyleSheet(styles.get(state, STYLE_BADGE_NORMAL))

    def _on_row_changed(self, row):
        if row >= 0: self.db_switched.emit(self.list_widget.item(row).text())
