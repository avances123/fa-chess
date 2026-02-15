from PySide6.QtWidgets import QWidget, QVBoxLayout, QLabel
from PySide6.QtCore import Qt
from src.ui.styles import STYLE_GAME_HEADER

class GameInfoHeader(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        self.label = QLabel("<i>No hay partida cargada</i>")
        self.label.setStyleSheet(STYLE_GAME_HEADER)
        self.label.setAlignment(Qt.AlignCenter)
        self.label.setWordWrap(True)
        self.label.setOpenExternalLinks(True)
        
        layout.addWidget(self.label)

    def update_info(self, game_data):
        if not game_data:
            self.clear_info()
            return

        site = game_data.get('site', '')
        site_html = f" • <a href='{site}' style='color: #1976d2; text-decoration: none;'>{site}</a>" if site.startswith("http") else f" • {site}" if site else ""

        header_html = f"""
            <div style='font-size: 14px;'>
                ⚪ <b>{game_data['white']}</b> ({game_data['w_elo']}) vs ⚫ <b>{game_data['black']}</b> ({game_data['b_elo']})
                <span style='color: #666; margin-left: 10px;'>[{game_data['result']}]</span>
            </div>
            <div style='font-size: 11px; color: #555; margin-top: 4px;'>
                {game_data['event']}{site_html} • {game_data['date']}
            </div>
        """
        self.label.setText(header_html)

    def update_material(self, board):
        """Método dummy para evitar errores tras la restauración"""
        pass

    def clear_info(self):
        self.label.setText("<i>Nueva partida (sin datos)</i>")
