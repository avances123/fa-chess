from PySide6.QtWidgets import QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QColorDialog
from PySide6.QtGui import QColor

class SettingsDialog(QDialog):
    def __init__(self, current_light, current_dark, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Configuración del Tablero")
        
        self.light_color = current_light
        self.dark_color = current_dark
        
        layout = QVBoxLayout(self)
        
        # Casillas Claras
        light_layout = QHBoxLayout()
        light_layout.addWidget(QLabel("Casillas Claras:"))
        self.btn_light = QPushButton()
        self.update_button_color(self.btn_light, self.light_color)
        self.btn_light.clicked.connect(self.pick_light)
        light_layout.addWidget(self.btn_light)
        layout.addLayout(light_layout)
        
        # Casillas Oscuras
        dark_layout = QHBoxLayout()
        dark_layout.addWidget(QLabel("Casillas Oscuras:"))
        self.btn_dark = QPushButton()
        self.update_button_color(self.btn_dark, self.dark_color)
        self.btn_dark.clicked.connect(self.pick_dark)
        dark_layout.addWidget(self.btn_dark)
        layout.addLayout(dark_layout)
        
        # Botones de Acción
        actions = QHBoxLayout()
        btn_ok = QPushButton("OK")
        btn_ok.clicked.connect(self.accept)
        btn_cancel = QPushButton("Cancelar")
        btn_cancel.clicked.connect(self.reject)
        actions.addWidget(btn_ok)
        actions.addWidget(btn_cancel)
        layout.addLayout(actions)

    def update_button_color(self, button, color):
        button.setStyleSheet(f"background-color: {color}; min-width: 50px;")

    def pick_light(self):
        c = QColorDialog.getColor(QColor(self.light_color), self, "Elegir Color Claro")
        if c.isValid():
            self.light_color = c.name()
            self.update_button_color(self.btn_light, self.light_color)

    def pick_dark(self):
        c = QColorDialog.getColor(QColor(self.dark_color), self, "Elegir Color Oscuro")
        if c.isValid():
            self.dark_color = c.name()
            self.update_button_color(self.btn_dark, self.dark_color)

    def get_colors(self):
        return self.light_color, self.dark_color
