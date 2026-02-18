from PySide6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QPushButton, 
                             QLabel, QColorDialog, QTabWidget, QWidget, 
                             QSpinBox, QDoubleSpinBox, QFormLayout)
from PySide6.QtGui import QColor, QIcon
from PySide6.QtCore import Qt
import qtawesome as qta
import os

class SettingsDialog(QDialog):
    def __init__(self, current_config, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Preferencias de fa-chess")
        self.resize(400, 350)
        
        # Copia local de la config para editar colores
        self.config = current_config.copy()
        
        layout = QVBoxLayout(self)
        self.tabs = QTabWidget()
        
        # --- PESTAÑA APARIENCIA ---
        tab_appearance = QWidget()
        app_layout = QFormLayout(tab_appearance)
        
        self.btn_light = QPushButton()
        self.btn_light.setFixedWidth(60)
        self.update_button_color(self.btn_light, self.config.get("colors", {}).get("light", "#eeeed2"))
        self.btn_light.clicked.connect(self.pick_light)
        app_layout.addRow("Color Casillas Claras:", self.btn_light)
        
        self.btn_dark = QPushButton()
        self.btn_dark.setFixedWidth(60)
        self.update_button_color(self.btn_dark, self.config.get("colors", {}).get("dark", "#8ca2ad"))
        self.btn_dark.clicked.connect(self.pick_dark)
        app_layout.addRow("Color Casillas Oscuras:", self.btn_dark)
        
        self.spin_perf = QSpinBox()
        self.spin_perf.setRange(0, 500)
        self.spin_perf.setValue(self.config.get("perf_threshold", 25))
        self.spin_perf.setSuffix(" Elo")
        self.spin_perf.setToolTip("Sensibilidad de los colores en el árbol:\n"
                                 "Si Perf > AvElo + Umbral -> Verde\n"
                                 "Si Perf < AvElo - Umbral -> Rojo")
        app_layout.addRow("Umbral Performance:", self.spin_perf)
        
        self.tabs.addTab(tab_appearance, qta.icon("fa5s.palette"), "Apariencia")
        
        # --- PESTAÑA VENENO 🧪 ---
        tab_venom = QWidget()
        ven_layout = QFormLayout(tab_venom)
        
        # 1. Eval de Trampa
        self.spin_v_eval = QDoubleSpinBox()
        self.spin_v_eval.setRange(0.1, 5.0)
        self.spin_v_eval.setSingleStep(0.1)
        self.spin_v_eval.setValue(self.config.get("venom_eval", 0.5))
        self.spin_v_eval.setPrefix("-")
        self.spin_v_eval.setSuffix(" puntos")
        self.spin_v_eval.setToolTip("Define qué se considera una 'mala posición' para el motor.\n"
                                   "Ej: Si pones 0.5, el motor debe dar al menos -0.50 para buscar veneno.")
        ven_layout.addRow("Evaluación de Trampa:", self.spin_v_eval)
        
        # 2. WinRate de Trampa
        self.spin_v_win = QSpinBox()
        self.spin_v_win.setRange(1, 99)
        self.spin_v_win.setValue(self.config.get("venom_win", 52))
        self.spin_v_win.setSuffix(" %")
        self.spin_v_win.setToolTip("Mínimo porcentaje de victorias necesario para marcar una trampa 🧪.")
        ven_layout.addRow("Win Rate de Trampa:", self.spin_v_win)
        
        # 3. WinRate de Oro Práctico (Tablas)
        self.spin_p_win = QSpinBox()
        self.spin_p_win.setRange(1, 99)
        self.spin_p_win.setValue(self.config.get("practical_win", 60))
        self.spin_p_win.setSuffix(" %")
        self.spin_p_win.setToolTip("Mínimo Win Rate para jugadas que el motor evalúa como tablas (0.00).")
        ven_layout.addRow("Win Rate Práctico:", self.spin_p_win)
        
        self.tabs.addTab(tab_venom, qta.icon("fa5s.vial"), "Veneno")
        
        layout.addWidget(self.tabs)
        
        # Botones de Acción
        actions = QHBoxLayout()
        btn_ok = QPushButton("Guardar Cambios")
        btn_ok.setIcon(qta.icon("fa5s.check"))
        btn_ok.clicked.connect(self.accept)
        btn_cancel = QPushButton("Cancelar")
        btn_cancel.clicked.connect(self.reject)
        actions.addStretch()
        actions.addWidget(btn_ok)
        actions.addWidget(btn_cancel)
        layout.addLayout(actions)

    def update_button_color(self, button, color):
        button.setStyleSheet(f"background-color: {color}; border: 1px solid #999;")

    def pick_light(self):
        # Manejo seguro de diccionarios anidados
        current = self.config.get("colors", {}).get("light", "#eeeed2")
        c = QColorDialog.getColor(QColor(current), self, "Elegir Color Claro")
        if c.isValid():
            if "colors" not in self.config: self.config["colors"] = {}
            self.config["colors"]["light"] = c.name()
            self.update_button_color(self.btn_light, c.name())

    def pick_dark(self):
        current = self.config.get("colors", {}).get("dark", "#8ca2ad")
        c = QColorDialog.getColor(QColor(current), self, "Elegir Color Oscuro")
        if c.isValid():
            if "colors" not in self.config: self.config["colors"] = {}
            self.config["colors"]["dark"] = c.name()
            self.update_button_color(self.btn_dark, c.name())

    def get_config(self):
        # Devolvemos solo lo que gestiona este diálogo
        # Nota: MainWindow es responsable de hacer el merge con save_bulk
        return {
            "colors": self.config.get("colors", {"light": "#eeeed2", "dark": "#8ca2ad"}),
            "perf_threshold": self.spin_perf.value(),
            "venom_eval": self.spin_v_eval.value(),
            "venom_win": self.spin_v_win.value(),
            "practical_win": self.spin_p_win.value()
        }
