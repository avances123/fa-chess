from PySide6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QPushButton, 
                             QLabel, QColorDialog, QTabWidget, QWidget, 
                             QLineEdit, QSpinBox, QDoubleSpinBox, QFileDialog, QFormLayout)
from PySide6.QtGui import QColor, QIcon
from PySide6.QtCore import Qt
import qtawesome as qta
import os

class SettingsDialog(QDialog):
    def __init__(self, current_config, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Configuración de fa-chess")
        self.resize(480, 400)
        
        self.config = current_config
        
        layout = QVBoxLayout(self)
        self.tabs = QTabWidget()
        
        # --- PESTAÑA APARIENCIA ---
        tab_appearance = QWidget()
        app_layout = QFormLayout(tab_appearance)
        
        self.btn_light = QPushButton()
        self.btn_light.setFixedWidth(60)
        self.update_button_color(self.btn_light, self.config.get("color_light", "#eeeed2"))
        self.btn_light.clicked.connect(self.pick_light)
        app_layout.addRow("Color Casillas Claras:", self.btn_light)
        
        self.btn_dark = QPushButton()
        self.btn_dark.setFixedWidth(60)
        self.update_button_color(self.btn_dark, self.config.get("color_dark", "#8ca2ad"))
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
        
        # --- PESTAÑA MOTOR ---
        tab_engine = QWidget()
        eng_layout = QFormLayout(tab_engine)
        
        path_layout = QHBoxLayout()
        self.edit_engine_path = QLineEdit(self.config.get("engine_path", "/usr/bin/stockfish"))
        btn_browse = QPushButton(qta.icon("fa5s.folder-open"), "")
        btn_browse.clicked.connect(self.browse_engine)
        path_layout.addWidget(self.edit_engine_path)
        path_layout.addWidget(btn_browse)
        eng_layout.addRow("Ruta Stockfish:", path_layout)
        
        self.spin_threads = QSpinBox()
        self.spin_threads.setRange(1, 128)
        self.spin_threads.setValue(self.config.get("engine_threads", 1))
        eng_layout.addRow("Hilos (Threads):", self.spin_threads)
        
        self.spin_hash = QSpinBox()
        self.spin_hash.setRange(16, 65536)
        self.spin_hash.setSingleStep(16)
        self.spin_hash.setValue(self.config.get("engine_hash", 64))
        self.spin_hash.setSuffix(" MB")
        eng_layout.addRow("Memoria Hash:", self.spin_hash)
        
        self.spin_depth = QSpinBox()
        self.spin_depth.setRange(0, 50)
        self.spin_depth.setValue(self.config.get("engine_depth", 10))
        self.spin_depth.setSpecialValueText("Infinito")
        self.spin_depth.setToolTip("Límite para el motor principal y análisis de partida. 0 = No para nunca.")
        eng_layout.addRow("Profundidad Motor (0=Inf):", self.spin_depth)
        
        self.spin_tree_depth = QSpinBox()
        self.spin_tree_depth.setRange(1, 30)
        self.spin_tree_depth.setValue(self.config.get("tree_depth", 12))
        self.spin_tree_depth.setToolTip("Profundidad fija para rellenar las filas del árbol. Mantén este valor bajo para velocidad.")
        eng_layout.addRow("Profundidad del Árbol:", self.spin_tree_depth)
        
        self.tabs.addTab(tab_engine, qta.icon("fa5s.microchip"), "Motor")

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
        c = QColorDialog.getColor(QColor(self.config["color_light"]), self, "Elegir Color Claro")
        if c.isValid():
            self.config["color_light"] = c.name()
            self.update_button_color(self.btn_light, self.config["color_light"])

    def pick_dark(self):
        c = QColorDialog.getColor(QColor(self.config["color_dark"]), self, "Elegir Color Oscuro")
        if c.isValid():
            self.config["color_dark"] = c.name()
            self.update_button_color(self.btn_dark, self.config["color_dark"])

    def browse_engine(self):
        path, _ = QFileDialog.getOpenFileName(self, "Seleccionar Stockfish", "/usr/bin", "Ejecutables (*)")
        if path:
            self.edit_engine_path.setText(path)

    def get_config(self):
        return {
            "color_light": self.config["color_light"],
            "color_dark": self.config["color_dark"],
            "perf_threshold": self.spin_perf.value(),
            "engine_path": self.edit_engine_path.text(),
            "engine_threads": self.spin_threads.value(),
            "engine_hash": self.spin_hash.value(),
            "engine_depth": self.spin_depth.value(),
            "tree_depth": self.spin_tree_depth.value(),
            "venom_eval": self.spin_v_eval.value(),
            "venom_win": self.spin_v_win.value(),
            "practical_win": self.spin_p_win.value()
        }
