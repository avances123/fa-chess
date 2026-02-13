from PySide6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QPushButton, 
                             QLabel, QLineEdit, QComboBox, QGroupBox, QCheckBox, 
                             QFrame, QSpacerItem, QSizePolicy)
from PySide6.QtCore import Qt
import qtawesome as qta

class SearchDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Filtro de Partidas")
        self.setMinimumWidth(350)
        main_layout = QVBoxLayout(self)

        # --- SECCIÓN 1: BÚSQUEDA RÁPIDA (PRESETS) ---
        presets_group = QGroupBox("Búsquedas Rápidas")
        presets_layout = QHBoxLayout(presets_group)
        
        btn_elite = QPushButton(qta.icon('fa5s.trophy', color='#fbc02d'), " Élite (+2700)")
        btn_elite.clicked.connect(lambda: self.apply_preset(2700))
        
        btn_master = QPushButton(qta.icon('fa5s.medal', color='#757575'), " Maestro (+2400)")
        btn_master.clicked.connect(lambda: self.apply_preset(2400))
        
        presets_layout.addWidget(btn_elite)
        presets_layout.addWidget(btn_master)
        main_layout.addWidget(presets_group)

        # --- SECCIÓN 2: CRITERIOS PERSONALIZADOS ---
        criteria_group = QGroupBox("Criterios de Búsqueda")
        criteria_layout = QVBoxLayout(criteria_group)

        # Jugadores
        player_layout = QHBoxLayout()
        self.white_input = QLineEdit()
        self.white_input.setPlaceholderText("Nombre Blancas...")
        self.black_input = QLineEdit()
        self.black_input.setPlaceholderText("Nombre Negras...")
        player_layout.addWidget(QLabel("W:"))
        player_layout.addWidget(self.white_input)
        player_layout.addWidget(QLabel("B:"))
        player_layout.addWidget(self.black_input)
        criteria_layout.addLayout(player_layout)

        # Elo y Resultado
        data_layout = QHBoxLayout()
        self.min_elo_input = QLineEdit()
        self.min_elo_input.setPlaceholderText("Elo mín...")
        self.result_combo = QComboBox()
        self.result_combo.addItems(["Cualquiera", "1-0", "0-1", "1/2-1/2"])
        
        data_layout.addWidget(QLabel("Min Elo:"))
        data_layout.addWidget(self.min_elo_input)
        data_layout.addWidget(QLabel("Res:"))
        data_layout.addWidget(self.result_combo)
        criteria_layout.addLayout(data_layout)

        # Fechas
        date_layout = QHBoxLayout()
        self.date_from = QLineEdit()
        self.date_from.setPlaceholderText("Desde (AAAA.MM.DD)")
        self.date_from.setInputMask("0000.00.00")
        
        self.date_to = QLineEdit()
        self.date_to.setPlaceholderText("Hasta (AAAA.MM.DD)")
        self.date_to.setInputMask("0000.00.00")
        
        date_layout.addWidget(QLabel("Fecha:"))
        date_layout.addWidget(self.date_from)
        date_layout.addWidget(QLabel("-"))
        date_layout.addWidget(self.date_to)
        criteria_layout.addLayout(date_layout)

        main_layout.addWidget(criteria_group)

        # --- SECCIÓN 3: FILTRO POR POSICIÓN ---
        pos_group = QGroupBox("Filtro por Posición")
        pos_layout = QVBoxLayout(pos_group)
        self.pos_check = QCheckBox(" Buscar solo partidas con la posición actual")
        self.pos_check.setIcon(qta.icon('fa5s.crosshairs', color='#1976d2'))
        self.pos_check.setChecked(False) 
        pos_layout.addWidget(self.pos_check)
        self.pos_info = QLabel("<i>Se usará la posición que muestra el tablero actualmente.</i>")
        self.pos_info.setStyleSheet("color: #666; font-size: 10px;")
        pos_layout.addWidget(self.pos_info)
        main_layout.addWidget(pos_group)

        # --- BOTONES DE ACCIÓN ---
        main_layout.addStretch()
        btns = QHBoxLayout()
        
        self.btn_ok = QPushButton(qta.icon('fa5s.filter', color='white'), " Aplicar Filtro")
        self.btn_ok.setStyleSheet("QPushButton { background-color: #2e7d32; color: white; font-weight: bold; padding: 6px; }")
        self.btn_ok.clicked.connect(self.accept)
        
        btn_clear = QPushButton(qta.icon('fa5s.eraser'), " Limpiar Formulario")
        btn_clear.setStatusTip("Vaciar todos los campos del formulario")
        btn_clear.clicked.connect(self.clear)
        
        btn_cancel = QPushButton("Cancelar")
        btn_cancel.clicked.connect(self.reject)
        
        btns.addWidget(btn_clear)
        btns.addStretch()
        btns.addWidget(btn_cancel)
        btns.addWidget(self.btn_ok)
        main_layout.addLayout(btns)

    def apply_preset(self, elo):
        self.clear()
        self.min_elo_input.setText(str(elo))
        # No cerramos el diálogo para que el usuario pueda refinar si quiere

    def clear(self):
        self.white_input.clear()
        self.black_input.clear()
        self.min_elo_input.clear()
        self.date_from.clear()
        self.date_to.clear()
        self.result_combo.setCurrentIndex(0)
        self.pos_check.setChecked(False)
        self.white_input.setFocus() # Devolver el foco al primer campo

    def reset_all(self):
        """Limpia el formulario y cierra el diálogo indicando que se deben quitar los filtros"""
        self.clear()
        self.accept() # Cerramos aplicando los campos vacíos

    def get_criteria(self):
        # Limpiamos las fechas si están incompletas (solo tienen los puntos de la máscara)
        d_from = self.date_from.text() if self.date_from.text() != "...." else None
        d_to = self.date_to.text() if self.date_to.text() != "...." else None
        
        return {
            "white": self.white_input.text(),
            "black": self.black_input.text(),
            "min_elo": self.min_elo_input.text(),
            "date_from": d_from,
            "date_to": d_to,
            "result": self.result_combo.currentText(),
            "use_position": self.pos_check.isChecked()
        }
