from PySide6.QtWidgets import QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QLineEdit, QComboBox

class SearchDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Buscar Partidas")
        layout = QVBoxLayout(self)

        self.white_input = QLineEdit()
        self.black_input = QLineEdit()
        self.min_elo_input = QLineEdit()
        self.result_combo = QComboBox()
        self.result_combo.addItems(["Cualquiera", "1-0", "0-1", "1/2-1/2"])

        layout.addWidget(QLabel("Blancas:"))
        layout.addWidget(self.white_input)
        layout.addWidget(QLabel("Negras:"))
        layout.addWidget(self.black_input)
        layout.addWidget(QLabel("Elo Mínimo:"))
        layout.addWidget(self.min_elo_input)
        layout.addWidget(QLabel("Resultado:"))
        layout.addWidget(self.result_combo)

        btns = QHBoxLayout()
        btn_ok = QPushButton("Filtrar")
        btn_ok.clicked.connect(self.accept)
        btn_clear = QPushButton("Limpiar")
        btn_clear.clicked.connect(self.clear)
        btn_cancel = QPushButton("Cancelar")
        btn_cancel.clicked.connect(self.reject)
        btns.addWidget(btn_ok)
        btns.addWidget(btn_clear)
        btns.addWidget(btn_cancel)
        layout.addLayout(btns)

    def clear(self):
        self.white_input.clear()
        self.black_input.clear()
        self.min_elo_input.clear()
        self.result_combo.setCurrentIndex(0)

    def get_criteria(self):
        return {
            "white": self.white_input.text(),
            "black": self.black_input.text(),
            "min_elo": self.min_elo_input.text(),
            "result": self.result_combo.currentText()
        }
