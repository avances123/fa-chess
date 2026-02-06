from PySide6.QtWidgets import QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QLineEdit

class EditGameDialog(QDialog):
    def __init__(self, data, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Editar Datos de Partida")
        layout = QVBoxLayout(self)

        self.inputs = {}
        for key in ["white", "black", "w_elo", "b_elo", "result", "date", "event"]:
            layout.addWidget(QLabel(f"{key.capitalize()}:"))
            edit = QLineEdit(str(data.get(key, "")))
            layout.addWidget(edit)
            self.inputs[key] = edit

        btns = QHBoxLayout()
        btn_ok = QPushButton("Guardar")
        btn_ok.clicked.connect(self.accept)
        btn_cancel = QPushButton("Cancelar")
        btn_cancel.clicked.connect(self.reject)
        btns.addWidget(btn_ok)
        btns.addWidget(btn_cancel)
        layout.addLayout(btns)

    def get_data(self):
        data = {}
        for key, edit in self.inputs.items():
            val = edit.text()
            if "elo" in key:
                try:
                    val = int(val)
                except:
                    val = 0
            data[key] = val
        return data
