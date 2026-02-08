from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                             QFrame, QGridLayout)
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
import qtawesome as qta

class AnalysisReport(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.stats = {"white": {}, "black": {}}
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        
        # Título
        title = QLabel("Informe de Precisión")
        title.setAlignment(Qt.AlignCenter)
        title.setStyleSheet("font-weight: bold; font-size: 16px; margin-bottom: 10px;")
        layout.addWidget(title)
        
        # Contenedor de Grid
        grid_frame = QFrame()
        grid_frame.setStyleSheet("background-color: #f5f5f5; border-radius: 5px;")
        self.grid = QGridLayout(grid_frame)
        self.grid.setSpacing(10)
        
        # Cabeceras
        self.lbl_white = QLabel("Blancas")
        self.lbl_black = QLabel("Negras")
        for lbl in [self.lbl_white, self.lbl_black]:
            lbl.setAlignment(Qt.AlignCenter)
            lbl.setStyleSheet("font-weight: bold; font-size: 14px; color: #333;")
        
        self.grid.addWidget(self.lbl_white, 0, 0)
        self.grid.addWidget(self.lbl_black, 0, 2)
        
        # Filas de datos
        self.labels = {}
        rows = [
            ("inaccuracies", "Imprecisiones", "#FFA726"), # Naranja
            ("mistakes", "Errores", "#EF6C00"),      # Naranja Oscuro
            ("blunders", "Errores Graves", "#D32F2F"), # Rojo
            ("acpl", "Pérdida Promedio (ACPL)", "#555"),
            ("accuracy", "Precisión Global", "#2E7D32") # Verde
        ]
        
        for i, (key, text, color) in enumerate(rows):
            row_idx = i + 1
            # Valor Blancas
            lbl_w = QLabel("-")
            lbl_w.setAlignment(Qt.AlignCenter)
            lbl_w.setStyleSheet(f"font-weight: bold; color: {color};")
            self.labels[f"white_{key}"] = lbl_w
            self.grid.addWidget(lbl_w, row_idx, 0)
            
            # Etiqueta Central
            lbl_text = QLabel(text)
            lbl_text.setAlignment(Qt.AlignCenter)
            lbl_text.setStyleSheet("color: #666; font-size: 11px;")
            self.grid.addWidget(lbl_text, row_idx, 1)
            
            # Valor Negras
            lbl_b = QLabel("-")
            lbl_b.setAlignment(Qt.AlignCenter)
            lbl_b.setStyleSheet(f"font-weight: bold; color: {color};")
            self.labels[f"black_{key}"] = lbl_b
            self.grid.addWidget(lbl_b, row_idx, 2)

        layout.addWidget(grid_frame)
        layout.addStretch()

    def update_stats(self, evaluations, moves_uci, white_name="Blancas", black_name="Negras"):
        """
        Calcula estadísticas basadas en la lista de evaluaciones (centipeones)
        """
        # Actualizar Cabeceras con Iconos
        self.lbl_white.setText(white_name)
        self.lbl_white.setPixmap(qta.icon('fa5s.chess-king', color='#eee').pixmap(16, 16)) # Icono Rey Blanco (simulado)
        # qtawesome no permite poner icono directo en label texto fácil, usamos layout o rich text
        # Simplificación: Usar el texto y el icono en un layout o HTML si fuera necesario.
        # Mejor enfoque simple: Texto directo
        self.lbl_white.setText(f"♔ {white_name}")
        self.lbl_black.setText(f"♚ {black_name}")
        
        # Reiniciar stats
        stats = {
            "white": {"inaccuracies": 0, "mistakes": 0, "blunders": 0, "loss_sum": 0, "moves": 0},
            "black": {"inaccuracies": 0, "mistakes": 0, "blunders": 0, "loss_sum": 0, "moves": 0}
        }
        
        # Necesitamos pares (eval_pre, eval_post) para cada movimiento
        # evals[0] es startpos. evals[1] es tras 1. e4 (jugada blanca).
        # Para blancas (índices pares en moves_uci, 0, 2...): Loss = evals[i] - evals[i+1] (si evals[i] > evals[i+1])
        # OJO: Las evaluaciones siempre son relativas a blancas en mi implementación interna (+ ventaja blanca)
        # Si juega blancas: Queremos maximizar eval. Loss = Eval_Pre - Eval_Post
        # Si juega negras: Queremos minimizar eval (hacerlo negativo). Loss = Eval_Post - Eval_Pre
        
        # Validar longitudes
        if len(evaluations) < len(moves_uci) + 1:
            return # Datos incompletos

        for i, move in enumerate(moves_uci):
            is_white = (i % 2 == 0)
            player = "white" if is_white else "black"
            
            # Saturar evaluaciones a +/- 1000 (10.0) para que errores en posiciones 
            # totalmente ganadas/perdidas no distorsionen el ACPL.
            eval_pre = max(-1000, min(1000, evaluations[i]))
            eval_post = max(-1000, min(1000, evaluations[i+1]))
            
            # Calcular pérdida (Loss) siempre positiva
            if is_white:
                loss = max(0, eval_pre - eval_post)
            else:
                loss = max(0, eval_post - eval_pre)
            
            # Acumular para ACPL
            stats[player]["loss_sum"] += loss
            stats[player]["moves"] += 1
            
            # Categorizar Error (Estándares competitivos)
            if loss >= 200:   # Blunder > 2.00
                stats[player]["blunders"] += 1
            elif loss >= 90:  # Mistake > 0.90
                stats[player]["mistakes"] += 1
            elif loss >= 40:  # Inaccuracy > 0.40
                stats[player]["inaccuracies"] += 1

        # Calcular finales y actualizar UI
        for color in ["white", "black"]:
            s = stats[color]
            moves = max(1, s["moves"])
            acpl = s["loss_sum"] / moves
            
            # Fórmula de Precisión ajustada (Aprox. realismo competitivo)
            # 0 ACPL -> 100% | 20 ACPL -> 90% | 50 ACPL -> 77% | 100 ACPL -> 60%
            import math
            accuracy = 100 * math.exp(-0.005 * acpl)
            
            self.labels[f"{color}_inaccuracies"].setText(str(s["inaccuracies"]))
            self.labels[f"{color}_mistakes"].setText(str(s["mistakes"]))
            self.labels[f"{color}_blunders"].setText(str(s["blunders"]))
            self.labels[f"{color}_acpl"].setText(f"{acpl:.1f}")
            self.labels[f"{color}_accuracy"].setText(f"{accuracy:.1f}%")