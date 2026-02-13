from PySide6.QtWidgets import QWidget, QSizePolicy
from PySide6.QtGui import QPainter, QColor, QPen, QPainterPath, QBrush, QLinearGradient
from PySide6.QtCore import Qt, QPointF, Signal

class EvaluationGraph(QWidget):
    move_selected = Signal(int) # Emite el índice de la jugada al hacer click/hover

    def __init__(self, parent=None):
        super().__init__(parent)
        self.evals = [] # Lista de valores en centipeones (ej: [20, 45, -10, ...])
        self.current_idx = -1
        self.hover_idx = -1
        self.setMouseTracking(True)
        # self.setFixedHeight(100) # Eliminamos altura fija para que sea responsivo
        self.setMinimumHeight(100) # Altura mínima para que no desaparezca
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding) # Ocupar todo el espacio disponible
        # Tema Claro (Fondo Blanco)
        self.setStyleSheet("background-color: #ffffff; border: 1px solid #cccccc;")
        
        # Configuración visual estilo Lichess (Tema Claro)
        self.max_y = 500 # Límite visual (+5 peones)
        self.color_white = QColor(200, 255, 200) # Verde muy suave para ventaja blanca
        self.color_black = QColor(255, 200, 200) # Rojo muy suave para ventaja negra
        self.color_line = QColor(50, 50, 50)     # Gris oscuro para la línea
        self.color_cursor = QColor(0, 0, 255, 180) # Azul semitransparente para cursor

    def set_evaluations(self, evals_list):
        """Recibe una lista de enteros (centipeones). None o 0 si no hay dato."""
        self.evals = [e if e is not None else 0 for e in evals_list]
        self.update()

    def set_current_move(self, idx):
        self.current_idx = idx
        self.update()

    def paintEvent(self, event):
        if not self.evals: return
        
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        
        w = self.width()
        h = self.height()
        mid_y = h / 2
        
        # Margen izquierdo para etiquetas Y
        margin_left = 30
        graph_w = w - margin_left
        
        # Fondo
        painter.fillRect(self.rect(), Qt.white)
        
        # Dibujar Eje Y (Líneas y Etiquetas)
        font = painter.font()
        font.setPixelSize(10)
        painter.setFont(font)
        
        def get_y_coord(cp_val):
            # Clampear y mapear a coordenadas de pantalla (solo área gráfica)
            val = max(-self.max_y, min(self.max_y, cp_val))
            percent = val / self.max_y 
            return mid_y - (percent * (h / 2) * 0.9)

        # Valores de referencia para dibujar
        y_refs = [300, 100, 0, -100, -300] # +3, +1, 0, -1, -3
        
        for ref in y_refs:
            y_pos = get_y_coord(ref)
            
            # Línea de cuadrícula
            if ref == 0:
                painter.setPen(QPen(QColor("#555"), 1, Qt.SolidLine)) # Línea 0 más fuerte
            else:
                painter.setPen(QPen(QColor("#ddd"), 1, Qt.DashLine)) # Cuadrícula sutil
            
            painter.drawLine(margin_left, y_pos, w, y_pos)
            
            # Etiqueta numérica
            label = f"{ref/100:+.0f}" if ref != 0 else "0"
            painter.setPen(QPen(QColor("#777")))
            painter.drawText(0, int(y_pos) - 5, margin_left - 5, 10, Qt.AlignRight | Qt.AlignVCenter, label)

        # Dibujar Gráfica
        num_moves = len(self.evals)
        if num_moves < 2: return
        
        step_x = graph_w / (num_moves - 1) if num_moves > 1 else graph_w
        
        # Función auxiliar ajustada con margen
        def get_x(i):
            return margin_left + (i * step_x)

        # Construir Paths
        path_line = QPainterPath()
        path_white = QPainterPath()
        path_black = QPainterPath()
        
        # Puntos iniciales
        start_y = get_y_coord(self.evals[0])
        path_line.moveTo(margin_left, start_y)
        
        path_white.moveTo(margin_left, mid_y)
        path_black.moveTo(margin_left, mid_y)
        
        for i, val in enumerate(self.evals):
            x = get_x(i)
            y = get_y_coord(val)
            
            path_line.lineTo(x, y)
            
            if val >= 0:
                path_white.lineTo(x, y)
                path_black.lineTo(x, mid_y)
            else:
                path_white.lineTo(x, mid_y)
                path_black.lineTo(x, y)

        # Cerrar paths
        path_white.lineTo(w, mid_y)
        path_white.closeSubpath()
        path_black.lineTo(w, mid_y)
        path_black.closeSubpath()
        
        # Dibujar Rellenos
        painter.setPen(Qt.NoPen)
        painter.setBrush(QBrush(self.color_white))
        painter.drawPath(path_white)
        
        painter.setBrush(QBrush(self.color_black))
        painter.drawPath(path_black)
        
        # Dibujar Línea Principal
        painter.setPen(QPen(self.color_line, 1.5))
        painter.setBrush(Qt.NoBrush)
        painter.drawPath(path_line)
        
        # Dibujar Cursor de Posición Actual
        if 0 <= self.current_idx < num_moves:
            cx = get_x(self.current_idx)
            painter.setPen(QPen(self.color_cursor, 2))
            painter.drawLine(cx, 0, cx, h)

    def _get_move_index_at_pos(self, pos):
        if not self.evals: return -1
        w = self.width()
        margin_left = 30
        graph_w = w - margin_left
        
        # Ajustar posición relativa al margen
        rel_x = pos.x() - margin_left
        if rel_x < 0: rel_x = 0
        
        num_moves = len(self.evals)
        step_x = graph_w / (num_moves - 1) if num_moves > 1 else graph_w
        
        idx = int(round(rel_x / step_x))
        return max(0, min(idx, num_moves - 1))

    def leaveEvent(self, event):
        self.hover_idx = -1
        self.update()
