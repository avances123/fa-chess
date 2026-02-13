from PySide6.QtWidgets import QWidget
from PySide6.QtGui import QPainter, QColor
from PySide6.QtCore import Qt

class ResultsWidget(QWidget):
    def __init__(self, w, d, b, total, is_white):
        super().__init__()
        self.w, self.d, self.b, self.total, self.is_white = w, d, b, total, is_white
        self.setMinimumWidth(80)
        self.setFixedHeight(18)
        if total > 0:
            p_win = ((w + 0.5 * d) / total) * 100 if is_white else ((b + 0.5 * d) / total) * 100
            self.setToolTip(f"Éxito: {p_win:.1f}% (W:{w} D:{d} L:{b})")

    def paintEvent(self, event):
        if self.total == 0: return
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        rect = self.rect().adjusted(2, 4, -2, -4)
        
        w_px = int(rect.width() * (self.w / self.total))
        d_px = int(rect.width() * (self.d / self.total))
        b_px = max(0, rect.width() - w_px - d_px)
        
        p.fillRect(rect.x(), rect.y(), w_px, rect.height(), QColor("#eee"))
        p.fillRect(rect.x() + w_px, rect.y(), d_px, rect.height(), QColor("#999"))
        p.fillRect(rect.x() + w_px + d_px, rect.y(), b_px, rect.height(), QColor("#333"))
        
        p.setPen(QColor("#777"))
        p.drawRect(rect)
