from PySide6.QtWidgets import QTableWidgetItem
from PySide6.QtCore import Qt

class SortableWidgetItem(QTableWidgetItem):
    """
    Permite ordenar celdas de una QTableWidget por un valor numérico oculto 
    en lugar del texto formateado (ej: ordena por 1500 en lugar de "1.5k").
    """
    def __lt__(self, other):
        try:
            v1 = self.data(Qt.UserRole)
            v2 = other.data(Qt.UserRole)
            if v1 is not None and v2 is not None:
                return float(v1) < float(v2)
        except: pass
        return super().__lt__(other)

def format_qty(n, precise=False):
    """
    Formatea números grandes para visualización en badges y tablas.
    4900000 -> 4.9M o 4.900.000 (si precise=True)
    """
    if precise:
        return f"{n:,}".replace(",", ".")
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M".replace(".00M", "M")
    if n >= 1_000:
        return f"{n/1_000:.1f}k".replace(".0k", "k")
    return str(n)
