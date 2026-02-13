import os

# Rutas del Proyecto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ASSETS_DIR = os.path.join(BASE_DIR, "assets")
CONFIG_FILE = os.path.expanduser("~/.config/fa-chess.json")

# Archivos de datos
ECO_FILE = os.path.join(ASSETS_DIR, "scid.eco")
PUZZLE_FILE = os.path.join(ASSETS_DIR, "puzzle.parquet")

# Estilos CSS (Tema Claro/Lichess)
LIGHT_STYLE = """
QMainWindow { background-color: #f5f5f5; }
QWidget { color: #222; font-family: 'Inter', sans-serif; font-size: 13px; }
QTableWidget, QListWidget { background-color: #ffffff; border: 1px solid #ddd; color: #000; }
QHeaderView::section { background-color: #f0f0f0; color: #333; padding: 6px; border: 1px solid #ddd; }
QPushButton { background-color: #e0e0e0; border: 1px solid #ccc; padding: 6px; border-radius: 3px; font-weight: bold;}
QLineEdit { background-color: #fff; border: 1px solid #ddd; padding: 4px; }
QTabWidget::pane { border: 1px solid #ccc; }
QTextBrowser { background-color: #fff; border: 1px solid #ddd; color: #000; font-size: 16px; }
"""