import os
import logging
import sys
import polars as pl

# Esquema unificado para todas las bases de datos de partidas
GAME_SCHEMA = {
    "id": pl.Int64, 
    "white": pl.String, 
    "black": pl.String, 
    "w_elo": pl.Int64, 
    "b_elo": pl.Int64, 
    "result": pl.String, 
    "date": pl.String, 
    "event": pl.String, 
    "site": pl.String,
    "line": pl.String, 
    "full_line": pl.String, 
    "fens": pl.List(pl.UInt64)
}

# Rutas del Proyecto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ASSETS_DIR = os.path.join(BASE_DIR, "assets")

# Carpeta de configuración de usuario (estándar ~/.config/fa-chess)
USER_CONFIG_DIR = os.path.expanduser("~/.config/fa-chess")
os.makedirs(USER_CONFIG_DIR, exist_ok=True)

CONFIG_FILE = os.path.join(USER_CONFIG_DIR, "fa-chess.json")
APP_DB_FILE = os.path.join(USER_CONFIG_DIR, "fa-chess.db")
CLIPBASE_FILE = os.path.join(USER_CONFIG_DIR, "fa-chess-clipbase.parquet")

# CONFIGURACIÓN DE LOGGING
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("fa-chess")
logger.info("Sistema de logging iniciado")

# Archivos de datos
ECO_FILE = os.path.join(ASSETS_DIR, "scid.eco")
PUZZLE_FILE = os.path.join(ASSETS_DIR, "puzzle.parquet")

# Estilos CSS (Tema Claro/Lichess)
LIGHT_STYLE = """
QMainWindow { background-color: #f5f5f5; }
QWidget { color: #222; font-family: 'Inter', sans-serif; font-size: 13px; }
QTableWidget, QListWidget { background-color: #ffffff; border: 1px solid #ddd; color: #000; }
QHeaderView::section { background-color: #f0f0f0; color: #333; padding: 6px; border: 1px solid #ddd; }
QPushButton { 
    background-color: #e0e0e0; border: 1px solid #ccc; padding: 6px; border-radius: 3px; font-weight: bold;
}
QPushButton:hover { background-color: #d0d0d0; }
QPushButton:pressed { background-color: #c0c0c0; padding-top: 8px; padding-left: 7px; border-style: inset; }
QLineEdit { background-color: #fff; border: 1px solid #ddd; padding: 4px; }
QTabWidget::pane { border: 1px solid #ccc; }
QTextBrowser { background-color: #fff; border: 1px solid #ddd; color: #000; font-size: 16px; }
"""
