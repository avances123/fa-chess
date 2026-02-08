# Estilos Globales de la Aplicación

# Colores Base
COLOR_PRIMARY = "#2e7d32" # Verde éxito
COLOR_ERROR = "#c62828"   # Rojo error/inverso
COLOR_TEXT_MUTE = "#555555"
COLOR_BG_MUTE = "#e0e0e0"
COLOR_BORDER = "#cccccc"

# Estilos de Componentes
STYLE_PROGRESS_BAR = """
    QProgressBar { 
        border: 1px solid #aaa; 
        background: #eee; 
        border-radius: 2px;
    } 
    QProgressBar::chunk { 
        background-color: #4caf50; 
    }
"""

STYLE_EVAL_BAR = """
    QProgressBar { 
        border: 1px solid #777; 
        background-color: #333; 
    } 
    QProgressBar::chunk { 
        background-color: #eee; 
    }
"""

# Estilos de Etiquetas (Badges)
def get_badge_style(color=COLOR_TEXT_MUTE, bg=COLOR_BG_MUTE, border=COLOR_BORDER):
    return f"""
        font-family: monospace; 
        font-weight: bold; 
        color: {color}; 
        background: {bg}; 
        padding: 4px; 
        border-radius: 3px; 
        border: 1px solid {border}; 
        margin-bottom: 5px;
    """

STYLE_BADGE_NORMAL = get_badge_style()
STYLE_BADGE_SUCCESS = get_badge_style(color=COLOR_PRIMARY, bg="#e8f5e9", border="#a5d6a7")
STYLE_BADGE_ERROR = get_badge_style(color=COLOR_ERROR, bg="#ffebee", border="#ffcdd2")

STYLE_LABEL_EVAL = """
    font-weight: bold; 
    font-size: 14px; 
    color: #d32f2f; 
    margin-left: 10px;
"""

STYLE_TABLE_HEADER = "QHeaderView::section { font-weight: bold; }"

STYLE_GAME_HEADER = """
    QLabel {
        background-color: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 4px;
        padding: 10px;
        margin-bottom: 5px;
        font-family: 'Segoe UI', sans-serif;
    }
"""
