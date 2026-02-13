from PySide6.QtWidgets import (QVBoxLayout, QHBoxLayout, QLabel, 
                             QTableWidget, QTableWidgetItem, QHeaderView, QWidget, QFrame, QScrollArea)
from PySide6.QtCore import Qt, QSize
from PySide6.QtGui import QFont, QColor
import qtawesome as qta
import chess
from src.ui.widgets.results_bar import ResultsWidget

class PlayerReportWidget(QWidget):
    def __init__(self, stats, parent=None):
        super().__init__(parent)
        self.stats = stats
        self.init_ui()

    def get_section_frame(self):
        f = QFrame()
        f.setStyleSheet("QFrame { background-color: #ffffff; border: 1px solid #e0e0e0; border-radius: 8px; }")
        return f

    def create_stat_item(self, icon, label, value, color="#333", tooltip=""):
        w = QWidget()
        w.setToolTip(tooltip)
        l = QHBoxLayout(w)
        l.setContentsMargins(0, 5, 0, 5)
        l.setSpacing(10) # Espaciado pequeño y fijo entre elementos
        
        icon_label = QLabel()
        icon_label.setPixmap(qta.icon(icon, color=color).pixmap(QSize(18, 18)))
        l.addWidget(icon_label)
        
        l.addWidget(QLabel(f"<span style='color: #666;'>{label}:</span>"))
        l.addWidget(QLabel(f"<b style='color: {color};'>{value}</b>"))
        l.addStretch() # El estiramiento va al final para que los datos queden a la izquierda
        return w

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        
        # Scroll Area para informes largos
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        content_w = QWidget()
        scroll.setWidget(content_w)
        l = QVBoxLayout(content_w)
        l.setSpacing(15)
        
        # --- CABECERA ---
        header_widget = QFrame()
        header_widget.setStyleSheet("background: #f8f9fa; border-radius: 8px; border: 1px solid #eee;")
        header = QHBoxLayout(header_widget)
        
        header.addWidget(QLabel(f"<span style='font-size: 24px; font-weight: bold;'>Dossier: {self.stats['name']}</span>"))
        header.addStretch()
        
        total = self.stats['as_white']['total'] + self.stats['as_black']['total']
        max_elo = self.stats.get('max_elo', '???')
        
        # Grupo de stats compacto a la derecha
        stats_group = QHBoxLayout()
        stats_group.addWidget(self.create_stat_item('fa5s.trophy', 'ELO Max', max_elo, '#d4af37'))
        stats_group.addWidget(self.create_stat_item('fa5s.chess', 'Partidas', total))
        header.addLayout(stats_group)
        
        l.addWidget(header_widget)

        # --- REPERTORIO Y ERRORES (50/50 Split) ---
        rep_layout = QHBoxLayout()
        rep_layout.setSpacing(15)
        
        for side, title, color in [('top_white', 'REPERTORIO BLANCAS', '#1976d2'), ('top_black', 'REPERTORIO NEGRAS', '#555')]:
            f = self.get_section_frame()
            fl = QVBoxLayout(f)
            fl.addWidget(QLabel(f"<b style='color: {color}; font-size: 12px;'>{title}</b>"))
            
            table = QTableWidget(len(self.stats[side]), 4)
            table.setHorizontalHeaderLabels(["Apertura", "Part.", "Pts %", "Teoría"])
            
            # Ayudas
            table.horizontalHeaderItem(1).setToolTip("Número total de partidas")
            table.horizontalHeaderItem(2).setToolTip("Puntos obtenidos %")
            table.horizontalHeaderItem(3).setToolTip("Media de jugadas de teoría seguida")
            
            table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
            for i in [1, 2, 3]: table.horizontalHeader().setSectionResizeMode(i, QHeaderView.ResizeToContents)
            table.verticalHeader().setVisible(False)
            table.setStyleSheet("QTableWidget { border: none; background: white; }")
            
            for i, r in enumerate(self.stats[side]):
                table.setItem(i, 0, QTableWidgetItem(r['opening_name']))
                table.setItem(i, 1, QTableWidgetItem(str(r['count'])))
                
                rate = r['win_rate']
                it_rate = QTableWidgetItem(f"{rate:.0f}%")
                it_rate.setTextAlignment(Qt.AlignCenter)
                if rate > 55: it_rate.setForeground(QColor("#2e7d32"))
                elif rate < 45: it_rate.setForeground(QColor("#c62828"))
                table.setItem(i, 2, it_rate)
                
                depth = r.get('avg_depth', 0)
                it_depth = QTableWidgetItem(f"{depth:.1f}")
                it_depth.setTextAlignment(Qt.AlignCenter)
                if depth < 8: it_depth.setBackground(QColor("#fff3e0"))
                table.setItem(i, 3, it_depth)
            
            table.setMinimumHeight(350)
            fl.addWidget(table)
            rep_layout.addWidget(f, 1) # Factor 1 para que ocupen 50% cada uno
            
        l.addLayout(rep_layout)

        # --- DEBILIDADES ---
        f_weak = self.get_section_frame()
        lw = QVBoxLayout(f_weak)
        lw.addWidget(QLabel("<b style='color: #c62828;'>DEBILIDADES TEÓRICAS DETECTADAS</b>"))
        
        all_rep = self.stats['top_white'] + self.stats['top_black']
        weakest = sorted(all_rep, key=lambda x: (x['win_rate'], x['avg_depth']))[:3]
        
        for w in weakest:
            lw.addWidget(QLabel(f"⚠️ <b>{w['opening_name']}</b>: Puntos {w['win_rate']:.1f}% | Desviación media en jugada {w['avg_depth']:.1f}"))
            
        l.addWidget(f_weak)
        main_layout.addWidget(scroll)
