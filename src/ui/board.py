import chess
import chess.svg
import math
from PySide6.QtWidgets import QGraphicsView, QGraphicsScene, QMenu, QGraphicsTextItem
from PySide6.QtSvgWidgets import QGraphicsSvgItem
from PySide6.QtSvg import QSvgRenderer
from PySide6.QtCore import Qt, QPointF, Signal
from PySide6.QtGui import QPainter, QBrush, QColor, QAction, QPainterPath, QFont
from shiboken6 import isValid
from src.core.utils import get_material_diff

class ChessBoard(QGraphicsView):
    piece_drag_started = Signal()
    piece_drag_finished = Signal()
    piece_moved = Signal(str)

    def __init__(self, board, parent_main):
        super().__init__()
        self.board = board
        self.parent_main = parent_main
        self.scene = QGraphicsScene(self)
        self.setScene(self.scene)
        self.setRenderHint(QPainter.Antialiasing)
        self.setViewportUpdateMode(QGraphicsView.FullViewportUpdate)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        
        self.renderers = {}
        self.piece_items = []
        self.selected_square = None
        self.drag_item = None
        self.flipped = False
        self.engine_move = None
        self.hover_move = None
        self.highlighted_square = None 
        self.show_last_move = True
        
        self.square_size = 100
        self.board_margin = 0 # Espacio extra si fuera necesario
        self.color_light = "#eeeed2"
        self.color_dark = "#8ca2ad"
        self.color_last_move = QColor(246, 246, 105, 180)

    def flip(self):
        self.flipped = not self.flipped
        self.update_board()

    def set_engine_move(self, uci):
        self.engine_move = uci
        self.update_board()

    def set_hover_move(self, uci):
        self.hover_move = uci
        self.update_board()

    def resizeEvent(self, event):
        side = min(self.width(), self.height())
        self.square_size = side / 8
        self.scene.setSceneRect(0, 0, side, side)
        self.update_board()
        super().resizeEvent(event)

    def wheelEvent(self, event):
        if hasattr(self.parent_main, 'game') and self.parent_main.game:
            if event.angleDelta().y() > 0: self.parent_main.game.step_back()
            else: self.parent_main.game.step_forward()

    def get_square(self, pos):
        c = int(pos.x() // self.square_size)
        r = int(pos.y() // self.square_size)
        file = c if not self.flipped else 7 - c
        rank = 7 - r if not self.flipped else r
        if 0 <= file < 8 and 0 <= rank < 8: return chess.square(file, rank)
        return None

    def get_square_coords(self, square):
        file = chess.square_file(square); rank = chess.square_rank(square)
        if self.flipped: return 7 - file, rank
        else: return file, 7 - rank

    def mousePressEvent(self, event):
        pos = self.mapToScene(event.position().toPoint())
        sq = self.get_square(pos)
        if sq is not None:
            piece = self.board.piece_at(sq)
            if piece and piece.color == self.board.turn:
                self.selected_square = sq; self.update_board()
                c, r = self.get_square_coords(sq); target_pos = QPointF(c * self.square_size, r * self.square_size)
                for item in self.piece_items:
                    if isValid(item) and (item.pos() - target_pos).manhattanLength() < 5:
                        self.drag_item = item; self.drag_item.setZValue(100); self.piece_drag_started.emit(); break
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event):
        if self.drag_item and isValid(self.drag_item):
            pos = self.mapToScene(event.position().toPoint())
            self.drag_item.setPos(pos - QPointF(self.square_size/2, self.square_size/2))
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event):
        self.piece_drag_finished.emit()
        if self.selected_square is not None:
            pos = self.mapToScene(event.position().toPoint())
            target_sq = self.get_square(pos)
            if target_sq is not None:
                move = chess.Move(self.selected_square, target_sq); piece = self.board.piece_at(self.selected_square)
                if piece and piece.piece_type == chess.PAWN and chess.square_rank(target_sq) in [0, 7]: move.promotion = chess.QUEEN
                if move in self.board.legal_moves:
                    if hasattr(self.parent_main, 'game') and self.parent_main.game is not None: self.parent_main.game.make_move(move)
                    else: self.board.push(move)
                    self.piece_moved.emit(move.uci())
            self.selected_square = None; self.drag_item = None; self.update_board()
        super().mouseReleaseEvent(event)

    def update_board(self):
        self.scene.clear()
        self.piece_items.clear()
        
        # 1. Dibujar Casillas y Coordenadas
        font_coords = QFont("sans-serif", 8, QFont.Bold)
        for r in range(8):
            for c in range(8):
                is_light = (r + c) % 2 == 0
                color_hex = self.color_light if is_light else self.color_dark
                self.scene.addRect(c * self.square_size, r * self.square_size, self.square_size, self.square_size, Qt.NoPen, QBrush(QColor(color_hex)))
                
                # Coordenadas (Estilo Lichess: dentro de la casilla)
                coord_color = QColor(self.color_dark) if is_light else QColor(self.color_light)
                
                # Números (Rank) - Solo en la primera columna visible
                if c == 0:
                    rank_num = str(8 - r) if not self.flipped else str(r + 1)
                    txt = QGraphicsTextItem(rank_num)
                    txt.setFont(font_coords); txt.setDefaultTextColor(coord_color)
                    txt.setPos(c * self.square_size + 2, r * self.square_size + 2)
                    self.scene.addItem(txt)
                
                # Letras (File) - Solo en la última fila visible
                if r == 7:
                    file_let = chr(97 + c) if not self.flipped else chr(104 - c)
                    txt = QGraphicsTextItem(file_let)
                    txt.setFont(font_coords); txt.setDefaultTextColor(coord_color)
                    txt.setPos((c + 1) * self.square_size - 15, (r + 1) * self.square_size - 18)
                    self.scene.addItem(txt)

        # 2. Resaltar último movimiento
        if self.show_last_move and self.board.move_stack:
            last_move = self.board.peek()
            for sq in [last_move.from_square, last_move.to_square]:
                c, r = self.get_square_coords(sq)
                self.scene.addRect(c * self.square_size, r * self.square_size, self.square_size, self.square_size, Qt.NoPen, QBrush(self.color_last_move))

        # 3. Dibujar Piezas
        for square, piece in self.board.piece_map().items():
            c, r = self.get_square_coords(square)
            piece_key = f"{piece.symbol()}_{int(self.square_size)}"
            if piece_key not in self.renderers:
                svg_data = chess.svg.piece(piece, size=int(self.square_size)).encode("utf-8")
                self.renderers[piece_key] = QSvgRenderer(svg_data)
            item = QGraphicsSvgItem(); item.setSharedRenderer(self.renderers[piece_key]); item.setPos(c * self.square_size, r * self.square_size); self.scene.addItem(item); self.piece_items.append(item)

        # 4. Dibujar Flechas
        self.draw_move_arrow(self.engine_move, QColor(0, 120, 255, 100))
        self.draw_move_arrow(self.hover_move, QColor(255, 0, 0, 60))
        
        # 5. Resaltado especial (Pistas / Posición)
        if self.highlighted_square is not None:
            from PySide6.QtGui import QPen
            c, r = self.get_square_coords(self.highlighted_square)
            # Dibujar un marco azul brillante alrededor de la casilla
            pen = QPen(QColor(0, 191, 255), 4) # DeepSkyBlue
            rect = self.scene.addRect(c * self.square_size + 2, r * self.square_size + 2,
                                     self.square_size - 4, self.square_size - 4,
                                     pen, QBrush(Qt.NoBrush))
            rect.setZValue(50) # Por encima de las casillas pero debajo de flechas

    def draw_move_arrow(self, uci, color):
        if not uci: return
        try:
            m = chess.Move.from_uci(uci); c1, r1 = self.get_square_coords(m.from_square); c2, r2 = self.get_square_coords(m.to_square)
            start = QPointF((c1 + 0.5) * self.square_size, (r1 + 0.5) * self.square_size); end = QPointF((c2 + 0.5) * self.square_size, (r2 + 0.5) * self.square_size); dx, dy = end.x() - start.x(), end.y() - start.y(); dist = math.hypot(dx, dy)
            if dist > 10:
                angle = math.atan2(dy, dx); sw, hw, hl = self.square_size * 0.25, self.square_size * 0.55, self.square_size * 0.55; sed = dist - hl; path = QPainterPath()
                def get_pt(d, w): px = d * math.cos(angle) - w * math.sin(angle); py = d * math.sin(angle) + w * math.cos(angle); return start + QPointF(px, py)
                path.moveTo(get_pt(0, -sw/2)); path.lineTo(get_pt(sed, -sw/2)); path.lineTo(get_pt(sed, -hw/2)); path.lineTo(end); path.lineTo(get_pt(sed, hw/2)); path.lineTo(get_pt(sed, sw/2)); path.lineTo(get_pt(0, sw/2)); path.closeSubpath()
                self.scene.addPath(path, Qt.NoPen, QBrush(color))
        except: pass
