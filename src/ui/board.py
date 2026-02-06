import chess
import chess.svg
from PySide6.QtWidgets import QGraphicsView, QGraphicsScene
from PySide6.QtSvgWidgets import QGraphicsSvgItem
from PySide6.QtSvg import QSvgRenderer
from PySide6.QtCore import Qt, QPointF
from PySide6.QtGui import QPainter, QBrush, QColor

class ChessBoard(QGraphicsView):
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
        
        self.square_size = 100
        self.color_light = "#eeeed2"
        self.color_dark = "#8ca2ad"
        self.color_piece_white = "#ffffff"
        self.color_piece_black = "#000000"

    def resizeEvent(self, event):
        side = min(self.width(), self.height())
        self.square_size = side / 8
        self.scene.setSceneRect(0, 0, side, side)
        self.update_board()
        super().resizeEvent(event)

    def wheelEvent(self, event):
        if event.angleDelta().y() > 0:
            self.parent_main.step_back()
        else:
            self.parent_main.step_forward()

    def get_square(self, pos):
        c = int(pos.x() // self.square_size)
        r = 7 - int(pos.y() // self.square_size)
        if 0 <= c < 8 and 0 <= r < 8:
            return chess.square(c, r)
        return None

    def mousePressEvent(self, event):
        pos = self.mapToScene(event.position().toPoint())
        sq = self.get_square(pos)
        
        if sq is not None:
            piece = self.board.piece_at(sq)
            if piece and piece.color == self.board.turn:
                self.selected_square = sq
                # Encontrar el item visual para arrastrarlo
                for item in self.piece_items:
                    r = chess.square_rank(sq)
                    c = chess.square_file(sq)
                    target_pos = QPointF(c * self.square_size, (7 - r) * self.square_size)
                    if (item.pos() - target_pos).manhattanLength() < 5:
                        self.drag_item = item
                        self.drag_item.setZValue(100) # Poner al frente
                        break
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event):
        if self.drag_item:
            pos = self.mapToScene(event.position().toPoint())
            # Centrar pieza en el cursor
            self.drag_item.setPos(pos - QPointF(self.square_size/2, self.square_size/2))
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event):
        if self.selected_square is not None:
            pos = self.mapToScene(event.position().toPoint())
            target_sq = self.get_square(pos)
            
            if target_sq is not None:
                move = chess.Move(self.selected_square, target_sq)
                # Auto-promoción a Dama
                if self.board.piece_at(self.selected_square).piece_type == chess.PAWN:
                    if chess.square_rank(target_sq) in [0, 7]:
                        move.promotion = chess.QUEEN

                if move in self.board.legal_moves:
                    self.parent_main.make_move(move)
            
            self.selected_square = None
            self.drag_item = None
            self.update_board() # Redibujar para recolocar piezas si el movimiento fue inválido
            
        super().mouseReleaseEvent(event)

    def update_board(self):
        self.scene.clear()
        self.piece_items.clear()
        
        # Dibujar Escaques
        for r in range(8):
            for c in range(8):
                color_hex = self.color_light if (r + c) % 2 == 0 else self.color_dark
                self.scene.addRect(c * self.square_size, (7 - r) * self.square_size, 
                                   self.square_size, self.square_size, 
                                   Qt.NoPen, QBrush(QColor(color_hex)))
        
        # Dibujar Piezas
        for square, piece in self.board.piece_map().items():
            r = chess.square_rank(square)
            c = chess.square_file(square)
            
            piece_key = f"{piece.symbol()}_{int(self.square_size)}"
            if piece_key not in self.renderers:
                svg_data = chess.svg.piece(piece, size=int(self.square_size)).encode("utf-8")
                self.renderers[piece_key] = QSvgRenderer(svg_data)
            
            item = QGraphicsSvgItem()
            item.setSharedRenderer(self.renderers[piece_key])
            item.setPos(c * self.square_size, (7 - r) * self.square_size)
            
            self.scene.addItem(item)
            self.piece_items.append(item)
