import chess
import chess.svg
from PySide6.QtWidgets import QGraphicsView, QGraphicsScene, QMenu
from PySide6.QtSvgWidgets import QGraphicsSvgItem
from PySide6.QtSvg import QSvgRenderer
from PySide6.QtCore import Qt, QPointF
from PySide6.QtGui import QPainter, QBrush, QColor, QAction

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
        self.flipped = False
        
        self.square_size = 100
        self.color_light = "#eeeed2"
        self.color_dark = "#8ca2ad"
        self.color_last_move = QColor(246, 246, 105, 180) # Amarillo translúcido

    def flip(self):
        self.flipped = not self.flipped
        self.update_board()

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
        r = int(pos.y() // self.square_size)
        
        if self.flipped:
            file = 7 - c
            rank = r
        else:
            file = c
            rank = 7 - r
            
        if 0 <= file < 8 and 0 <= rank < 8:
            return chess.square(file, rank)
        return None

    def get_square_coords(self, square):
        file = chess.square_file(square)
        rank = chess.square_rank(square)
        if self.flipped:
            return 7 - file, rank
        else:
            return file, 7 - rank

    def mousePressEvent(self, event):
        pos = self.mapToScene(event.position().toPoint())
        sq = self.get_square(pos)
        
        if sq is not None:
            piece = self.board.piece_at(sq)
            if piece and piece.color == self.board.turn:
                self.selected_square = sq
                self.update_board() # Para mostrar jugadas legales
                c, r = self.get_square_coords(sq)
                target_pos = QPointF(c * self.square_size, r * self.square_size)
                
                for item in self.piece_items:
                    if (item.pos() - target_pos).manhattanLength() < 5:
                        self.drag_item = item
                        self.drag_item.setZValue(100)
                        break
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event):
        if self.drag_item:
            pos = self.mapToScene(event.position().toPoint())
            self.drag_item.setPos(pos - QPointF(self.square_size/2, self.square_size/2))
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event):
        if self.selected_square is not None:
            pos = self.mapToScene(event.position().toPoint())
            target_sq = self.get_square(pos)
            
            if target_sq is not None:
                move = chess.Move(self.selected_square, target_sq)
                
                # Gestión de Promoción
                piece = self.board.piece_at(self.selected_square)
                if piece and piece.piece_type == chess.PAWN:
                    if chess.square_rank(target_sq) in [0, 7]:
                        move.promotion = self.select_promotion_piece()

                if move in self.board.legal_moves:
                    self.parent_main.make_move(move)
            
            self.selected_square = None
            self.drag_item = None
            self.update_board()
            
        super().mouseReleaseEvent(event)

    def select_promotion_piece(self):
        menu = QMenu(self)
        pieces = {
            "Dama": chess.QUEEN,
            "Torre": chess.ROOK,
            "Alfil": chess.BISHOP,
            "Caballo": chess.KNIGHT
        }
        for name, p_type in pieces.items():
            action = QAction(name, self)
            action.setData(p_type)
            menu.addAction(action)
        
        # Ejecutar menú en la posición del cursor
        action = menu.exec(self.cursor().pos())
        return action.data() if action else chess.QUEEN

    def update_board(self):
        self.scene.clear()
        self.piece_items.clear()
        
        # 1. Dibujar Escaques
        for r in range(8):
            for c in range(8):
                color_hex = self.color_light if (r + c) % 2 == 0 else self.color_dark
                self.scene.addRect(c * self.square_size, r * self.square_size, 
                                   self.square_size, self.square_size, 
                                   Qt.NoPen, QBrush(QColor(color_hex)))

        # 2. Resaltar último movimiento
        if self.board.move_stack:
            last_move = self.board.peek()
            for sq in [last_move.from_square, last_move.to_square]:
                c, r = self.get_square_coords(sq)
                self.scene.addRect(c * self.square_size, r * self.square_size,
                                   self.square_size, self.square_size,
                                   Qt.NoPen, QBrush(self.color_last_move))
        
        # 2.b Resaltar jugadas legales
        if self.selected_square is not None:
            for move in self.board.legal_moves:
                if move.from_square == self.selected_square:
                    c, r = self.get_square_coords(move.to_square)
                    margin = self.square_size * 0.35
                    size = self.square_size * 0.3
                    self.scene.addEllipse(c * self.square_size + margin, 
                                          r * self.square_size + margin, 
                                          size, size, 
                                          Qt.NoPen, QBrush(QColor(0, 0, 0, 60)))
        
        # 3. Dibujar Piezas
        for square, piece in self.board.piece_map().items():
            c, r = self.get_square_coords(square)
            
            piece_key = f"{piece.symbol()}_{int(self.square_size)}"
            if piece_key not in self.renderers:
                svg_data = chess.svg.piece(piece, size=int(self.square_size)).encode("utf-8")
                self.renderers[piece_key] = QSvgRenderer(svg_data)
            
            item = QGraphicsSvgItem()
            item.setSharedRenderer(self.renderers[piece_key])
            item.setPos(c * self.square_size, r * self.square_size)
            
            self.scene.addItem(item)
            self.piece_items.append(item)
