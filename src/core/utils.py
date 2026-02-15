import chess

def uci_to_san(board, uci):
    # ... (existente)
    try:
        move = chess.Move.from_uci(uci)
        if move in board.legal_moves:
            return board.san(move)
        return uci 
    except:
        return uci

def get_material_diff(board):
    """
    Calcula la ventaja material comparando las piezas de ambos bandos.
    Retorna un diccionario con la ventaja de puntos y la lista de piezas extra para cada color.
    """
    values = {chess.PAWN: 1, chess.KNIGHT: 3, chess.BISHOP: 3, chess.ROOK: 5, chess.QUEEN: 9}
    
    counts = {}
    for color in [chess.WHITE, chess.BLACK]:
        counts[color] = {}
        for piece_type in values.keys():
            counts[color][piece_type] = len(board.pieces(piece_type, color))
            
    diffs = {chess.WHITE: {}, chess.BLACK: {}}
    scores = {chess.WHITE: 0, chess.BLACK: 0}
    
    for pt in values.keys():
        w = counts[chess.WHITE][pt]
        b = counts[chess.BLACK][pt]
        if w > b:
            diffs[chess.WHITE][pt] = w - b
            scores[chess.WHITE] += (w - b) * values[pt]
        elif b > w:
            diffs[chess.BLACK][pt] = b - w
            scores[chess.BLACK] += (b - w) * values[pt]
            
    # Ajustar scores para que sea la ventaja neta (uno tendrá 0 o ambos 0)
    net_w = max(0, scores[chess.WHITE] - scores[chess.BLACK])
    net_b = max(0, scores[chess.BLACK] - scores[chess.WHITE])
    
    return {
        chess.WHITE: {'score': net_w, 'diffs': diffs[chess.WHITE]},
        chess.BLACK: {'score': net_b, 'diffs': diffs[chess.BLACK]}
    }
