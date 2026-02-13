import chess

def uci_to_san(board, uci):
    """
    Convierte una jugada UCI a SAN de forma segura usando el board proporcionado.
    Útil para el árbol de aperturas, el motor de análisis y el historial.
    """
    try:
        move = chess.Move.from_uci(uci)
        if move in board.legal_moves:
            return board.san(move)
        return uci # Si no es legal en esta posición, devolvemos UCI
    except:
        return uci
