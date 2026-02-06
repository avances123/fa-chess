import chess.pgn
import polars as pl
import os
import time

def convert_pgn_to_parquet(pgn_path, output_path, max_games=1000000):
    """
    Convierte un PGN a Parquet extrayendo solo lo necesario para el árbol.
    """
    games_data = []
    start_time = time.time()
    
    print(f"Iniciando conversión de {pgn_path}...")
    
    with open(pgn_path, encoding="utf-8", errors="ignore") as pgn:
        count = 0
        while count < max_games:
            game = chess.pgn.read_game(pgn)
            if game is None:
                break
            
            # Extraemos metadatos
            headers = game.headers
            result = headers.get("Result", "*")
            w_elo = headers.get("WhiteElo", "0")
            b_elo = headers.get("BlackElo", "0")
            
            # Extraemos las primeras 12 jugadas (suficiente para el árbol de apertura)
            moves = []
            node = game
            for _ in range(12):
                if not node.variations:
                    break
                node = node.variation(0)
                moves.append(node.move.uci())
            
            moves_str = " ".join(moves)
            
            games_data.append({
                "w_elo": int(w_elo) if w_elo.isdigit() else 0,
                "b_elo": int(b_elo) if b_elo.isdigit() else 0,
                "result": result,
                "line": moves_str
            })
            
            count += 1
            if count % 10000 == 0:
                elapsed = time.time() - start_time
                print(f"Procesadas {count} partidas... ({count/elapsed:.1f} games/s)")

    # Creamos el DataFrame de Polars
    df = pl.DataFrame(games_data)
    
    # Guardamos en Parquet (mucho más rápido y pequeño que PGN)
    df.write_parquet(output_path)
    print(f"¡Hecho! Guardado en {output_path}")
    print(f"Tamaño final: {os.path.getsize(output_path) / (1024*1024):.2f} MB")

if __name__ == "__main__":
    # Prueba con una muestra pequeña del archivo que ya tienes filtrado
    PGN_FILE = "/data/chess/base_1M.pgn" 
    OUTPUT_FILE = "/data/chess/base_1M.parquet"
    
    if os.path.exists(PGN_FILE):
        convert_pgn_to_parquet(PGN_FILE, OUTPUT_FILE, max_games=1000000)
    else:
        print(f"No se encuentra el archivo {PGN_FILE}")
