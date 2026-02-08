import chess.pgn
import chess.polyglot
import polars as pl
import os
import time
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn

# Esquema unificado (debe coincidir con DBManager)
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

def count_games(pgn_path):
    """Cuenta rápidamente las partidas buscando el tag [Event ]"""
    count = 0
    with open(pgn_path, "rb") as f:
        for line in f:
            if line.startswith(b"[Event "):
                count += 1
    return count

def extract_game_data(count, game):
    """Lógica unificada para extraer datos usando Zobrist Hashes para las posiciones"""
    headers = game.headers
    board = game.board()
    hashes = [chess.polyglot.zobrist_hash(board)]
    uci_moves = []
    
    for move in game.mainline_moves():
        uci_moves.append(move.uci())
        board.push(move)
        hashes.append(chess.polyglot.zobrist_hash(board))
    
    def safe_int(val):
        if not val: return 0
        try:
            clean_val = "".join(filter(str.isdigit, str(val)))
            return int(clean_val) if clean_val else 0
        except: return 0

    return {
        "id": count,
        "white": headers.get("White", "Unknown"),
        "black": headers.get("Black", "Unknown"),
        "w_elo": safe_int(headers.get("WhiteElo")),
        "b_elo": safe_int(headers.get("BlackElo")),
        "result": headers.get("Result", "*"),
        "date": headers.get("Date", "????.??.??"),
        "event": headers.get("Event", "?"),
        "site": headers.get("Site", ""),
        "line": " ".join(uci_moves[:12]),
        "full_line": " ".join(uci_moves),
        "fens": hashes 
    }

def convert_pgn_to_parquet(pgn_path, output_path, max_games=10000000, chunk_size=10000):
    """
    Convierte un PGN a Parquet procesando por chunks para optimizar memoria.
    """
    real_total = count_games(pgn_path)
    total_to_process = min(real_total, max_games)
    
    dfs = [] # Lista de DataFrames de Polars (más eficiente que lista de dicts)
    current_chunk = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[blue]{task.completed}/{task.total} games"),
        TextColumn("[green]{task.fields[speed]} g/s"),
        TimeElapsedColumn(),
    ) as progress:
        
        task = progress.add_task(
            f"Convirtiendo {os.path.basename(pgn_path)}", 
            total=total_to_process, 
            speed="0.0"
        )
        
        with open(pgn_path, encoding="utf-8", errors="ignore") as pgn:
            count = 0
            start_time = time.time()
            
            while count < total_to_process:
                game = chess.pgn.read_game(pgn)
                if game is None:
                    break
                
                current_chunk.append(extract_game_data(count, game))
                count += 1
                
                # Procesar por chunks para liberar memoria de Python
                if len(current_chunk) >= chunk_size:
                    dfs.append(pl.DataFrame(current_chunk, schema=GAME_SCHEMA))
                    current_chunk = []
                    
                    elapsed = time.time() - start_time
                    speed = f"{count / elapsed:.1f}" if elapsed > 0 else "0.0"
                    progress.update(task, completed=count, speed=speed)

            # Finalización del último chunk
            if current_chunk:
                dfs.append(pl.DataFrame(current_chunk, schema=GAME_SCHEMA))
            
            elapsed = time.time() - start_time
            speed = f"{count / elapsed:.1f}" if elapsed > 0 else "0.0"
            progress.update(task, completed=count, total=count, speed=speed)

    if dfs:
        print(f"\nConcatenando {len(dfs)} bloques y escribiendo Parquet...")
        # Polars es extremadamente rápido concatenando DataFrames con el mismo esquema
        final_df = pl.concat(dfs)
        final_df.write_parquet(output_path)
        print(f"Base de datos generada con éxito en: {output_path}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Uso: uv run src/converter.py <archivo.pgn> [archivo.parquet]")
        sys.exit(1)
    
    input_pgn = sys.argv[1]
    output_parquet = sys.argv[2] if len(sys.argv) > 2 else input_pgn.replace(".pgn", ".parquet")
    
    convert_pgn_to_parquet(input_pgn, output_parquet)