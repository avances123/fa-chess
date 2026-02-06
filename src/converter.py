import chess.pgn
import polars as pl
import os
import time
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn

def count_games(pgn_path):
    """Cuenta rápidamente las partidas buscando el tag [Event ]"""
    count = 0
    with open(pgn_path, "rb") as f:
        for line in f:
            if line.startswith(b"[Event "):
                count += 1
    return count

def convert_pgn_to_parquet(pgn_path, output_path, max_games=1000000):
    """
    Convierte un PGN a Parquet con total estático.
    """
    # Contamos primero para tener un total real y estático
    real_total = count_games(pgn_path)
    total_to_process = min(real_total, max_games)
    
    games_data = []

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
                
                headers = game.headers
                moves = []
                node = game
                for _ in range(12):
                    if not node.variations: break
                    node = node.variation(0)
                    moves.append(node.move.uci())
                
                games_data.append({
                    "id": count,
                    "white": headers.get("White", "Unknown"),
                    "black": headers.get("Black", "Unknown"),
                    "w_elo": int(headers.get("WhiteElo", "0") if headers.get("WhiteElo", "0").isdigit() else 0),
                    "b_elo": int(headers.get("BlackElo", "0") if headers.get("BlackElo", "0").isdigit() else 0),
                    "result": headers.get("Result", "*"),
                    "date": headers.get("Date", "????.??.??"),
                    "event": headers.get("Event", "?"),
                    "line": " ".join(moves),
                    "full_line": " ".join([m.uci() for m in game.mainline_moves()])
                })
                
                count += 1
                if count % 200 == 0:
                    elapsed = time.time() - start_time
                    speed = f"{count / elapsed:.1f}" if elapsed > 0 else "0.0"
                    progress.update(task, completed=count, speed=speed)

            # Finalización
            elapsed = time.time() - start_time
            speed = f"{count / elapsed:.1f}" if elapsed > 0 else "0.0"
            progress.update(task, completed=count, total=count, speed=speed)

    if games_data:
        pl.DataFrame(games_data).write_parquet(output_path)
