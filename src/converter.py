import chess.pgn
import chess.polyglot
import polars as pl
import os
import time
import io
import tempfile
import shutil
from concurrent.futures import ProcessPoolExecutor
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn

# Esquema unificado
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

def extract_game_data(count, game):
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
        "id": count, "white": headers.get("White", "Unknown"), "black": headers.get("Black", "Unknown"),
        "w_elo": safe_int(headers.get("WhiteElo")), "b_elo": safe_int(headers.get("BlackElo")),
        "result": headers.get("Result", "*"), "date": headers.get("Date", "????.??.??"),
        "event": headers.get("Event", "?"), "site": headers.get("Site", ""),
        "line": " ".join(uci_moves[:12]), "full_line": " ".join(uci_moves), "fens": hashes 
    }

def process_pgn_chunk_to_parquet(args):
    """Función de trabajador: procesa texto y guarda un archivo parquet temporal"""
    chunk_str, start_id, temp_dir, chunk_index = args
    pgn_io = io.StringIO(chunk_str)
    results = []
    current_id = start_id
    
    while True:
        game = chess.pgn.read_game(pgn_io)
        if game is None: break
        results.append(extract_game_data(current_id, game))
        current_id += 1
    
    if results:
        # Guardamos este bloque inmediatamente a disco para liberar RAM
        chunk_path = os.path.join(temp_dir, f"chunk_{chunk_index:06d}.parquet")
        pl.DataFrame(results, schema=GAME_SCHEMA).write_parquet(chunk_path)
        return chunk_path
    return None

def count_games_fast(pgn_path):
    count = 0
    with open(pgn_path, "rb") as f:
        for line in f:
            if line.startswith(b"[Event "): count += 1
    return count

def convert_pgn_to_parquet(pgn_path, output_path, max_games=100000000, chunk_size=5000, progress_callback=None, workers=None):
    start_time = time.time()
    num_workers = workers if workers else os.cpu_count()
    
    # 1. Preparación de entorno temporal
    total_games_file = count_games_fast(pgn_path)
    total_to_process = min(total_games_file, max_games)
    
    temp_work_dir = tempfile.mkdtemp(prefix="fa_chess_conv_")
    print(f"Directorio temporal: {temp_work_dir}")

    # 2. Troceado de archivo (Stream de texto)
    chunks_args = []
    game_count = 0
    chunk_index = 0
    
    with open(pgn_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = []
        for line in f:
            if line.startswith("[Event ") and lines:
                game_count += 1
                if game_count % chunk_size == 0:
                    chunks_args.append(("".join(lines), game_count - chunk_size, temp_work_dir, chunk_index))
                    lines = []
                    chunk_index += 1
                if game_count >= total_to_process: break
            lines.append(line)
        if lines and game_count < total_to_process:
            chunks_args.append(("".join(lines), (game_count // chunk_size) * chunk_size, temp_work_dir, chunk_index))

    # 3. Procesamiento Paralelo
    parquet_files = []
    processed_count = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[blue]{task.completed}/{task.total} partidas"),
        TextColumn("[green]{task.fields[speed]} p/s"),
        TimeElapsedColumn(),
    ) as progress:
        
        task = progress.add_task(f"Analizando en {num_workers} hilos...", total=total_to_process, speed="0")
        
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            for chunk_path in executor.map(process_pgn_chunk_to_parquet, chunks_args):
                if chunk_path:
                    parquet_files.append(chunk_path)
                    processed_count += chunk_size # Aproximado para la barra
                    if processed_count > total_to_process: processed_count = total_to_process
                    
                    elapsed = time.time() - start_time
                    speed = f"{processed_count / elapsed:.0f}" if elapsed > 0 else "0"
                    
                    progress.update(task, completed=processed_count, speed=speed)
                    if progress_callback:
                        progress_callback(int((processed_count / total_to_process) * 100))

    # 4. Fusión Final con Polars Streaming (La clave para la RAM)
    if parquet_files:
        print(f"\nFusionando {len(parquet_files)} bloques en el archivo final...")
        # Polars puede leer múltiples parquets y unirlos sin cargar todo a la vez
        pl.scan_parquet(os.path.join(temp_work_dir, "*.parquet")).collect(streaming=True).write_parquet(output_path)
        
    # Limpieza
    shutil.rmtree(temp_work_dir)
    
    end_time = time.time()
    final_count = total_to_process
    print(f"\n¡Éxito! Procesadas {final_count} partidas en {end_time - start_time:.1f}s.")
    print(f"Velocidad media: {final_count/(end_time - start_time):.0f} partidas/s")

from src.config import GAME_SCHEMA, logger

def convert_lichess_puzzles(csv_path, output_path):
    """
    Convierte los 5.7M de puzzles de Lichess a Parquet conservando toda la metadata.
    """
    logger.info(f"Conversor: Leyendo CSV masivo de Lichess: {csv_path}")
    start_time = time.time()
    
    try:
        # Usamos scan para procesar de forma eficiente
        df = pl.scan_csv(csv_path).select([
            pl.col("PuzzleId"),
            pl.col("FEN"),
            pl.col("Moves"),
            pl.col("Rating").cast(pl.Int32),
            pl.col("Popularity").cast(pl.Int16),
            pl.col("Themes"),
            pl.col("OpeningTags").fill_null("")
        ])

        logger.info("Conversor: Escribiendo archivo Parquet de alta velocidad (Streaming)...")
        df.collect(streaming=True).write_parquet(output_path)
        
        logger.info(f"Conversor: Éxito! {output_path} generado en {time.time() - start_time:.1f}s")
    except Exception as e:
        logger.error(f"Conversor: Fallo crítico en la conversión: {e}")
        raise e

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Uso: fa-chess-convert <archivo.pgn> <archivo.parquet>")
        sys.exit(1)
    convert_pgn_to_parquet(sys.argv[1], sys.argv[2])
