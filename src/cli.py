import argparse
import sys
import os
from src.converter import convert_pgn_to_parquet, convert_lichess_puzzles

def main():
    parser = argparse.ArgumentParser(description="Convierte ficheros PGN o Puzzles a formato Parquet.")
    parser.add_argument("input", help="Fichero de entrada (PGN o CSV)")
    parser.add_argument("output", help="Fichero Parquet de salida")
    parser.add_argument("--max", type=int, default=None, help="Máximo de partidas")
    parser.add_argument("--workers", type=int, default=None, help="Número de núcleos")
    parser.add_argument("--puzzles", action="store_true", help="Importar CSV de Puzzles de Lichess")

    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"Error: No existe {args.input}")
        sys.exit(1)

    try:
        if args.puzzles:
            convert_lichess_puzzles(args.input, args.output)
        else:
            max_val = args.max if args.max is not None else 999999999
            convert_pgn_to_parquet(args.input, args.output, max_games=max_val, workers=args.workers)
    except KeyboardInterrupt:
        print("\nConversión cancelada por el usuario.")
        sys.exit(1)
    except Exception as e:
        print(f"\nError durante la conversión: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()