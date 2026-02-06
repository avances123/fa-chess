import argparse
import sys
import os
from src.converter import convert_pgn_to_parquet

def main():
    parser = argparse.ArgumentParser(description="Convierte ficheros PGN a formato Parquet para fa-chess.")
    parser.add_argument("input", help="Ruta al fichero PGN de entrada")
    parser.add_argument("output", help="Ruta al fichero Parquet de salida")
    parser.add_argument("--max", type=int, default=None, help="Número máximo de partidas a procesar (por defecto: todas)")

    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"Error: El fichero de entrada '{args.input}' no existe.")
        sys.exit(1)

    # Si max es None, pasamos un número muy grande o manejamos en el convertidor
    max_val = args.max if args.max is not None else 999999999
    
    try:
        convert_pgn_to_parquet(args.input, args.output, max_games=max_val)
    except KeyboardInterrupt:
        print("\nConversión cancelada por el usuario.")
        sys.exit(1)
    except Exception as e:
        print(f"\nError durante la conversión: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()