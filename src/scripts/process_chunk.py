import sys
import polars as pl
import argparse
import os

def process_chunk(db_path, start_row, length, temp_dir, chunk_num):
    try:
        # Leer bloque usando scan_parquet + slice + collect para eficiencia
        chunk = pl.scan_parquet(db_path).slice(start_row, length).collect()
        
        # Generar estadísticas parciales
        chunk_tree = chunk.select([
            pl.col("fens").list.slice(0, pl.col("fens").list.len() - 1).alias("hash"),
            pl.col("full_line").str.split(" ").alias("uci"),
            "result", "w_elo", "b_elo"
        ]).explode(["hash", "uci"]).filter(pl.col("uci").is_not_null()).group_by(["hash", "uci"]).agg([
            pl.len().alias("c"),
            pl.col("result").filter(pl.col("result") == "1-0").len().alias("w"),
            pl.col("result").filter(pl.col("result") == "1/2-1/2").len().alias("d"),
            pl.col("result").filter(pl.col("result") == "0-1").len().alias("b"),
            pl.col("w_elo").sum().alias("s_w_elo"),
            pl.col("b_elo").sum().alias("s_b_elo")
        ])
        
        # PARTICIONADO: Dividir en 16 cubos basados en el hash
        chunk_tree = chunk_tree.with_columns(
            (pl.col("hash") % 16).alias("bucket")
        )
        
        for b in range(16):
            bucket_df = chunk_tree.filter(pl.col("bucket") == b).drop("bucket")
            if bucket_df.height > 0:
                out_path = os.path.join(temp_dir, f"b{b}_p{chunk_num}.parquet")
                bucket_df.write_parquet(out_path)
                
    except Exception as e:
        print(f"Error procesando chunk {chunk_num}: {e}", file=sys.stderr)
        sys.exit(1)

def merge_bucket(temp_dir, bucket_idx, out_part_path):
    try:
        files = os.path.join(temp_dir, f"b{bucket_idx}_*.parquet")
        # scan_parquet maneja el wildcard perfectamente
        pl.scan_parquet(files).group_by(["hash", "uci"]).agg([
            pl.sum("c"), pl.sum("w"), pl.sum("d"), pl.sum("b"),
            pl.sum("s_w_elo"), pl.sum("s_b_elo")
        ]).filter(pl.col("c") > 1).with_columns([
            (pl.col("s_w_elo") / pl.col("c")).cast(pl.Int32).alias("avg_w_elo"),
            (pl.col("s_b_elo") / pl.col("c")).cast(pl.Int32).alias("avg_b_elo")
        ]).drop(["s_w_elo", "s_b_elo"]).sort(["hash", "c"], descending=[False, True]).collect().write_parquet(out_part_path)
    except Exception as e:
        # Si no hay archivos para ese cubo, simplemente ignoramos
        pass

def final_merge(bucket_results_dir, out_path):
    try:
        files = os.path.join(bucket_results_dir, "*.parquet")
        pl.scan_parquet(files).sink_parquet(out_path)
    except Exception as e:
        print(f"Error en merge final: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["process", "merge_bucket", "final_merge"], default="process")
    parser.add_argument("--db")
    parser.add_argument("--start", type=int)
    parser.add_argument("--len", type=int)
    parser.add_argument("--temp_dir")
    parser.add_argument("--chunk_num", type=int)
    parser.add_argument("--bucket", type=int)
    parser.add_argument("--out")
    args = parser.parse_args()
    
    if args.mode == "process":
        process_chunk(args.db, args.start, args.len, args.temp_dir, args.chunk_num)
    elif args.mode == "merge_bucket":
        merge_bucket(args.temp_dir, args.bucket, args.out)
    elif args.mode == "final_merge":
        final_merge(args.temp_dir, args.out)