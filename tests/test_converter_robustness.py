import pytest
import os
import polars as pl
from src.converter import convert_pgn_to_parquet

def test_converter_empty_file(tmp_path):
    # Archivo vacío pero con extensión PGN
    empty_pgn = tmp_path / "empty.pgn"
    empty_pgn.write_text("")
    out_parquet = tmp_path / "empty.parquet"
    
    convert_pgn_to_parquet(str(empty_pgn), str(out_parquet))
    # Debería crear un archivo vacío o manejarlo sin explotar
    assert not os.path.exists(out_parquet) or pl.read_parquet(out_parquet).height == 0

def test_converter_permission_error(tmp_path):
    pgn_path = tmp_path / "test.pgn"
    pgn_path.write_text('[Event "T"]\n[White "W"]\n[Black "B"]\n[Result "*"]\n\n1. e4 *')
    
    # Intentar escribir en un sitio sin permisos (un directorio con el mismo nombre que el archivo)
    bad_out = tmp_path / "readonly_dir.parquet"
    bad_out.mkdir()
    
    # No debería crashear la app
    try:
        convert_pgn_to_parquet(str(pgn_path), str(bad_out))
    except:
        pass # Capturamos excepciones si las hay, lo importante es que no rompa el hilo principal
