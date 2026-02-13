import sqlite3
import os
import io
import json
import polars as pl
from src.config import logger
from yoyo import read_migrations, get_backend

class AppDBManager:
    def __init__(self, db_path):
        self.db_path = db_path
        logger.info(f"AppDB: Gestionando base de datos en {db_path}")
        self.run_migrations()

    def run_migrations(self):
        """Ejecuta las migraciones usando yoyo-migrations"""
        try:
            # Localizamos la carpeta de migraciones (relativa a este archivo)
            migrations_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'migrations')
            
            # Configuramos el backend de yoyo para SQLite
            backend = get_backend(f"sqlite:///{self.db_path}")
            migrations = read_migrations(migrations_dir)
            
            # Aplicamos todas las migraciones pendientes
            if migrations:
                logger.info(f"AppDB: Aplicando {len(migrations)} migraciones pendientes...")
                with backend.lock():
                    backend.apply_migrations(backend.to_apply(migrations))
                logger.info("AppDB: Migraciones completadas con éxito")
            else:
                logger.debug("AppDB: No hay migraciones pendientes")
                
        except Exception as e:
            logger.error(f"AppDB: Error crítico en migraciones: {e}")
            # Si hay un error grave en la base de datos de la app (bloqueos, corrupción),
            # avisamos pero permitimos que la app intente seguir sin cache.
            raise e

    def get_connection(self):
        return sqlite3.connect(self.db_path)

    # --- MÉTODOS DE CONFIGURACIÓN ---
    def set_config(self, key, value):
        """Guarda un valor de configuración (lo serializa a JSON)"""
        with self.get_connection() as conn:
            conn.execute("INSERT OR REPLACE INTO app_config (key, value) VALUES (?, ?)",
                         (key, json.dumps(value)))

    def get_config(self, key, default=None):
        """Recupera un valor de configuración"""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("SELECT value FROM app_config WHERE key = ?", (key,))
                row = cursor.fetchone()
                return json.loads(row[0]) if row else default
        except:
            return default

    # --- MÉTODOS PARA PUZZLES ---
    def save_puzzle_status(self, puzzle_id, status):
        with self.get_connection() as conn:
            conn.execute("INSERT OR REPLACE INTO puzzle_stats (puzzle_id, status) VALUES (?, ?)", 
                         (str(puzzle_id), status))

    def get_all_puzzle_stats(self):
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("SELECT puzzle_id, status FROM puzzle_stats")
                return {row[0]: row[1] for row in cursor.fetchall()}
        except: return {}

    # --- MÉTODOS PARA ÁRBOL DE APERTURA ---
    def save_opening_stats(self, db_path, pos_hash, stats_df):
        try:
            stats_json = stats_df.write_json()
            with self.get_connection() as conn:
                conn.execute("INSERT OR REPLACE INTO opening_cache (db_path, pos_hash, stats_json) VALUES (?, ?, ?)",
                             (db_path, str(pos_hash), stats_json))
        except Exception as e:
            logger.error(f"AppDB: Error al guardar caché: {e}")

    def get_opening_stats(self, db_path, pos_hash):
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("SELECT stats_json FROM opening_cache WHERE db_path = ? AND pos_hash = ?", 
                                     (db_path, str(pos_hash)))
                row = cursor.fetchone()
                if row:
                    return pl.read_json(io.BytesIO(row[0].encode()))
        except Exception as e:
            logger.error(f"AppDB: Error al leer caché: {e}")
        return None
