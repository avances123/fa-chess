import os
import json
import time
import polars as pl
from datetime import datetime
from config import CONFIG_FILE
from PySide6.QtCore import QObject, Signal

# Rutas del Proyecto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLIPBASE_FILE = os.path.expanduser("~/.config/fa-chess-clipbase.parquet")

class DBManager(QObject):
    # Señales para que la UI reaccione a cambios en los datos
    database_loaded = Signal(str)  # nombre de la base
    active_db_changed = Signal(str) # nombre de la base
    filter_updated = Signal(object) # DataFrame filtrado
    stats_ready = Signal(object)    # DataFrame de estadísticas

    def __init__(self):
        super().__init__()
        self.dbs = {}
        self.db_metadata = {}
        self.active_db_name = "Clipbase"
        self.current_filter_df = None # Estado del filtro actual
        
        # Caché de estadísticas de transposición
        self.stats_cache = {} 
        self.MAX_CACHE_SIZE = 100
        
        # Índice de árbol activo (DataFrame resumen)
        self.current_tree = None
        
        self.init_clipbase()

    def init_clipbase(self):
        schema = {
            "id": pl.Int64, "white": pl.String, "black": pl.String, 
            "w_elo": pl.Int64, "b_elo": pl.Int64, "result": pl.String, 
            "date": pl.String, "event": pl.String, "line": pl.String, 
            "full_line": pl.String, "fens": pl.List(pl.UInt64)
        }
        
        if os.path.exists(CLIPBASE_FILE):
            try:
                self.dbs["Clipbase"] = pl.read_parquet(CLIPBASE_FILE)
            except:
                self.dbs["Clipbase"] = pl.DataFrame(schema=schema)
        else:
            self.dbs["Clipbase"] = pl.DataFrame(schema=schema)
            
        self.db_metadata["Clipbase"] = {"read_only": False, "path": CLIPBASE_FILE}
        self.current_filter_df = None

    def save_clipbase(self):
        """Guardar la Clipbase actual en disco para persistencia"""
        if "Clipbase" in self.dbs:
            self.dbs["Clipbase"].write_parquet(CLIPBASE_FILE)

    def create_new_database(self, path):
        """Crea un archivo Parquet vacío con el esquema correcto"""
        schema = {
            "id": pl.Int64, "white": pl.String, "black": pl.String, 
            "w_elo": pl.Int64, "b_elo": pl.Int64, "result": pl.String, 
            "date": pl.String, "event": pl.String, "line": pl.String, 
            "full_line": pl.String, "fens": pl.List(pl.UInt64)
        }
        df = pl.DataFrame(schema=schema)
        df.write_parquet(path)
        return self.load_parquet(path)

    def delete_database_from_disk(self, name):
        """Elimina el archivo físico de la base de datos"""
        if name == "Clipbase": return False
        
        path = self.db_metadata.get(name, {}).get("path")
        if path and os.path.exists(path):
            try:
                os.remove(path)
                return self.remove_database(name)
            except Exception as e:
                print(f"Error al borrar archivo: {e}")
                return False
        return False

    def set_readonly(self, name, status):
        if name in self.db_metadata and name != "Clipbase":
            self.db_metadata[name]["read_only"] = status
            return True
        return False

    def load_parquet(self, path):
        name = os.path.basename(path)
        self.dbs[name] = pl.read_parquet(path)
        self.db_metadata[name] = {"read_only": True, "path": path}
        self.active_db_name = name
        self.current_filter_df = None # Reset filtro al cargar base
        
        # Intentar cargar árbol asociado
        self.load_tree(path)
        
        self.database_loaded.emit(name)
        self.active_db_changed.emit(name)
        return name

    def load_tree(self, db_path):
        """Carga el índice de árbol (.tree.parquet) en modo Lazy"""
        self.current_tree = None
        self.stats_cache.clear() # Limpiar caché para forzar uso del nuevo árbol
        if not db_path: return
        
        tree_path = db_path.replace(".parquet", ".tree.parquet")
        if os.path.exists(tree_path):
            try:
                # Mantenemos solo el plan de escaneo, no cargamos datos en RAM
                self.current_tree = pl.scan_parquet(tree_path)
                print(f"Índice de árbol conectado (Lazy): {tree_path}")
            except Exception as e:
                print(f"Error conectando árbol: {e}")

    def get_stats_from_tree(self, pos_hash):
        """Consulta instantánea al índice del árbol"""
        if self.current_tree is None: return None
        
        # Solo consultamos el árbol si NO hay filtro activo 
        if self.current_filter_df is not None: return None
        
        try:
            target = int(pos_hash)
            # Filtramos en el LazyFrame y SOLO recolectamos la fila necesaria
            res = self.current_tree.filter(pl.col("hash") == pl.lit(target, dtype=pl.UInt64)).collect()
            return res if res.height > 0 else None
        except:
            return None

    def get_current_view(self):
        """Devuelve el DataFrame filtrado actual o la base completa"""
        if self.current_filter_df is not None:
            return self.current_filter_df
        return self.get_active_df()

    def set_active_db(self, name):
        if name in self.dbs:
            self.active_db_name = name
            self.current_filter_df = None # Reset filtro
            self.stats_cache.clear()      # Limpiar caché de estadísticas
            
            # Recargar árbol de esta base
            path = self.db_metadata.get(name, {}).get("path")
            self.load_tree(path)
            
            # Emitir señales para que la UI se entere del cambio total
            self.active_db_changed.emit(name)
            self.filter_updated.emit(None) # Notificar que ya NO hay filtro

    def remove_database(self, name):
        if name == "Clipbase": return False
        if name in self.dbs: del self.dbs[name]
        if name in self.db_metadata: del self.db_metadata[name]
        if self.active_db_name == name:
            self.active_db_name = "Clipbase"
            self.current_filter_df = None
            self.active_db_changed.emit("Clipbase")
        return True

    def filter_db(self, criteria):
        df = self.dbs.get(self.active_db_name)
        if df is None: return
        
        q = df.lazy()
        if criteria.get("white"): q = q.filter(pl.col("white").str.contains(criteria["white"]))
        if criteria.get("black"): q = q.filter(pl.col("black").str.contains(criteria["black"]))
        
        if criteria.get("position_hash"):
            if "fens" in df.columns:
                target = int(criteria["position_hash"])
                q = q.filter(pl.col("fens").list.contains(pl.lit(target, dtype=pl.UInt64)))

        min_elo = criteria.get("min_elo")
        if min_elo and str(min_elo).isdigit():
            m = int(min_elo)
            q = q.filter((pl.col("w_elo") >= m) | (pl.col("b_elo") >= m))
            
        result = criteria.get("result")
        if result and result != "Cualquiera":
            q = q.filter(pl.col("result") == result)
            
        self.current_filter_df = q.collect()
        self.stats_cache.clear() # ¡CRÍTICO! Limpiar caché al cambiar filtro
        self.filter_updated.emit(self.current_filter_df)
        return self.current_filter_df

    def invert_filter(self):
        """Muestra el conjunto opuesto de partidas al filtro actual"""
        df_active = self.get_active_df()
        if df_active is None: return None
        
        if self.current_filter_df is None:
            # Si no hay filtro, el inverso es el conjunto vacío
            self.current_filter_df = df_active.clear()
        else:
            # Inverso: Base completa filtrada por los IDs que NO están en el filtro actual
            current_ids = self.current_filter_df.select("id")
            self.current_filter_df = df_active.filter(~pl.col("id").is_in(current_ids["id"]))
            
        self.stats_cache.clear() # Limpiar caché al invertir
        self.filter_updated.emit(self.current_filter_df)
        return self.current_filter_df

    def get_cached_stats(self, pos_hash):
        return self.stats_cache.get(pos_hash)

    def cache_stats(self, pos_hash, stats_df):
        if len(self.stats_cache) >= self.MAX_CACHE_SIZE:
            # Eliminar un elemento arbitrario (política simple)
            self.stats_cache.pop(next(iter(self.stats_cache)))
        self.stats_cache[pos_hash] = stats_df

    def get_stats_for_position(self, line_uci, is_white):
        # Mantenido por compatibilidad, pero StatsWorker ya no lo usa directamente
        pass

    def get_active_df(self): return self.dbs.get(self.active_db_name)
    
    def add_to_clipbase(self, game_data): 
        schema = self.dbs["Clipbase"].schema
        self.dbs["Clipbase"] = pl.concat([self.dbs["Clipbase"], pl.DataFrame([game_data], schema=schema)])
        if self.active_db_name == "Clipbase": self.current_filter_df = None
        
    def delete_game(self, db_name, game_id):
        if db_name in self.dbs: 
            self.dbs[db_name] = self.dbs[db_name].filter(pl.col("id") != game_id)
            if self.active_db_name == db_name: self.current_filter_df = None
            return True
        return False
        
    def update_game(self, db_name, game_id, new_data):
        if db_name in self.dbs:
            self.dbs[db_name] = self.dbs[db_name].with_columns([pl.when(pl.col("id") == game_id).then(pl.lit(new_data[k])).otherwise(pl.col(k)).alias(k) for k in new_data.keys()])
            if self.active_db_name == db_name: self.current_filter_df = None
            return True
        return False
        
    def get_game_by_id(self, db_name, game_id):
        df = self.dbs.get(db_name)
        if df is not None:
            res = df.filter(pl.col("id") == game_id)
            if not res.is_empty(): return res.row(0, named=True)
        return None