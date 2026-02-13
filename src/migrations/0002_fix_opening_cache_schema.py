from yoyo import step

__depends__ = {"0001_initial_schema"}

def migrate_db_path(conn):
    cursor = conn.cursor()
    # Solo actuamos si la columna db_hash existe (esquema viejo)
    cursor.execute("PRAGMA table_info(opening_cache)")
    columns = [info[1] for info in cursor.fetchall()]
    
    if "db_hash" in columns and "db_path" not in columns:
        cursor.execute("""
            CREATE TABLE opening_cache_new (
                db_path TEXT,
                pos_hash TEXT,
                stats_json TEXT,
                PRIMARY KEY (db_path, pos_hash)
            )
        """)
        cursor.execute("INSERT INTO opening_cache_new SELECT * FROM opening_cache")
        cursor.execute("DROP TABLE opening_cache")
        cursor.execute("ALTER TABLE opening_cache_new RENAME TO opening_cache")

steps = [
    step(migrate_db_path)
]
