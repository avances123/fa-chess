from yoyo import step

__depends__ = {}

steps = [
    step(
        "CREATE TABLE app_config (key TEXT PRIMARY KEY, value TEXT)",
        "DROP TABLE app_config"
    ),
    step(
        "CREATE TABLE IF NOT EXISTS puzzle_stats (puzzle_id TEXT PRIMARY KEY, status TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)",
        "DROP TABLE puzzle_stats"
    ),
    step(
        "CREATE TABLE IF NOT EXISTS opening_cache (db_hash TEXT, pos_hash TEXT, stats_json TEXT, PRIMARY KEY (db_hash, pos_hash))",
        "DROP TABLE opening_cache"
    )
]
