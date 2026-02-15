from yoyo import step

__depends__ = {'0002_fix_opening_cache_schema'}

steps = [
    step(
        "ALTER TABLE opening_cache ADD COLUMN engine_eval REAL",
        "ALTER TABLE opening_cache DROP COLUMN engine_eval"
    )
]
