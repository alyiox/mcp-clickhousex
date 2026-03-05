SELECT
    name,
    engine,
    total_rows,
    total_bytes
FROM system.tables
WHERE database = %(database)s
ORDER BY name
