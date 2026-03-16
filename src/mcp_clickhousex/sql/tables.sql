SELECT
    name,
    engine,
    primary_key,
    sorting_key,
    partition_key,
    total_rows,
    total_bytes
FROM system.tables
WHERE database = %(database)s
ORDER BY name
