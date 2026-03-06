SELECT
    name,
    type,
    default_kind,
    default_expression,
    comment
FROM system.columns
WHERE database = %(database)s
  AND table = %(table)s
ORDER BY position
