-- No system-database filter (system, information_schema, …):
--   Other tools reference Src: databases for their database parameter.
--   Excluding system databases would prevent agents from discovering
--   and querying diagnostic tables (query_log, parts, etc.).
--
-- No fuzzy/LIKE filter on name:
--   The result set is typically 3–10 rows, so filtering adds complexity
--   without meaningful benefit. LLM agents reason better over complete,
--   deterministic lists than partial matches at the discovery layer.
SELECT
    name,
    engine
FROM system.databases
ORDER BY name
