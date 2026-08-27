SELECT
    n.nspname AS schema_name,
    t.relname AS table_name,
    c.conname AS constraint_name,
    c.contype AS constraint_type
FROM pg_constraint c
         JOIN pg_class t      ON t.oid = c.conrelid
         JOIN pg_namespace n  ON n.oid = t.relnamespace
WHERE c.conname ILIKE '%terms%'
ORDER BY n.nspname, t.relname, c.conname;

SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE indexname ILIKE '%termsof%'
   OR indexdef  ILIKE '%termsof%'
ORDER BY schemaname, tablename, indexname;