-- List foreign keys on event_qrcodes and event_payments with full definitions
SELECT
  c.conname AS constraint_name,
  t.relname AS table_name,
  rt.relname AS referenced_table,
  pg_get_constraintdef(c.oid) AS definition
FROM pg_constraint c
JOIN pg_class t ON t.oid = c.conrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN pg_class rt ON rt.oid = c.confrelid
JOIN pg_namespace rn ON rn.oid = rt.relnamespace
WHERE c.contype = 'f'
  AND n.nspname = 'public'
  AND t.relname IN ('event_qrcodes','event_payments')
ORDER BY t.relname, c.conname;

-- Using information_schema to include delete_rule/update_rule
SELECT
  rc.constraint_name,
  tc.table_name,
  kcu.column_name,
  ccu.table_name AS referenced_table,
  ccu.column_name AS referenced_column,
  rc.update_rule,
  rc.delete_rule
FROM information_schema.referential_constraints rc
JOIN information_schema.table_constraints tc
  ON rc.constraint_name = tc.constraint_name AND rc.constraint_schema = tc.constraint_schema
JOIN information_schema.key_column_usage kcu
  ON kcu.constraint_name = tc.constraint_name AND kcu.constraint_schema = tc.constraint_schema
JOIN information_schema.constraint_column_usage ccu
  ON ccu.constraint_name = tc.constraint_name AND ccu.constraint_schema = tc.constraint_schema
WHERE tc.table_schema = 'public'
  AND tc.table_name IN ('event_qrcodes','event_payments')
ORDER BY tc.table_name, rc.constraint_name;

-- Orphan checks (by LEFT JOIN)
SELECT COUNT(*) AS qrcodes_orphelins
FROM public.event_qrcodes q
LEFT JOIN public.evenements e ON e.id = q.event_id
WHERE q.event_id IS NOT NULL AND e.id IS NULL;

SELECT COUNT(*) AS payments_orphelins
FROM public.event_payments p
LEFT JOIN public.evenements e ON e.id = p.event_id
WHERE p.event_id IS NOT NULL AND e.id IS NULL;

-- Sample rows (if any)
SELECT q.* FROM public.event_qrcodes q
LEFT JOIN public.evenements e ON e.id = q.event_id
WHERE q.event_id IS NOT NULL AND e.id IS NULL
ORDER BY q.created_at DESC
LIMIT 50;

SELECT p.* FROM public.event_payments p
LEFT JOIN public.evenements e ON e.id = p.event_id
WHERE p.event_id IS NOT NULL AND e.id IS NULL
ORDER BY p.created_at DESC
LIMIT 50;
