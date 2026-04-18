-- Vérifier si la suppression d'un événement supprime aussi les paiements et QR codes
-- Exécuter dans Supabase SQL Editor

-- 1. Contraintes sur event_payments (clé étrangère vers evenements)
SELECT 
  tc.constraint_name,
  tc.constraint_type,
  kcu.column_name,
  ccu.table_name AS foreign_table_name,
  ccu.column_name AS foreign_column_name,
  rc.delete_rule  -- CASCADE, SET NULL, RESTRICT, NO ACTION
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
  ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu
  ON tc.constraint_name = ccu.constraint_name
JOIN information_schema.referential_constraints rc
  ON tc.constraint_name = rc.constraint_name
WHERE tc.table_name = 'event_payments'
  AND tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = 'public';

-- 2. Contraintes sur event_qrcodes (clé étrangère vers evenements)
SELECT 
  tc.constraint_name,
  tc.constraint_type,
  kcu.column_name,
  ccu.table_name AS foreign_table_name,
  ccu.column_name AS foreign_column_name,
  rc.delete_rule
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
  ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu
  ON tc.constraint_name = ccu.constraint_name
JOIN information_schema.referential_constraints rc
  ON tc.constraint_name = rc.constraint_name
WHERE tc.table_name = 'event_qrcodes'
  AND tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = 'public';

-- 3. Vérifier s'il y a des données existantes pour tester
SELECT 
  (SELECT COUNT(*) FROM public.evenements) as evenements_count,
  (SELECT COUNT(*) FROM public.event_payments) as payments_count,
  (SELECT COUNT(*) FROM public.event_qrcodes) as qrcodes_count;

-- 4. Simulation de suppression (commentée pour sécurité)
-- Décommenter pour tester sur un événement de test
/*
BEGIN;
-- Créer un événement de test
INSERT INTO public.evenements (id, title, date, author_id, created_at)
VALUES ('test-delete-event', 'Test Delete Event', CURRENT_DATE, 'test-user', NOW());

-- Créer paiement et QR pour cet événement
INSERT INTO public.event_payments (event_id, user_id, status, amount_total, amount_paid, created_at)
VALUES ('test-delete-event', 'test-user', 'paid', 10, 10, NOW());

INSERT INTO public.event_qrcodes (event_id, user_id, qrcode_value, status, created_at)
VALUES ('test-delete-event', 'test-user', 'test-qr-value', 'active', NOW());

-- Vérifier que tout est créé
SELECT 'Before delete' as step, COUNT(*) as count FROM public.event_payments WHERE event_id = 'test-delete-event';
SELECT 'Before delete' as step, COUNT(*) as count FROM public.event_qrcodes WHERE event_id = 'test-delete-event';

-- Supprimer l'événement
DELETE FROM public.evenements WHERE id = 'test-delete-event';

-- Vérifier ce qui reste
SELECT 'After delete' as step, COUNT(*) as count FROM public.event_payments WHERE event_id = 'test-delete-event';
SELECT 'After delete' as step, COUNT(*) as count FROM public.event_qrcodes WHERE event_id = 'test-delete-event';

ROLLBACK;
*/
