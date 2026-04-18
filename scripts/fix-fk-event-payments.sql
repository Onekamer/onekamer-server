-- Plan de correction FK pour event_payments
-- Objectif: empêcher de nouveaux orphelins tout en conservant l'historique
-- Choix: ON DELETE SET NULL sur event_payments.event_id -> evenements(id)

BEGIN;

-- 0) Autoriser event_id à être NULL pour conserver l'historique quand un événement est supprimé
ALTER TABLE public.event_payments
  ALTER COLUMN event_id DROP NOT NULL;

-- 1) Nettoyer les orphelins existants (mettre event_id = NULL si l'événement n'existe plus)
UPDATE public.event_payments p
SET event_id = NULL
WHERE p.event_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM public.evenements e WHERE e.id = p.event_id
  );

-- 2) Créer/Remplacer la contrainte FK sur event_payments.event_id -> evenements(id) en ON DELETE SET NULL
DO $$
DECLARE
  rec RECORD;
BEGIN
  SELECT c.conname AS name
  INTO rec
  FROM pg_constraint c
  JOIN pg_class t ON t.oid = c.conrelid
  JOIN pg_namespace n ON n.oid = t.relnamespace
  WHERE c.contype = 'f' AND n.nspname = 'public' AND t.relname = 'event_payments'
  LIMIT 1;

  IF rec.name IS NOT NULL THEN
    -- Si la règle de suppression n'est pas SET NULL, on remplace la contrainte
    IF EXISTS (
      SELECT 1
      FROM information_schema.referential_constraints rc
      WHERE rc.constraint_name = rec.name AND rc.constraint_schema = 'public' AND rc.delete_rule <> 'SET NULL'
    ) THEN
      EXECUTE 'ALTER TABLE public.event_payments DROP CONSTRAINT ' || rec.name;
      EXECUTE 'ALTER TABLE public.event_payments
               ADD CONSTRAINT fk_event_payments_event_id
               FOREIGN KEY (event_id) REFERENCES public.evenements(id)
               ON DELETE SET NULL';
    END IF;
  ELSE
    -- Aucune FK trouvée: la créer
    EXECUTE 'ALTER TABLE public.event_payments
             ADD CONSTRAINT fk_event_payments_event_id
             FOREIGN KEY (event_id) REFERENCES public.evenements(id)
             ON DELETE SET NULL';
  END IF;
END $$;

-- 3) S'assurer que event_qrcodes.event_id -> evenements(id) est bien ON DELETE CASCADE
--    Si une FK existe mais avec un delete_rule différent, la récréer avec CASCADE.
DO $$
DECLARE
  rec RECORD;
BEGIN
  SELECT
    c.conname AS name
  INTO rec
  FROM pg_constraint c
  JOIN pg_class t ON t.oid = c.conrelid
  JOIN pg_namespace n ON n.oid = t.relnamespace
  WHERE c.contype = 'f' AND n.nspname = 'public' AND t.relname = 'event_qrcodes'
  LIMIT 1;

  IF rec.name IS NOT NULL THEN
    -- Vérifier le delete_rule
    IF EXISTS (
      SELECT 1
      FROM information_schema.referential_constraints rc
      WHERE rc.constraint_name = rec.name AND rc.constraint_schema = 'public' AND rc.delete_rule <> 'CASCADE'
    ) THEN
      EXECUTE 'ALTER TABLE public.event_qrcodes DROP CONSTRAINT ' || rec.name;
      EXECUTE 'ALTER TABLE public.event_qrcodes
               ADD CONSTRAINT fk_event_qrcodes_event_id
               FOREIGN KEY (event_id) REFERENCES public.evenements(id)
               ON DELETE CASCADE';
    END IF;
  ELSE
    -- Aucune FK trouvée: la créer
    EXECUTE 'ALTER TABLE public.event_qrcodes
             ADD CONSTRAINT fk_event_qrcodes_event_id
             FOREIGN KEY (event_id) REFERENCES public.evenements(id)
             ON DELETE CASCADE';
  END IF;
END $$;

COMMIT;
