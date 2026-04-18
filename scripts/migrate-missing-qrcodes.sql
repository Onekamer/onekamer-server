-- Script de rattrapage : créer les QR codes manquants pour les événements payés
-- À exécuter manuellement dans Supabase SQL Editor

-- 1. Vérifier les paiements payés sans QR correspondant
SELECT 
  ep.user_id,
  ep.event_id,
  ep.status,
  ep.amount_total,
  ep.amount_paid,
  e.title,
  e.date,
  e.end_date,
  e.end_time,
  CASE 
    WHEN eq.id IS NULL THEN 'MANQUANT'
    ELSE 'OK'
  END as qr_status
FROM public.event_payments ep
LEFT JOIN public.event_qrcodes eq ON ep.event_id = eq.event_id AND ep.user_id = eq.user_id AND eq.status = 'active'
INNER JOIN public.evenements e ON ep.event_id = e.id
WHERE ep.status = 'paid'
  AND eq.id IS NULL
ORDER BY e.title;

-- 2. Créer les QR codes manquants (uniquement pour les événements non terminés)
INSERT INTO public.event_qrcodes (user_id, event_id, qrcode_value, status, created_at)
SELECT 
  ep.user_id,
  ep.event_id,
  gen_random_uuid()::text,
  'active',
  NOW()
FROM public.event_payments ep
INNER JOIN public.evenements e ON ep.event_id = e.id
LEFT JOIN public.event_qrcodes eq ON ep.event_id = eq.event_id AND ep.user_id = eq.user_id AND eq.status = 'active'
WHERE ep.status = 'paid'
  AND eq.id IS NULL
  AND (
    -- Événement non terminé
    (e.end_date IS NOT NULL AND e.end_date::date >= CURRENT_DATE)
    OR (e.end_date IS NULL AND e.date::date >= CURRENT_DATE)
    -- Ou événement aujourd'hui (autoriser le jour même)
    OR (e.end_date::date = CURRENT_DATE OR e.date::date = CURRENT_DATE)
  );

-- 3. Vérifier le résultat
SELECT 
  eq.user_id,
  eq.event_id,
  eq.status,
  e.title,
  e.date,
  e.end_date
FROM public.event_qrcodes eq
INNER JOIN public.evenements e ON eq.event_id = e.id
WHERE eq.status = 'active'
ORDER BY e.title, eq.created_at DESC;
