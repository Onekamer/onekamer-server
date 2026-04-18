-- Vérifier le problème d'affichage du prix (0,01€ au lieu de 1€)
-- Événement ID: e570e1d3-bc3d-4fd1-b0a1-eeb5479acd40

-- 1. Vérifier les données de l'événement
SELECT 
  id,
  title,
  price,
  price_amount,
  currency
FROM public.evenements
WHERE id = 'e570e1d3-bc3d-4fd1-b0a1-eeb5479acd40';

-- 2. Vérifier les données de paiement
SELECT 
  event_id,
  user_id,
  status,
  amount_total,
  amount_paid,
  currency
FROM public.event_payments
WHERE event_id = 'e570e1d3-bc3d-4fd1-b0a1-eeb5479acd40'
AND user_id = '6b39743d-186b-4655-94ac-65686b51a105';

-- 3. Vérifier ce que renvoie getPaymentSnapshot (simulation)
-- La fonction utilise amount_total depuis event_payments puis evenements
WITH payment_snapshot AS (
  SELECT 
    ep.event_id,
    ep.user_id,
    ep.amount_total AS amount_total_from_payments,
    ev.price_amount AS amount_total_from_events,
    COALESCE(ep.amount_total, ev.price_amount) AS final_amount_total,
    ep.amount_paid,
    ep.currency,
    ev.currency AS event_currency
  FROM public.event_payments ep
  INNER JOIN public.evenements ev ON ep.event_id = ev.id
  WHERE ep.event_id = 'e570e1d3-bc3d-4fd1-b0a1-eeb5479acd40'
  AND ep.user_id = '6b39743d-186b-4655-94ac-65686b51a105'
)
SELECT 
  event_id,
  user_id,
  amount_total_from_payments,
  amount_total_from_events,
  final_amount_total,
  amount_paid,
  CASE 
    WHEN amount_paid >= final_amount_total AND final_amount_total > 0 THEN 'paid'
    WHEN amount_paid > 0 THEN 'deposit_paid'
    WHEN final_amount_total <= 0 OR final_amount_total IS NULL THEN 'free'
    ELSE 'unpaid'
  END AS calculated_status
FROM payment_snapshot;
