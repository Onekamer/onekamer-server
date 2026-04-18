-- Vérifier si le webhook Stripe a bien été reçu pour l'événement test
-- Événement ID: e570e1d3-bc3d-4fd1-b0a1-eeb5479acd40

SELECT 
  category,
  action,
  status,
  context,
  created_at
FROM public.logs 
WHERE context->>'event_id' = 'e570e1d3-bc3d-4fd1-b0a1-eeb5479acd40'
  AND category = 'event_payment'
ORDER BY created_at DESC;

-- Vérifier aussi tous les webhooks reçus récemment
SELECT 
  category,
  action,
  status,
  context,
  created_at
FROM public.logs 
WHERE category = 'event_payment'
  AND action = 'pi.succeeded'
ORDER BY created_at DESC
LIMIT 10;

-- Vérifier si le payment_intent_id existe dans les logs
SELECT DISTINCT 
  context->>'payment_intent_id' as payment_intent_id,
  COUNT(*) as count
FROM public.logs 
WHERE category = 'event_payment'
  AND context->>'payment_intent_id' IS NOT NULL
GROUP BY context->>'payment_intent_id'
ORDER BY count DESC;
