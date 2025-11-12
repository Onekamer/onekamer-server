# 🟢 OneKamer – Backend API (PRODUCTION)

## 🌍 Description
API **Node.js / Express** hébergée sur **Render**, connectée à **Supabase**, **Stripe** et **BunnyCDN**.  
Ce serveur gère toutes les interactions sécurisées entre le front OneKamer.co et les services externes, notamment :

- 💳 Gestion des paiements et abonnements **Stripe**
- 🧾 Synchronisation des profils et plans dans **Supabase**
- 🪙 Gestion automatique des **OK COINS**
- 🖼️ Intégration des médias via **BunnyCDN**
- 🛡️ Webhooks sécurisés et validation des événements Stripe
- ⚙️ RPC Supabase : `upsert_subscription_from_stripe()` et `apply_plan_to_profile()`

---

## 🧠 Architecture & Environnement

| Composant | Technologie | Hébergement |
|------------|-------------|--------------|
| Backend API | Node.js / Express | Render |
| Base de données | Supabase (PostgreSQL) | Supabase Cloud |
| Paiement | Stripe (Checkout + Webhook) | Render |
| Stockage médias | BunnyCDN (Edge Storage + CDN) | Bunny.net |
| Sécurité | RLS + Policies | Supabase |

---

## ⚙️ Variables d’environnement

Les variables suivantes doivent être définies dans Render :

```bash
SUPABASE_URL=<ton_supabase_url>
SUPABASE_SERVICE_ROLE_KEY=<ta_cle_service_role>
STRIPE_SECRET_KEY=<ta_cle_stripe_live>
STRIPE_WEBHOOK_SECRET=<ta_cle_webhook_stripe>
FRONTEND_URL=https://onekamer.co
BUNNY_API_KEY=<ta_cle_bunny>
BUNNY_STORAGE_ZONE=<ta_zone_storage>
BUNNY_CDN_URL=https://onekamer-media-cdn.b-cdn.net
PORT=10000

## 🧩 Fonctionnalités clés

- Vérification automatique de la signature Stripe ✅  
- Synchronisation des abonnements Supabase ↔ Stripe 🧾  
- Attribution dynamique des accès via `plan_features` 🔑  
- Gestion complète des événements Stripe (`stripe_events`, `stripe_events_log`) 📊  
- Stockage et diffusion des médias via **BunnyCDN** 🌍  
- Support des achats OK COINS 💰  

🚀 Routes principales

| Méthode | Route                      | Description                                                       |
| ------- | -------------------------- | ----------------------------------------------------------------- |
| `POST`  | `/create-checkout-session` | Crée une session Stripe Checkout                                  |
| `POST`  | `/activate-free-plan`      | Active un plan gratuit utilisateur                                |
| `POST`  | `/webhook`                 | Webhook Stripe (paiements & abonnements)                          |
| `GET`   | `/fix-partenaire-images`   | (Maintenance) Correction automatique des images partenaires Bunny |

## 🧰 Commandes utiles

# Installation des dépendances
npm install

# Lancement du serveur (production)
npm start



👨🏽‍💻 Auteurs
Développé par William Soppo & Annaëlle Bilounga
© 2025 OneKamer SAS — Tous droits réservés.

## 🧾 Licence
**Propriété privée – Usage exclusif de OneKamer SAS.**  
Toute reproduction ou diffusion non autorisée du code est strictement interdite.

