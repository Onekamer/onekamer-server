// ============================================================
// OneKamer - Serveur Stripe + Supabase (OK COINS + Abonnements)
// ============================================================

// Charger les variables d'environnement depuis .env.local
import * as dotenv from "dotenv";
dotenv.config({ path: ".env.local" });

// Importer les modules nécessaires
import express from "express";
import Stripe from "stripe";
import bodyParser from "body-parser";
import cors from "cors";
import { createClient } from "@supabase/supabase-js";

// Initialiser Express
const app = express();

// Autoriser ton front Horizon / OneKamer à appeler l’API
app.use(
  cors({
    origin: [process.env.FRONTEND_URL || "https://onekamer.co"],
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);

// Initialiser Stripe avec la clé secrète
const stripe = new Stripe(process.env.STRIPE_SECRET_KEY, {
  apiVersion: "2024-06-20",
});

// Initialiser le client Supabase
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// ============================================================
// 1️⃣ Webhook Stripe (OK COINS + Abonnements)
// ============================================================

app.post("/webhook", bodyParser.raw({ type: "application/json" }), async (req, res) => {
  const sig = req.headers["stripe-signature"];
  const endpointSecret = process.env.STRIPE_WEBHOOK_SECRET;
  let event;

  try {
    event = stripe.webhooks.constructEvent(req.body, sig, endpointSecret);
  } catch (err) {
    console.error("❌ Webhook verification failed:", err.message);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  console.log("📦 Événement Stripe reçu :", event.type);

  try {
    // =========================================================
    // (A) Paiement OK COINS
    // =========================================================
    if (event.type === "checkout.session.completed") {
      const session = event.data.object;
      const { userId, packId, planKey } = session.metadata || {};

      // Cas 1 : Achat OK COINS
      if (packId) {
        // Vérifie si déjà traité
        const { error: evtErr } = await supabase
          .from("stripe_events")
          .insert({ event_id: event.id });
        if (evtErr && evtErr.code === "23505") {
          console.log("🔁 Événement déjà traité :", event.id);
          return res.json({ received: true });
        }

        // Crédite les OK COINS
        const { data, error } = await supabase.rpc("okc_grant_pack_after_payment", {
          p_user: userId,
          p_pack_id: parseInt(packId, 10),
          p_status: "paid",
        });

        if (error) {
          console.error("❌ Erreur RPC Supabase (OK COINS):", error);
        } else {
          console.log("✅ OK COINS crédités :", data);
        }
      }

      // Cas 2 : Abonnement Stripe (Standard / VIP)
      if (session.mode === "subscription" && planKey) {
        const subscription = await stripe.subscriptions.retrieve(session.subscription);
        const priceId = subscription.items.data[0]?.price?.id ?? null;
        const currentPeriodEnd = new Date(subscription.current_period_end * 1000).toISOString();
        const cancelAtPeriodEnd = Boolean(subscription.cancel_at_period_end);
        const status =
          subscription.status === "trialing"
            ? "trialing"
            : subscription.status === "active"
            ? "active"
            : subscription.status === "canceled"
            ? "cancelled"
            : "active";

        // Met à jour Supabase via RPC
        const { error: rpcError } = await supabase.rpc("upsert_subscription_from_stripe", {
          p_user_id: userId,
          p_plan_key: planKey,
          p_stripe_customer_id: session.customer,
          p_stripe_subscription_id: subscription.id,
          p_stripe_price_id: priceId,
          p_status: status,
          p_current_period_end: currentPeriodEnd,
          p_cancel_at_period_end: cancelAtPeriodEnd,
        });

        if (rpcError) console.error("❌ Erreur RPC Supabase (abo):", rpcError);
        else console.log("✅ Abonnement mis à jour dans Supabase");
      }

      // Cas 3 : Achat unique “VIP à vie”
      if (session.mode === "payment" && planKey === "vip_lifetime") {
        const { error: insertErr } = await supabase.from("abonnements").insert({
          profile_id: userId,
          plan_name: "VIP à vie",
          status: "active",
          start_date: new Date().toISOString(),
          auto_renew: false,
          is_permanent: true,
        });
        if (insertErr) console.error("❌ Erreur insert VIP à vie:", insertErr);

        const { error: rpcErr } = await supabase.rpc("apply_plan_to_profile", {
          p_user_id: userId,
          p_plan_key: "vip",
        });
        if (rpcErr) console.error("❌ Erreur RPC apply_plan_to_profile:", rpcErr);
      }
    }

    // =========================================================
    // (B) Mise à jour / annulation d’abonnement Stripe
    // =========================================================
    if (
      event.type === "customer.subscription.updated" ||
      event.type === "customer.subscription.deleted"
    ) {
      const sub = event.data.object;
      const priceId = sub.items.data[0]?.price?.id ?? null;
      const currentPeriodEnd = new Date(sub.current_period_end * 1000).toISOString();
      const cancelAtPeriodEnd = Boolean(sub.cancel_at_period_end);
      const status =
        event.type === "customer.subscription.deleted"
          ? "cancelled"
          : sub.status === "active"
          ? "active"
          : sub.status === "trialing"
          ? "trialing"
          : sub.status === "canceled"
          ? "cancelled"
          : "active";

      // Trouver l’utilisateur lié à cet abonnement Stripe
      const { data: abo, error: aboErr } = await supabase
        .from("abonnements")
        .select("profile_id")
        .eq("stripe_subscription_id", sub.id)
        .limit(1)
        .maybeSingle();

      if (aboErr) console.error("Erreur recherche abo:", aboErr);
      if (!abo?.profile_id) return res.json({ received: true });

      // Identifier le plan
      const { data: plan } = await supabase
        .from("pricing_plans")
        .select("key")
        .eq("stripe_price_id", priceId)
        .maybeSingle();

      const planKey = plan?.key || "standard";

      // Appel RPC pour mise à jour
      const { error: rpcError } = await supabase.rpc("upsert_subscription_from_stripe", {
        p_user_id: abo.profile_id,
        p_plan_key: planKey,
        p_stripe_customer_id: sub.customer,
        p_stripe_subscription_id: sub.id,
        p_stripe_price_id: priceId,
        p_status: status,
        p_current_period_end: currentPeriodEnd,
        p_cancel_at_period_end: cancelAtPeriodEnd,
      });

      if (rpcError) console.error("❌ Erreur update subscription:", rpcError);
      else console.log("✅ Abonnement mis à jour après event Stripe");
    }

    res.json({ received: true });
  } catch (err) {
    console.error("❌ Erreur interne Webhook :", err);
    res.status(500).send("Erreur serveur interne");
  }
});

// ============================================================
// 2️⃣ Création de session Stripe - OK COINS
// ============================================================

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

app.post("/create-checkout-session", async (req, res) => {
  try {
    const { packId, userId } = req.body;

    if (!packId || !userId) {
      return res.status(400).json({ error: "packId et userId sont requis" });
    }

    // Récupère les infos du pack dans Supabase
    const { data: pack, error: packErr } = await supabase
      .from("okcoins_packs")
      .select("pack_name, price_eur, is_active")
      .eq("id", packId)
      .single();

    if (packErr || !pack || !pack.is_active) {
      return res.status(404).json({ error: "Pack introuvable ou inactif" });
    }

    const session = await stripe.checkout.sessions.create({
      mode: "payment",
      payment_method_types: ["card"],
      line_items: [
        {
          price_data: {
            currency: "eur",
            product_data: { name: pack.pack_name },
            unit_amount: Math.round(Number(pack.price_eur) * 100),
          },
          quantity: 1,
        },
      ],
      success_url: `${process.env.FRONTEND_URL}/paiement-success?packId=${packId}`,
      cancel_url: `${process.env.FRONTEND_URL}/paiement-annule`,
      metadata: { userId, packId: String(packId) },
    });

    res.json({ url: session.url });
  } catch (err) {
    console.error("❌ Erreur création session Stripe :", err);
    res.status(500).json({ error: "Erreur serveur interne" });
  }
});

// ============================================================
// 3️⃣ Création de session Stripe - Abonnements (corrigée ✅)
// ============================================================

app.post("/create-subscription-session", async (req, res) => {
  try {
    const { userId, planKey, priceId } = req.body;
    if (!userId || !planKey)
      return res.status(400).json({ error: "userId et planKey sont requis" });

    let finalPriceId = priceId;

    if (!finalPriceId) {
      const { data: plan, error: planErr } = await supabase
        .from("pricing_plans")
        .select("stripe_price_id")
        .eq("key", planKey)
        .maybeSingle();
      if (planErr || !plan) throw new Error("Impossible de trouver le plan Stripe ID");
      finalPriceId = plan.stripe_price_id;
    }

    const session = await stripe.checkout.sessions.create({
      mode: "subscription",
      payment_method_types: ["card"],
      line_items: [{ price: finalPriceId, quantity: 1 }],
      allow_promotion_codes: true,
      success_url: `${process.env.FRONTEND_URL}/success?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${process.env.FRONTEND_URL}/cancel`,
      metadata: { userId, planKey },
    });

    // ✅ Correction ici : on renvoie maintenant l'URL
    res.json({ url: session.url });
  } catch (err) {
    console.error("❌ Erreur création session abonnement :", err);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// 4️⃣ Activation du plan gratuit
// ============================================================

app.post("/activate-free-plan", async (req, res) => {
  try {
    const { userId } = req.body;
    if (!userId) return res.status(400).json({ error: "userId requis" });

    const { error: rpcErr } = await supabase.rpc("apply_plan_to_profile", {
      p_user_id: userId,
      p_plan_key: "free",
    });
    if (rpcErr) throw new Error(rpcErr.message);

    const { error: insertErr } = await supabase.from("abonnements").insert({
      profile_id: userId,
      plan_name: "Gratuit",
      status: "active",
      auto_renew: false,
    });
    if (insertErr) throw new Error(insertErr.message);

    res.json({ ok: true });
  } catch (e) {
    console.error("❌ Erreur activation plan gratuit :", e);
    res.status(500).json({ error: e.message });
  }
});

// ============================================================
// 5️⃣ Route de santé (Render health check)
// ============================================================

app.get("/", (req, res) => {
  res.send("✅ OneKamer backend est opérationnel !");
});

// ============================================================
// 6️⃣ Lancement serveur
// ============================================================

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Serveur OneKamer actif sur port ${PORT}`);
});
