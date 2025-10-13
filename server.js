// ============================================================
// OneKamer - Serveur Stripe + Supabase (OK COINS + Abonnements + Accès sécurisé)
// ============================================================

import * as dotenv from "dotenv";
dotenv.config({ path: ".env.local" });

import express from "express";
import Stripe from "stripe";
import bodyParser from "body-parser";
import cors from "cors";
import { createClient } from "@supabase/supabase-js";

// ✅ Correction : utiliser le fetch natif de Node 18+
const fetch = globalThis.fetch;

const app = express();

// Autoriser ton front Horizon / OneKamer
app.use(
  cors({
    origin: [process.env.FRONTEND_URL || "https://onekamer.co"],
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);

// ============================================================
// 🔗 Connexions aux services
// ============================================================

const stripe = new Stripe(process.env.STRIPE_SECRET_KEY, { apiVersion: "2024-06-20" });

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

// ============================================================
// 🧠 Vérification d’accès utilisateur via Supabase (check_user_access)
// ============================================================

async function hasAccess(userId, section, action = "read") {
  try {
    const { data, error } = await supabase.rpc("check_user_access", {
      p_user_id: userId,
      p_section: section,
      p_action: action,
    });

    if (error) {
      console.error("❌ Erreur check_user_access:", error.message);
      return false;
    }

    return data === true;
  } catch (err) {
    console.error("❌ Exception hasAccess:", err);
    return false;
  }
}

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
    if (event.type === "checkout.session.completed") {
      const session = event.data.object;
      const { userId, packId, planKey } = session.metadata || {};

      // Cas 1 : Achat OK COINS
      if (packId) {
        const { error: evtErr } = await supabase.from("stripe_events").insert({ event_id: event.id });
        if (evtErr && evtErr.code === "23505") {
          console.log("🔁 Événement déjà traité :", event.id);
          return res.json({ received: true });
        }

        const { data, error } = await supabase.rpc("okc_grant_pack_after_payment", {
          p_user: userId,
          p_pack_id: parseInt(packId, 10),
          p_status: "paid",
        });

        if (error) console.error("❌ Erreur RPC Supabase (OK COINS):", error);
        else console.log("✅ OK COINS crédités :", data);
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

    res.json({ received: true });
  } catch (err) {
    console.error("❌ Erreur interne Webhook :", err);
    res.status(500).send("Erreur serveur interne");
  }
});

// ============================================================
// 2️⃣ Sécurisation de création - Groupes / Partenaires / Événements / Faits Divers
// ============================================================

app.post("/create-groupe", async (req, res) => {
  const { userId, groupeData } = req.body;
  const allowed = await hasAccess(userId, "groupes", "create");

  if (!allowed)
    return res.status(403).json({ error: "⛔ Accès refusé : vous devez être Standard ou VIP pour créer un groupe." });

  const { error } = await supabase.from("groupes").insert([groupeData]);
  if (error) return res.status(500).json({ error: "Erreur création groupe" });
  res.json({ success: true });
});

app.post("/create-partenaire", async (req, res) => {
  const { userId, partenaireData } = req.body;
  const allowed = await hasAccess(userId, "partenaires", "create");

  if (!allowed)
    return res.status(403).json({ error: "⛔ Accès refusé : vous devez être VIP pour suggérer un partenaire." });

  const { error } = await supabase.from("partenaires").insert([partenaireData]);
  if (error) return res.status(500).json({ error: "Erreur création partenaire" });
  res.json({ success: true });
});

app.post("/create-evenement", async (req, res) => {
  const { userId, eventData } = req.body;
  const allowed = await hasAccess(userId, "evenements", "create");

  if (!allowed)
    return res.status(403).json({ error: "⛔ Accès refusé : vous devez être VIP pour créer un événement." });

  const { error } = await supabase.from("evenements").insert([eventData]);
  if (error) return res.status(500).json({ error: "Erreur création événement" });
  res.json({ success: true });
});

app.post("/create-fait-divers", async (req, res) => {
  const { userId, faitData } = req.body;

  // ✅ Vérifie si admin
  const { data: profile, error: profErr } = await supabase
    .from("profiles")
    .select("is_admin")
    .eq("id", userId)
    .maybeSingle();

  if (profErr || !profile) {
    console.error("Erreur récupération profil:", profErr);
    return res.status(500).json({ error: "Erreur interne profil." });
  }

  if (!profile.is_admin) {
    return res.status(403).json({ error: "⛔ Accès réservé aux administrateurs." });
  }

  const { error } = await supabase.from("faits_divers").insert([faitData]);
  if (error) return res.status(500).json({ error: "Erreur création fait divers" });
  res.json({ success: true });
});

// ============================================================
// 3️⃣ Notification Telegram - Retrait OK COINS
// ============================================================

app.post("/notify-withdrawal", async (req, res) => {
  const { userId, username, email, amount } = req.body;

  if (!userId || !username || !email || !amount)
    return res.status(400).json({ error: "Informations incomplètes pour la notification." });

  try {
    const { error: insertErr } = await supabase.from("okcoins_transactions").insert({
      user_id: userId,
      amount,
      type: "withdrawal",
      status: "pending",
      notified: false,
    });

    if (insertErr) throw new Error("Erreur d'enregistrement du retrait");

    const message = `
💸 *Nouvelle demande de retrait OK COINS*  
👤 Utilisateur : ${username}  
📧 Email : ${email}  
🆔 ID : ${userId}  
💰 Montant demandé : ${amount.toLocaleString()} pièces  
🕒 ${new Date().toLocaleString("fr-FR")}
`;

    const response = await fetch(`https://api.telegram.org/bot${process.env.TELEGRAM_BOT_TOKEN}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: process.env.TELEGRAM_CHAT_ID,
        text: message,
        parse_mode: "Markdown",
      }),
    });

    const data = await response.json();
    if (!data.ok) throw new Error(data.description || "Erreur API Telegram");

    await supabase
      .from("okcoins_transactions")
      .update({ notified: true })
      .eq("user_id", userId)
      .eq("amount", amount)
      .eq("type", "withdrawal");

    res.json({ success: true });
  } catch (err) {
    console.error("❌ Erreur notification Telegram :", err);
    res.status(500).json({ error: "Échec notification Telegram" });
  }
});

// ============================================================
// 4️⃣ Route de santé
// ============================================================

app.get("/", (req, res) => res.send("✅ OneKamer backend est opérationnel !"));

// ============================================================
// 5️⃣ Lancement serveur
// ============================================================

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`🚀 Serveur OneKamer actif sur port ${PORT}`));
