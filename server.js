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

// ✅ Node 18+ : fetch global
const fetch = globalThis.fetch;

const app = express();

// ============================================================
// CORS (front Horizon / OneKamer)
// ============================================================
app.use(
  cors({
    origin: [process.env.FRONTEND_URL || "https://onekamer.co"],
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);

// ============================================================
// Services
// ============================================================
const stripe = new Stripe(process.env.STRIPE_SECRET_KEY, {
  apiVersion: "2024-06-20",
});

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// ============================================================
// ⚠️ Webhook Stripe doit lire le raw body
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

      // (A) OK COINS - achat de pack
      if (packId) {
        // idempotence simple
        const { error: evtErr } = await supabase
          .from("stripe_events")
          .insert({ event_id: event.id });
        if (evtErr && evtErr.code === "23505") {
          console.log("🔁 Événement déjà traité :", event.id);
          return res.json({ received: true });
        }

        const { data, error } = await supabase.rpc("okc_grant_pack_after_payment", {
          p_user: userId,
          p_pack_id: parseInt(packId, 10),
          p_status: "paid",
        });
        if (error) console.error("❌ RPC okc_grant_pack_after_payment:", error);
        else console.log("✅ OK COINS crédités :", data);
      }

      // (B) Abonnement Stripe (standard / vip)
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

        if (rpcError) console.error("❌ RPC upsert_subscription_from_stripe:", rpcError);
        else console.log("✅ Abonnement mis à jour dans Supabase");
      }

      // (C) Paiement unique “VIP à vie”
      if (session.mode === "payment" && planKey === "vip_lifetime") {
        const { error: insertErr } = await supabase.from("abonnements").insert({
          profile_id: userId,
          plan_name: "VIP à vie",
          status: "active",
          start_date: new Date().toISOString(),
          auto_renew: false,
          is_permanent: true,
        });
        if (insertErr) console.error("❌ Insert VIP à vie:", insertErr);

        const { error: rpcErr } = await supabase.rpc("apply_plan_to_profile", {
          p_user_id: userId,
          p_plan_key: "vip",
        });
        if (rpcErr) console.error("❌ RPC apply_plan_to_profile:", rpcErr);
      }
    }

    res.json({ received: true });
  } catch (err) {
    console.error("❌ Erreur interne Webhook :", err);
    res.status(500).send("Erreur serveur interne");
  }
});

// ============================================================
// Les autres routes utilisent du JSON normal
// ============================================================
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// ============================================================
// 🧠 Vérification d’accès via Supabase (check_user_access)
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
// 2️⃣ Paiement OK COINS - Checkout
// ============================================================
app.post("/create-checkout-session", async (req, res) => {
  try {
    const { packId, userId } = req.body;

    if (!packId || !userId) {
      return res.status(400).json({ error: "packId et userId sont requis" });
    }

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
// 3️⃣ Abonnements - Checkout (Stripe)
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

    res.json({ url: session.url });
  } catch (err) {
    console.error("❌ Erreur création session abonnement :", err);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// 4️⃣ Activation plan gratuit (utile au premier login)
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
// 5️⃣ Créations sécurisées (Groupes / Partenaires / Événements / Annonces)
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

app.post("/create-annonce", async (req, res) => {
  const { userId, annonceData } = req.body;
  const allowed = await hasAccess(userId, "annonces", "create");

  if (!allowed)
    return res.status(403).json({ error: "⛔ Accès refusé : vous devez être VIP pour créer une annonce." });

  const { error } = await supabase.from("annonces").insert([annonceData]);
  if (error) return res.status(500).json({ error: "Erreur création annonce" });
  res.json({ success: true });
});

// ============================================================
// 6️⃣ Faits divers : Admin uniquement (création)
// ============================================================
app.post("/create-fait-divers", async (req, res) => {
  const { userId, faitData } = req.body;

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
// 7️⃣ Rencontre
//   - Création du profil : accessible à tous (Free/Standard/VIP)
//   - Interactions (like / pass / match) : VIP uniquement
// ============================================================
app.post("/create-rencontre-profile", async (req, res) => {
  const { userId, profileData } = req.body;

  if (!userId || !profileData) {
    return res.status(400).json({ error: "userId et profileData requis" });
  }

  // Insertion souple : si la table n'existe pas encore, on n'explose pas
  try {
    const { error } = await supabase.from("rencontre_profiles").insert([
      {
        user_id: userId,
        ...profileData,
      },
    ]);

    if (error) {
      // Si table absente, on renvoie un succès informatif (aucun blocage)
      if ((error.code || "").toString() === "42P01") {
        console.warn("ℹ️ Table 'rencontre_profiles' absente. Profil non persisté pour l’instant.");
        return res.json({
          success: true,
          note: "Profil rencontre reçu. La persistance sera activée une fois la table disponible.",
        });
      }
      return res.status(500).json({ error: "Erreur création du profil rencontre." });
    }

    res.json({ success: true });
  } catch (e) {
    console.error("❌ create-rencontre-profile:", e);
    res.status(500).json({ error: "Erreur serveur (profil rencontre)" });
  }
});

app.post("/match-action", async (req, res) => {
  const { userId, targetUserId, action } = req.body; // action ∈ {'like','pass'}

  if (!userId || !targetUserId || !action) {
    return res.status(400).json({ error: "userId, targetUserId et action requis" });
  }

  // 🔐 VIP uniquement pour interagir
  const allowed = await hasAccess(userId, "rencontre", "interact"); // l'action côté SQL peut mapper sur 'read' VIP-only
  if (!allowed) {
    return res.status(403).json({
      error:
        "Fonctionnalité réservée aux membres VIP. Passez au forfait VIP pour aimer, passer ou matcher.",
    });
  }

  // Tentative d'enregistrement de l'action (si la table existe)
  try {
    const { error } = await supabase.from("rencontre_actions").insert([
      {
        user_id: userId,
        target_user_id: targetUserId,
        action, // 'like' ou 'pass'
      },
    ]);

    if (error) {
      if ((error.code || "").toString() === "42P01") {
        console.warn("ℹ️ Table 'rencontre_actions' absente. Action non persistée pour l’instant.");
        return res.json({
          success: true,
          note: "Action rencontre autorisée (VIP). Persistance activée quand la table sera créée.",
        });
      }
      return res.status(500).json({ error: "Erreur enregistrement action rencontre." });
    }

    res.json({ success: true });
  } catch (e) {
    console.error("❌ match-action:", e);
    res.status(500).json({ error: "Erreur serveur (match-action)" });
  }
});

// ============================================================
// 8️⃣ Notification Telegram - Retrait OK COINS
// ============================================================
app.post("/notify-withdrawal", async (req, res) => {
  const { userId, username, email, amount } = req.body;

  if (!userId || !username || !email || !amount)
    return res.status(400).json({ error: "Informations incomplètes pour la notification." });

  try {
    // Log interne (si table présente)
    await supabase
      .from("okcoins_transactions")
      .insert({ user_id: userId, amount, type: "withdrawal", status: "pending", notified: false })
      .then(({ error }) => {
        if (error) {
          console.warn("ℹ️ okcoins_transactions absent ou autre erreur (non bloquant).", error.message);
        }
      });

    const message = `
💸 *Nouvelle demande de retrait OK COINS*  
👤 Utilisateur : ${username}  
📧 Email : ${email}  
🆔 ID : ${userId}  
💰 Montant demandé : ${Number(amount).toLocaleString()} pièces  
🕒 ${new Date().toLocaleString("fr-FR")}
`;

    const response = await fetch(
      `https://api.telegram.org/bot${process.env.TELEGRAM_BOT_TOKEN}/sendMessage`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: process.env.TELEGRAM_CHAT_ID,
          text: message,
          parse_mode: "Markdown",
        }),
      }
    );

    const data = await response.json();
    if (!data.ok) throw new Error(data.description || "Erreur API Telegram");

    // Marquer notifié si la table existe
    await supabase
      .from("okcoins_transactions")
      .update({ notified: true })
      .eq("user_id", userId)
      .eq("amount", amount)
      .eq("type", "withdrawal")
      .then(({ error }) => {
        if (error) {
          console.warn("ℹ️ Impossible de marquer notified=true (non bloquant).", error.message);
        }
      });

    res.json({ success: true });
  } catch (err) {
    console.error("❌ Erreur notification Telegram :", err);
    res.status(500).json({ error: "Échec notification Telegram" });
  }
});

// ============================================================
// 9️⃣ Healthcheck
// ============================================================
app.get("/", (req, res) => res.send("✅ OneKamer backend est opérationnel !"));

// ============================================================
// 🔟 Lancement serveur
// ============================================================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`🚀 Serveur OneKamer actif sur port ${PORT}`));
