// ============================================================
// OneKamer - Serveur Stripe + Supabase (OK COINS + Abonnements + Rencontre + Accès)
// ============================================================

import * as dotenv from "dotenv";
dotenv.config({ path: ".env.local" });

import express from "express";
import Stripe from "stripe";
import bodyParser from "body-parser";
import cors from "cors";
import { createClient } from "@supabase/supabase-js";

// ✅ Node 18+ : fetch natif
const fetch = globalThis.fetch;

// ------------------------------------------------------------
// Initialisation
// ------------------------------------------------------------
const app = express();

app.use(
  cors({
    origin: [process.env.FRONTEND_URL || "https://onekamer.co"],
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);

const stripe = new Stripe(process.env.STRIPE_SECRET_KEY, {
  apiVersion: "2024-06-20",
});

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// ============================================================
// 0️⃣ Helpers : profils & contrôle d’accès (AJOUTÉS)
// ============================================================

// Récupère (plan, is_admin) d'un user
async function getUserPlanAndAdmin(userId) {
  const { data, error } = await supabase
    .from("profiles")
    .select("plan, is_admin")
    .eq("id", userId)
    .maybeSingle();

  if (error) throw error;
  const plan = (data?.plan || "free").toLowerCase();
  const isAdmin = Boolean(data?.is_admin);
  return { plan, isAdmin };
}

// Matrice de droits côté back (alignée avec la logique BDD/Horizon)
function hasAccessMatrix({ plan, isAdmin }, section, action = "read") {
  if (isAdmin) return true;

  const P = String(plan || "free").toLowerCase();

  const rules = {
    // Lecture ouverte pour tous; création VIP only
    annonces: { read: ["free", "standard", "vip"], create: ["vip"] },

    // Lecture ouverte pour tous; création VIP only
    evenements: { read: ["free", "standard", "vip"], create: ["vip"] },

    // Lecture pour tous; création admin only (donc ici false si non admin)
    faits_divers: { read: ["free", "standard", "vip"], create: [] },

    // Lecture + commentaires pour tous
    echanges: { read: ["free", "standard", "vip"], comment: ["free", "standard", "vip"] },

    // Partenaires: lecture Standard/VIP, création VIP
    partenaires: { read: ["standard", "vip"], create: ["vip"] },

    // Groupes: lecture/participation tous, création/gestion Standard & VIP
    groupes: {
      read: ["free", "standard", "vip"],
      participate: ["free", "standard", "vip"],
      create: ["standard", "vip"],
      manage: ["standard", "vip"],
    },

    // Rencontre:
    // - Lecture générale (feed / liste) réservée au VIP
    // - Création du profil autorisée à TOUS (free, standard, vip)
    // - Interaction (match/like/passe) réservée au VIP (standard = non, free = non)
    rencontre: {
      read: ["vip"],
      create_profile: ["free", "standard", "vip"],
      interact: ["vip"],
    },

    // OK COINS: accès pour tous (dons)
    okcoins: { read: ["free", "standard", "vip"], donate: ["free", "standard", "vip"] },
  };

  const sect = rules[section];
  if (!sect) return false;
  const allowedPlans = sect[action];
  if (!allowedPlans) return false;

  return allowedPlans.includes(P);
}

// Wrapper d’accès
async function checkAccess(userId, section, action = "read") {
  const info = await getUserPlanAndAdmin(userId);
  return hasAccessMatrix(info, section, action);
}

// ============================================================
// 1️⃣ Webhook Stripe (OK COINS + Abonnements) — (INCHANGÉ)
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
    // (A) Paiement OK COINS
    if (event.type === "checkout.session.completed") {
      const session = event.data.object;
      const { userId, packId, planKey } = session.metadata || {};

      // Cas 1 : Achat OK COINS
      if (packId) {
        // Déduplication
        const { error: evtErr } = await supabase
          .from("stripe_events")
          .insert({ event_id: event.id });
        if (evtErr && evtErr.code === "23505") {
          console.log("🔁 Événement déjà traité :", event.id);
          return res.json({ received: true });
        }

        // Créditer OK COINS
        const { data, error } = await supabase.rpc("okc_grant_pack_after_payment", {
          p_user: userId,
          p_pack_id: parseInt(packId, 10),
          p_status: "paid",
        });

        if (error) console.error("❌ Erreur RPC Supabase (OK COINS):", error);
        else console.log("✅ OK COINS crédités :", data);
      }

      // Cas 2 : Abonnements
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

    // (B) Mise à jour / annulation d’abonnement Stripe
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

      // Cherche l'utilisateur lié à l'abonnement stripe
      const { data: abo, error: aboErr } = await supabase
        .from("abonnements")
        .select("profile_id")
        .eq("stripe_subscription_id", sub.id)
        .limit(1)
        .maybeSingle();

      if (aboErr) console.error("Erreur recherche abo:", aboErr);
      if (!abo?.profile_id) return res.json({ received: true });

      // Identifie le plan
      const { data: plan } = await supabase
        .from("pricing_plans")
        .select("key")
        .eq("stripe_price_id", priceId)
        .maybeSingle();

      const planKey = plan?.key || "standard";

      // Mise à jour via RPC
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
// 2️⃣ Création de session Stripe - OK COINS — (INCHANGÉ)
// ============================================================

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

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
// 3️⃣ Création de session Stripe - Abonnements — (INCHANGÉ)
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
// 4️⃣ Activation du plan gratuit — (INCHANGÉ)
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
// 5️⃣ Notification Telegram - Retrait OK COINS — (DÉJÀ PRÉSENT)
// ============================================================

app.post("/notify-withdrawal", async (req, res) => {
  const { userId, username, email, amount } = req.body;

  if (!userId || !username || !email || !amount) {
    return res.status(400).json({ error: "Informations incomplètes pour la notification." });
  }

  try {
    const message =
      `💸 *Nouvelle demande de retrait OK COINS*\n` +
      `👤 Utilisateur : ${username}\n` +
      `📧 Email : ${email}\n` +
      `🆔 ID : ${userId}\n` +
      `💰 Montant demandé : ${Number(amount).toLocaleString("fr-FR")} pièces\n` +
      `🕒 ${new Date().toLocaleString("fr-FR")}`;

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

    console.log("📨 Notification Telegram envoyée avec succès.");
    res.json({ success: true });
  } catch (err) {
    console.error("❌ Erreur notification Telegram :", err);
    res.status(500).json({ error: "Échec notification Telegram" });
  }
});

// ============================================================
// 6️⃣ Rencontres (profils / likes / matches / messages) — (AJOUTÉS)
// ============================================================

// 6.1 Créer / mettre à jour le profil rencontre
app.post("/rencontre/profile.upsert", async (req, res) => {
  try {
    const { userId, payload } = req.body; // payload = { name, age, city, ... } conforme à ta table
    if (!userId || !payload?.name) {
      return res.status(400).json({ error: "userId et name requis" });
    }

    // Tous les plans peuvent créer/éditer leur profil
    const allowed = await checkAccess(userId, "rencontre", "create_profile");
    if (!allowed) {
      return res.status(403).json({ error: "Accès refusé (créer profil)" });
    }

    // upsert par user_id (clé unique dans ta table)
    const { data, error } = await supabase
      .from("rencontres")
      .upsert(
        {
          user_id: userId,
          ...payload,
          updated_at: new Date().toISOString(),
        },
        { onConflict: "user_id" }
      )
      .select()
      .single();

    if (error) throw error;
    res.json({ ok: true, profile: data });
  } catch (e) {
    console.error("❌ rencontre/profile.upsert :", e);
    res.status(500).json({ error: e.message });
  }
});

// 6.2 Like / Match
app.post("/rencontre/like", async (req, res) => {
  try {
    const { userId, targetRencontreId } = req.body; // targetRencontreId = rencontres.id
    if (!userId || !targetRencontreId) {
      return res.status(400).json({ error: "userId et targetRencontreId requis" });
    }

    // Seuls VIP peuvent interagir
    const allowed = await checkAccess(userId, "rencontre", "interact");
    if (!allowed) {
      return res.status(403).json({ error: "Accès refusé : interaction VIP requise" });
    }

    // Récupère le profil Rencontre de l'utilisateur (doit exister)
    const { data: myRec, error: myErr } = await supabase
      .from("rencontres")
      .select("id")
      .eq("user_id", userId)
      .maybeSingle();
    if (myErr) throw myErr;
    if (!myRec?.id) return res.status(400).json({ error: "Crée d'abord ton profil rencontre" });

    const likerId = myRec.id; // rencontres.id de l'utilisateur
    const likedId = targetRencontreId;

    // Insère le like
    const { data: likeRow, error: likeErr } = await supabase
      .from("rencontres_likes")
      .insert({ liker_id: likerId, liked_id: likedId })
      .select()
      .single();

    if (likeErr && likeErr.code !== "23505") throw likeErr; // 23505 = déjà liké

    // Vérifie s’il y a match (l’inverse existe ?)
    const { data: reverse, error: revErr } = await supabase
      .from("rencontres_likes")
      .select("id")
      .eq("liker_id", likedId)
      .eq("liked_id", likerId)
      .maybeSingle();
    if (revErr) throw revErr;

    if (reverse?.id) {
      // Marque les deux likes en match
      await supabase
        .from("rencontres_likes")
        .update({ is_match: true })
        .in("id", [reverse.id, likeRow?.id].filter(Boolean));

      // Crée le match si pas déjà existant (unique LEAST/GREATEST)
      const { error: mErr } = await supabase
        .from("rencontres_matches")
        .insert({
          user1_id: likerId < likedId ? likerId : likedId,
          user2_id: likerId < likedId ? likedId : likerId,
        });
      if (mErr && mErr.code !== "23505") throw mErr;

      return res.json({ ok: true, matched: true });
    }

    res.json({ ok: true, matched: false });
  } catch (e) {
    console.error("❌ rencontre/like :", e);
    res.status(500).json({ error: e.message });
  }
});

// 6.3 Envoyer un message dans un match
app.post("/rencontre/message", async (req, res) => {
  try {
    const { userId, matchId, content } = req.body;
    if (!userId || !matchId || !content) {
      return res.status(400).json({ error: "userId, matchId et content requis" });
    }

    // VIP requis pour interagir
    const allowed = await checkAccess(userId, "rencontre", "interact");
    if (!allowed) return res.status(403).json({ error: "Accès refusé : VIP requis" });

    // Récupère le rencontres.id du user
    const { data: myRec, error: myErr } = await supabase
      .from("rencontres")
      .select("id")
      .eq("user_id", userId)
      .maybeSingle();
    if (myErr) throw myErr;
    if (!myRec?.id) return res.status(400).json({ error: "Crée d'abord ton profil rencontre" });

    // Vérifie que le user est participant du match
    const { data: matchRow, error: mErr } = await supabase
      .from("rencontres_matches")
      .select("id, user1_id, user2_id")
      .eq("id", matchId)
      .maybeSingle();
    if (mErr) throw mErr;
    if (!matchRow) return res.status(404).json({ error: "Match introuvable" });

    const isParticipant = [matchRow.user1_id, matchRow.user2_id].includes(myRec.id);
    if (!isParticipant) return res.status(403).json({ error: "Non participant du match" });

    // Détermine receiver
    const receiverId = matchRow.user1_id === myRec.id ? matchRow.user2_id : matchRow.user1_id;

    const { data, error } = await supabase
      .from("rencontres_messages_prives")
      .insert({
        match_id: matchId,
        sender_id: myRec.id,
        receiver_id: receiverId,
        content: String(content).trim(),
      })
      .select()
      .single();

    if (error) throw error;
    res.json({ ok: true, message: data });
  } catch (e) {
    console.error("❌ rencontre/message :", e);
    res.status(500).json({ error: e.message });
  }
});

// 6.4 (Option) Liste des matchs du user
app.get("/rencontre/matches/:userId", async (req, res) => {
  try {
    const userId = req.params.userId;
    if (!userId) return res.status(400).json({ error: "userId requis" });

    // VIP requis pour voir les matchs
    const allowed = await checkAccess(userId, "rencontre", "interact");
    if (!allowed) return res.status(403).json({ error: "Accès refusé : VIP requis" });

    const { data: myRec, error: myErr } = await supabase
      .from("rencontres")
      .select("id")
      .eq("user_id", userId)
      .maybeSingle();
    if (myErr) throw myErr;
    if (!myRec?.id) return res.json({ ok: true, matches: [] });

    const { data, error } = await supabase
      .from("rencontres_matches")
      .select("id, user1_id, user2_id, created_at")
      .or(`user1_id.eq.${myRec.id},user2_id.eq.${myRec.id}`)
      .order("created_at", { ascending: false });

    if (error) throw error;
    res.json({ ok: true, matches: data });
  } catch (e) {
    console.error("❌ rencontre/matches :", e);
    res.status(500).json({ error: e.message });
  }
});

// ============================================================
// 7️⃣ Endpoint utilitaire : checker l’accès depuis le front (AJOUTÉ)
// ============================================================
// ex: GET /access/partenaires/read?userId=...
// ex: GET /access/rencontre/interact?userId=...
app.get("/access/:section/:action?", async (req, res) => {
  try {
    const { section, action = "read" } = req.params;
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId requis" });

    const allowed = await checkAccess(String(userId), section, action);
    res.json({ ok: true, section, action, userId, allowed });
  } catch (e) {
    console.error("❌ /access :", e);
    res.status(500).json({ error: e.message });
  }
});

// ============================================================
// 8️⃣ Route de santé (Render health check) — (INCHANGÉ)
// ============================================================

app.get("/", (req, res) => {
  res.send("✅ OneKamer backend est opérationnel !");
});

// ============================================================
// 9️⃣ Lancement serveur — (INCHANGÉ)
// ============================================================

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Serveur OneKamer actif sur port ${PORT}`);
});
