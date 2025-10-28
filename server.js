// ============================================================
// OneKamer - Serveur Stripe + Supabase (OK COINS + Abonnements)
// ============================================================

import * as dotenv from "dotenv";
dotenv.config({ path: ".env.local" });

import express from "express";
import Stripe from "stripe";
import bodyParser from "body-parser";
import cors from "cors";
import { createClient } from "@supabase/supabase-js";
import uploadRoute from "./api/upload.js";
import partenaireDefaultsRoute from "./api/fix-partenaire-images.js";
import fixAnnoncesImagesRoute from "./api/fix-annonces-images.js";
import fixEvenementsImagesRoute from "./api/fix-evenements-images.js";
import notificationsRouter from "./api/notifications.js";


// ✅ Correction : utiliser le fetch natif de Node 18+ (pas besoin d'import)
const fetch = globalThis.fetch;
// =======================================================
// ✅ CONFIGURATION CORS — OneKamer Render + Horizon
// =======================================================
const app = express();
// 🔹 Récupération et gestion de plusieurs origines depuis l'environnement
const allowedOrigins = process.env.CORS_ORIGIN
  ? process.env.CORS_ORIGIN.split(",").map(origin => origin.trim())
  : [
      "https://onekamer.co",                        // Horizon (production)
      "https://onekamer-front-render.onrender.com", // Render (test/labo)
    ];

app.use(
  cors({
    origin: function (origin, callback) {
      // Autorise les appels sans origin (ex: Postman, tests internes)
      if (!origin) return callback(null, true);

      if (allowedOrigins.includes(origin)) {
        callback(null, true);
      } else {
        console.warn(`🚫 CORS refusé pour l'origine : ${origin}`);
        callback(new Error("Non autorisé par CORS"));
      }
    },
    credentials: true,
  })
);

console.log("✅ CORS actif pour :", allowedOrigins.join(", "));

app.use("/api", uploadRoute);
app.use("/api", partenaireDefaultsRoute);
app.use("/api", fixAnnoncesImagesRoute);
app.use("/api", fixEvenementsImagesRoute);
app.use("/api", notificationsRouter);


const stripe = new Stripe(process.env.STRIPE_SECRET_KEY, {
  apiVersion: "2024-06-20",
});

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// ============================================================
// 🔎 Journalisation auto (évènements sensibles) -> public.server_logs
//   Colonnes attendues (recommandées) :
//     id uuid default gen_random_uuid() PK
//     created_at timestamptz default now()
//     category text            -- ex: 'stripe', 'subscription', 'okcoins', 'withdrawal', 'profile'
//     action text              -- ex: 'webhook.received', 'checkout.created', ...
//     status text              -- 'success' | 'error' | 'info'
//     user_id uuid null
//     context jsonb null
//   ⚠️ Le code fonctionne même si des colonnes supplémentaires existent.
// ============================================================

function safeJson(obj) {
  try {
    return JSON.parse(
      JSON.stringify(obj, (_key, val) => {
        if (typeof val === "bigint") return val.toString();
        return val;
      })
    );
  } catch (_e) {
    return { note: "context serialization failed" };
  }
}

async function logEvent({ category, action, status, userId = null, context = {} }) {
  try {
    const payload = {
      category,
      action,
      status,
      user_id: userId || null,
      context: safeJson(context),
    };
    const { error } = await supabase.from("server_logs").insert(payload);
    if (error) {
      console.warn("⚠️ Log insert failed:", error.message);
    }
  } catch (e) {
    console.warn("⚠️ Log error:", e?.message || e);
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
    await logEvent({
      category: "stripe",
      action: "webhook.verify",
      status: "error",
      context: { error: err.message },
    });
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  console.log("📦 Événement Stripe reçu :", event.type);
  await logEvent({
    category: "stripe",
    action: "webhook.received",
    status: "info",
    context: { event_type: event.type, event_id: event.id },
  });

  try {
    if (event.type === "checkout.session.completed") {
      const session = event.data.object;
      const { userId, packId, planKey } = session.metadata || {};

      // Cas 1 : Achat OK COINS
      if (packId) {
        try {
          const { error: evtErr } = await supabase
            .from("stripe_events")
            .insert({ event_id: event.id });
          if (evtErr && evtErr.code === "23505") {
            console.log("🔁 Événement déjà traité :", event.id);
            await logEvent({
              category: "okcoins",
              action: "checkout.completed.duplicate",
              status: "info",
              userId,
              context: { event_id: event.id, packId },
            });
            return res.json({ received: true });
          }

          const { data, error } = await supabase.rpc("okc_grant_pack_after_payment", {
            p_user: userId,
            p_pack_id: parseInt(packId, 10),
            p_status: "paid",
          });

          if (error) {
            console.error("❌ Erreur RPC Supabase (OK COINS):", error);
            await logEvent({
              category: "okcoins",
              action: "checkout.completed.credit",
              status: "error",
              userId,
              context: { packId, rpc_error: error.message },
            });
          } else {
            console.log("✅ OK COINS crédités :", data);
            await logEvent({
              category: "okcoins",
              action: "checkout.completed.credit",
              status: "success",
              userId,
              context: { packId, data },
            });
          }
        } catch (e) {
          await logEvent({
            category: "okcoins",
            action: "checkout.completed.credit",
            status: "error",
            userId,
            context: { packId, error: e?.message || e },
          });
          throw e;
        }
      }

      // Cas 2 : Abonnement Stripe (Standard / VIP)
      if (session.mode === "subscription" && planKey) {
        try {
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

          if (rpcError) {
            console.error("❌ Erreur RPC Supabase (abo):", rpcError);
            await logEvent({
              category: "subscription",
              action: "upsert.from_webhook",
              status: "error",
              userId,
              context: { planKey, subscription_id: subscription.id, rpc_error: rpcError.message },
            });
          } else {
            console.log("✅ Abonnement mis à jour dans Supabase");
            await logEvent({
              category: "subscription",
              action: "upsert.from_webhook",
              status: "success",
              userId,
              context: { planKey, subscription_id: subscription.id },
            });
          }
        } catch (e) {
          await logEvent({
            category: "subscription",
            action: "upsert.from_webhook",
            status: "error",
            userId,
            context: { planKey, error: e?.message || e },
          });
          throw e;
        }
      }

      // Cas 3 : Achat unique “VIP à vie”
      if (session.mode === "payment" && planKey === "vip_lifetime") {
        try {
          const { error: insertErr } = await supabase.from("abonnements").insert({
            profile_id: userId,
            plan_name: "VIP à vie",
            status: "active",
            start_date: new Date().toISOString(),
            auto_renew: false,
            is_permanent: true,
          });
          if (insertErr) {
            console.error("❌ Erreur insert VIP à vie:", insertErr);
            await logEvent({
              category: "subscription",
              action: "vip_lifetime.insert",
              status: "error",
              userId,
              context: { error: insertErr.message },
            });
          } else {
            const { error: rpcErr } = await supabase.rpc("apply_plan_to_profile", {
              p_user_id: userId,
              p_plan_key: "vip",
            });
            if (rpcErr) {
              console.error("❌ Erreur RPC apply_plan_to_profile:", rpcErr);
              await logEvent({
                category: "subscription",
                action: "vip_lifetime.apply_plan",
                status: "error",
                userId,
                context: { error: rpcErr.message },
              });
            } else {
              await logEvent({
                category: "subscription",
                action: "vip_lifetime.completed",
                status: "success",
                userId,
                context: {},
              });
            }
          }
        } catch (e) {
          await logEvent({
            category: "subscription",
            action: "vip_lifetime",
            status: "error",
            userId,
            context: { error: e?.message || e },
          });
          throw e;
        }
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

      try {
        // Trouver l’utilisateur lié à cet abonnement Stripe
        const { data: abo, error: aboErr } = await supabase
          .from("abonnements")
          .select("profile_id")
          .eq("stripe_subscription_id", sub.id)
          .limit(1)
          .maybeSingle();

        if (aboErr) {
          console.error("Erreur recherche abo:", aboErr);
          await logEvent({
            category: "subscription",
            action: "stripe.sub.update.lookup_user",
            status: "error",
            context: { subscription_id: sub.id, error: aboErr.message },
          });
        }
        if (!abo?.profile_id) {
          await logEvent({
            category: "subscription",
            action: "stripe.sub.update.no_user",
            status: "info",
            context: { subscription_id: sub.id },
          });
          return res.json({ received: true });
        }

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

        if (rpcError) {
          console.error("❌ Erreur update subscription:", rpcError);
          await logEvent({
            category: "subscription",
            action: "stripe.sub.update",
            status: "error",
            userId: abo.profile_id,
            context: { subscription_id: sub.id, planKey, error: rpcError.message },
          });
        } else {
          console.log("✅ Abonnement mis à jour après event Stripe");
          await logEvent({
            category: "subscription",
            action: "stripe.sub.update",
            status: "success",
            userId: abo.profile_id,
            context: { subscription_id: sub.id, planKey, status },
          });
        }
      } catch (e) {
        await logEvent({
          category: "subscription",
          action: "stripe.sub.update",
          status: "error",
          context: { subscription_id: sub?.id, error: e?.message || e },
        });
        throw e;
      }
    }

    res.json({ received: true });
  } catch (err) {
    console.error("❌ Erreur interne Webhook :", err);
    await logEvent({
      category: "stripe",
      action: "webhook.handler",
      status: "error",
      context: { event_type: event?.type, error: err?.message || err },
    });
    res.status(500).send("Erreur serveur interne");
  }
});

// ============================================================
// 2️⃣ Création de session Stripe - OK COINS
// ============================================================

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

app.post("/create-checkout-session", async (req, res) => {
  const { packId, userId } = req.body;

  try {
    if (!packId || !userId) {
      await logEvent({
        category: "okcoins",
        action: "checkout.create",
        status: "error",
        userId,
        context: { reason: "missing packId or userId" },
      });
      return res.status(400).json({ error: "packId et userId sont requis" });
    }

    // Récupère les infos du pack dans Supabase
    const { data: pack, error: packErr } = await supabase
      .from("okcoins_packs")
      .select("pack_name, price_eur, is_active")
      .eq("id", packId)
      .single();

    if (packErr || !pack || !pack.is_active) {
      await logEvent({
        category: "okcoins",
        action: "checkout.create",
        status: "error",
        userId,
        context: { packId, error: packErr?.message || "Pack introuvable ou inactif" },
      });
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

    await logEvent({
      category: "okcoins",
      action: "checkout.create",
      status: "success",
      userId,
      context: { packId, session_id: session.id },
    });

    res.json({ url: session.url });
  } catch (err) {
    console.error("❌ Erreur création session Stripe :", err);
    await logEvent({
      category: "okcoins",
      action: "checkout.create",
      status: "error",
      userId: req.body?.userId || null,
      context: { packId: req.body?.packId, error: err?.message || err },
    });
    res.status(500).json({ error: "Erreur serveur interne" });
  }
});

// ============================================================
// 3️⃣ Création de session Stripe - Abonnements
// ============================================================

app.post("/create-subscription-session", async (req, res) => {
  const { userId, planKey, priceId } = req.body;

  try {
    if (!userId || !planKey) {
      await logEvent({
        category: "subscription",
        action: "checkout.subscription.create",
        status: "error",
        userId,
        context: { reason: "missing userId or planKey" },
      });
      return res.status(400).json({ error: "userId et planKey sont requis" });
    }

    let finalPriceId = priceId;

    if (!finalPriceId) {
      const { data: plan, error: planErr } = await supabase
        .from("pricing_plans")
        .select("stripe_price_id")
        .eq("key", planKey)
        .maybeSingle();
      if (planErr || !plan) {
        await logEvent({
          category: "subscription",
          action: "checkout.subscription.create",
          status: "error",
          userId,
          context: { planKey, error: planErr?.message || "Impossible de trouver le plan Stripe ID" },
        });
        throw new Error("Impossible de trouver le plan Stripe ID");
      }
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

    await logEvent({
      category: "subscription",
      action: "checkout.subscription.create",
      status: "success",
      userId,
      context: { planKey, price_id: finalPriceId, session_id: session.id },
    });

    res.json({ url: session.url });
  } catch (err) {
    console.error("❌ Erreur création session abonnement :", err);
    await logEvent({
      category: "subscription",
      action: "checkout.subscription.create",
      status: "error",
      userId: req.body?.userId || null,
      context: { planKey: req.body?.planKey, error: err?.message || err },
    });
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// 4️⃣ Activation du plan gratuit
// ============================================================

app.post("/activate-free-plan", async (req, res) => {
  try {
    const { userId } = req.body;
    if (!userId) {
      await logEvent({
        category: "profile",
        action: "plan.free.activate",
        status: "error",
        context: { reason: "missing userId" },
      });
      return res.status(400).json({ error: "userId requis" });
    }

    const { error: rpcErr } = await supabase.rpc("apply_plan_to_profile", {
      p_user_id: userId,
      p_plan_key: "free",
    });
    if (rpcErr) {
      await logEvent({
        category: "profile",
        action: "plan.free.apply",
        status: "error",
        userId,
        context: { error: rpcErr.message },
      });
      throw new Error(rpcErr.message);
    }

    const { error: insertErr } = await supabase.from("abonnements").insert({
      profile_id: userId,
      plan_name: "Gratuit",
      status: "active",
      auto_renew: false,
    });
    if (insertErr) {
      await logEvent({
        category: "profile",
        action: "plan.free.insert",
        status: "error",
        userId,
        context: { error: insertErr.message },
      });
      throw new Error(insertErr.message);
    }

    await logEvent({
      category: "profile",
      action: "plan.free.activated",
      status: "success",
      userId,
      context: {},
    });

    res.json({ ok: true });
  } catch (e) {
    console.error("❌ Erreur activation plan gratuit :", e);
    await logEvent({
      category: "profile",
      action: "plan.free.activate",
      status: "error",
      userId: req?.body?.userId || null,
      context: { error: e?.message || e },
    });
    res.status(500).json({ error: e.message });
  }
});

// ============================================================
// 5️⃣ Notification Telegram - Retrait OK COINS
// ============================================================

app.post("/notify-withdrawal", async (req, res) => {
  const { userId, username, email, amount } = req.body;

  if (!userId || !username || !email || !amount) {
    await logEvent({
      category: "withdrawal",
      action: "telegram.notify",
      status: "error",
      userId: userId || null,
      context: { reason: "missing fields", body: req.body },
    });
    return res.status(400).json({ error: "Informations incomplètes pour la notification." });
  }

  try {
    const message = `
💸 *Nouvelle demande de retrait OK COINS*  
👤 Utilisateur : ${username}  
📧 Email : ${email}  
🆔 ID : ${userId}  
💰 Montant demandé : ${Number(amount).toLocaleString("fr-FR")} pièces  
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

    console.log("📨 Notification Telegram envoyée avec succès.");
    await logEvent({
      category: "withdrawal",
      action: "telegram.notify",
      status: "success",
      userId,
      context: { telegram_message_id: data?.result?.message_id || null },
    });

    res.json({ success: true });
  } catch (err) {
    console.error("❌ Erreur notification Telegram :", err);
    await logEvent({
      category: "withdrawal",
      action: "telegram.notify",
      status: "error",
      userId,
      context: { error: err?.message || err },
    });
    res.status(500).json({ error: "Échec notification Telegram" });
  }
});

// ============================================================
// 7️⃣ Notifications OneSignal
// ============================================================

app.post("/send-notification", async (req, res) => {
  const { title, message } = req.body;

  if (!title || !message) {
    return res.status(400).json({ error: "title et message requis" });
  }

  try {
    const response = await fetch("https://onesignal.com/api/v1/notifications", {
      method: "POST",
      headers: {
        "Authorization": `Basic ${process.env.ONESIGNAL_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        app_id: process.env.ONESIGNAL_APP_ID,
        headings: { en: title },
        contents: { en: message },
        included_segments: ["All"],
        url: "https://onekamer.co", // optionnel: lien cliquable
      }),
    });

    const data = await response.json();

    if (data.errors) {
      console.error("❌ Erreur OneSignal:", data.errors);
      await logEvent({
        category: "onesignal",
        action: "notification.send",
        status: "error",
        context: { title, message, errors: data.errors },
      });
      return res.status(500).json({ error: data.errors });
    }

    console.log("✅ Notification OneSignal envoyée :", data.id);
    await logEvent({
      category: "onesignal",
      action: "notification.send",
      status: "success",
      context: { title, message, notification_id: data.id },
    });

    res.json({ success: true, notification_id: data.id });
  } catch (err) {
    console.error("❌ Erreur envoi OneSignal:", err);
    await logEvent({
      category: "onesignal",
      action: "notification.send",
      status: "error",
      context: { title, message, error: err.message },
    });
    res.status(500).json({ error: err.message });
  }
});
console.log("✅ Route OneSignal /send-notification chargée");

// ============================================================
// 6️⃣ Route de santé (Render health check)
// ============================================================

app.get("/", (req, res) => {
  res.send("✅ OneKamer backend est opérationnel !");
});

// ============================================================
// 7️⃣ Lancement serveur
// ============================================================

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Serveur OneKamer actif sur port ${PORT}`);
});
