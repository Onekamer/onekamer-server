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
import nodemailer from "nodemailer";
import uploadRoute from "./api/upload.js";
import partenaireDefaultsRoute from "./api/fix-partenaire-images.js";
import fixAnnoncesImagesRoute from "./api/fix-annonces-images.js";
import fixEvenementsImagesRoute from "./api/fix-evenements-images.js";
import pushRouter from "./api/push.js";
import webpush from "web-push";
import qrcodeRouter from "./api/qrcode.js";
import cron from "node-cron";

// ✅ Correction : utiliser le fetch natif de Node 18+ (pas besoin d'import)
const fetch = globalThis.fetch;
// =======================================================
// ✅ CONFIGURATION CORS — OneKamer Render + Horizon
// =======================================================
const app = express();
const NOTIF_PROVIDER = process.env.NOTIFICATIONS_PROVIDER || "onesignal";
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
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowedHeaders: [
      "Content-Type",
      "Authorization",
      "X-Requested-With",
      "Accept",
      "x-admin-token",
    ],
    credentials: true,
  })
);

console.log("✅ CORS actif pour :", allowedOrigins.join(", "));

app.use("/api", uploadRoute);
app.use("/api", partenaireDefaultsRoute);
app.use("/api", fixAnnoncesImagesRoute);
app.use("/api", fixEvenementsImagesRoute);
app.use("/api", pushRouter);
app.use("/api", qrcodeRouter);


const stripe = new Stripe(process.env.STRIPE_SECRET_KEY, {
  apiVersion: "2024-06-20",
});

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// ============================================================
// 📧 Email - Brevo HTTP API (PROD) + fallback Nodemailer
// ============================================================

const smtpHost = process.env.SMTP_HOST;
const smtpPort = process.env.SMTP_PORT ? parseInt(process.env.SMTP_PORT, 10) : 587;
const smtpUser = process.env.SMTP_USER;
const smtpPass = process.env.SMTP_PASS;
const fromEmail = process.env.FROM_EMAIL || "no-reply@onekamer.co";

const brevoApiKey = process.env.BREVO_API_KEY;
const brevoApiUrl = process.env.BREVO_API_URL || "https://api.brevo.com/v3/smtp/email";

let mailTransport = null;

function getMailTransport() {
  if (!mailTransport) {
    if (!smtpHost || !smtpUser || !smtpPass) {
      console.warn("⚠️ SMTP non configuré (HOST/USER/PASS manquants)");
      throw new Error("SMTP non configuré côté serveur PROD");
    }
    console.log("📧 Initialisation transport SMTP Nodemailer", {
      host: smtpHost,
      port: smtpPort,
      secure: smtpPort === 465,
    });
    mailTransport = nodemailer.createTransport({
      host: smtpHost,
      port: smtpPort,
      secure: smtpPort === 465,
      auth: {
        user: smtpUser,
        pass: smtpPass,
      },
      connectionTimeout: 15000,
      socketTimeout: 15000,
    });
  }
  return mailTransport;
}

async function sendEmailViaBrevo({ to, subject, text }) {
  if (!brevoApiKey) {
    console.warn("⚠️ BREVO_API_KEY manquant, tentative via transport SMTP Nodemailer");
    const transport = getMailTransport();
    await transport.sendMail({ from: fromEmail, to, subject, text });
    return;
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 20000);

  try {
    const response = await fetch(brevoApiUrl, {
      method: "POST",
      headers: {
        "api-key": brevoApiKey,
        "Content-Type": "application/json",
        accept: "application/json",
      },
      body: JSON.stringify({
        sender: { email: fromEmail, name: "OneKamer" },
        to: [{ email: to }],
        subject,
        textContent: text,
      }),
      signal: controller.signal,
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Brevo API error ${response.status}: ${errorText}`);
    }

    console.log("📧 Brevo HTTP API → email envoyé à", to);
  } catch (err) {
    console.error("❌ Erreur Brevo HTTP API:", err.message || err);
    throw err;
  } finally {
    clearTimeout(timeout);
  }
}

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
// 🔔 Notification push admin (retraits) via système natif
//    - Utilise NOTIFICATIONS_PROVIDER = 'supabase_light'
//    - Envoie vers /api/push/send pour tous les profils admin
// ============================================================

async function sendAdminWithdrawalPush(req, { username, amount }) {
  if (NOTIF_PROVIDER !== "supabase_light") return;

  try {
    const { data: admins, error } = await supabase
      .from("profiles")
      .select("id")
      .or("role.eq.admin,is_admin.is.true");

    if (error) {
      console.warn("⚠️ Erreur lecture profils admin pour push retrait:", error.message);
      await logEvent({
        category: "withdrawal",
        action: "push.notify",
        status: "error",
        context: { stage: "fetch_admins", error: error.message },
      });
      return;
    }

    if (!admins || admins.length === 0) {
      await logEvent({
        category: "withdrawal",
        action: "push.notify",
        status: "info",
        context: { note: "no_admins_found" },
      });
      return;
    }

    const targetUserIds = admins.map((a) => a.id).filter(Boolean);
    if (targetUserIds.length === 0) return;

    const baseUrl = `${req.protocol}://${req.get("host")}`;

    const safeName = username || "Un membre";
    const title = "Nouvelle demande de retrait OK COINS";
    const message = `${safeName} a demandé un retrait de ${amount.toLocaleString("fr-FR")} pièces.`;

    const response = await fetch(`${baseUrl}/api/push/send`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        title,
        message,
        targetUserIds,
        url: "https://onekamer.co/okcoins",
        data: { type: "okcoins_withdrawal" },
      }),
    });

    const data = await response.json().catch(() => null);

    if (!response.ok) {
      await logEvent({
        category: "withdrawal",
        action: "push.notify",
        status: "error",
        context: { stage: "push_send", status: response.status, body: data },
      });
      return;
    }

    await logEvent({
      category: "withdrawal",
      action: "push.notify",
      status: "success",
      context: { sent: data?.sent ?? null, target_count: targetUserIds.length },
    });
  } catch (e) {
    console.warn("⚠️ Erreur sendAdminWithdrawalPush:", e?.message || e);
    await logEvent({
      category: "withdrawal",
      action: "push.notify",
      status: "error",
      context: { stage: "exception", error: e?.message || String(e) },
    });
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

// Désinscription de l'appareil courant (suppression par endpoint)
app.post("/push/unsubscribe", bodyParser.json(), async (req, res) => {
  if (NOTIF_PROVIDER !== "supabase_light") return res.status(200).json({ ignored: true });

  try {
    const { endpoint, userId } = req.body || {};
    if (!endpoint) return res.status(400).json({ error: "endpoint requis" });

    const { error, count } = await supabase
      .from("push_subscriptions")
      .delete()
      .eq("endpoint", endpoint)
      .select("id", { count: "exact" });
    if (error) {
      console.error("❌ Erreur delete subscription:", error.message);
      return res.status(500).json({ error: "Erreur suppression subscription" });
    }

    await logEvent({
      category: "notifications",
      action: "push.unsubscribe",
      status: "success",
      userId: userId || null,
      context: { endpoint, deleted: count ?? 0 },
    });

    res.json({ success: true, deleted: count ?? 0 });
  } catch (e) {
    console.error("❌ Erreur /push/unsubscribe:", e);
    res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// ============================================================
// 2️⃣ Création de session Stripe - OK COINS
// ============================================================

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// ============================================================
// 📧 Emails admin (enqueue + process + count)
// ============================================================

function assertAdmin(req) {
  const token = req.headers["x-admin-token"];
  if (!token || token !== process.env.ADMIN_API_TOKEN) {
    const err = new Error("Accès refusé (admin token invalide)");
    err.statusCode = 401;
    throw err;
  }
}

function buildInfoAllBody({ username, message }) {
  const safeName = username || "membre";
  return `Bonjour ${safeName},\n\n${message}\n\n— L'équipe OneKamer`;
}

app.options("/admin/email/enqueue-info-all-users", cors());
app.options("/admin/email/process-jobs", cors());
app.options("/admin/email/count-segment", cors());

app.post("/admin/email/enqueue-info-all-users", cors(), async (req, res) => {
  try {
    assertAdmin(req);

    const { subject, message, limit, emails, segment } = req.body || {};
    if (!subject || !message) {
      return res.status(400).json({ error: "subject et message sont requis" });
    }

    if (Array.isArray(emails) && emails.length > 0) {
      const cleanEmails = emails
        .map((e) => (typeof e === "string" ? e.trim() : ""))
        .filter((e) => e.length > 0);

      if (cleanEmails.length === 0) {
        return res.json({ inserted: 0, message: "Aucune adresse email valide dans emails[]" });
      }

      const emailUsernameMap = new Map();
      const { data: profilesByEmail, error: profilesByEmailErr } = await supabase
        .from("profiles")
        .select("email, username")
        .in("email", cleanEmails);

      if (profilesByEmailErr) {
        console.error("⚠️ Erreur lecture profiles pour emails explicites:", profilesByEmailErr.message);
      } else if (profilesByEmail && profilesByEmail.length > 0) {
        for (const p of profilesByEmail) {
          if (p.email) {
            emailUsernameMap.set(p.email, p.username || null);
          }
        }
      }

      const rows = cleanEmails.map((email) => ({
        status: "pending",
        type: "info_all_users",
        to_email: email,
        subject,
        template: "INFO_ALL",
        payload: {
          user_id: null,
          username: emailUsernameMap.get(email) || null,
          message,
        },
      }));

      const { error: insertErr } = await supabase.from("email_jobs").insert(rows);
      if (insertErr) {
        console.error("❌ Erreur insert email_jobs (emails explicites):", insertErr.message);
        return res.status(500).json({ error: "Erreur création jobs" });
      }

      return res.json({ inserted: rows.length, mode: "explicit_emails" });
    }

    const max = typeof limit === "number" && limit > 0 ? Math.min(limit, 1000) : 500;

    let profilesQuery = supabase
      .from("profiles")
      .select("id, email, username, plan")
      .not("email", "is", null);

    const normalizedSegment = (segment || "all").toString().toLowerCase();
    if (["free", "standard", "vip"].includes(normalizedSegment)) {
      profilesQuery = profilesQuery.eq("plan", normalizedSegment);
    }

    profilesQuery = profilesQuery.limit(max);

    const { data: profiles, error } = await profilesQuery;

    if (error) {
      console.error("❌ Erreur lecture profiles pour email_jobs:", error.message);
      return res.status(500).json({ error: "Erreur lecture profils" });
    }

    if (!profiles || profiles.length === 0) {
      return res.json({ inserted: 0, message: "Aucun profil avec email" });
    }

    const rows = profiles.map((p) => ({
      status: "pending",
      type: "info_all_users",
      to_email: p.email,
      subject,
      template: "INFO_ALL",
      payload: {
        user_id: p.id,
        username: p.username,
        message,
      },
    }));

    const { error: insertErr } = await supabase.from("email_jobs").insert(rows);
    if (insertErr) {
      console.error("❌ Erreur insert email_jobs:", insertErr.message);
      return res.status(500).json({ error: "Erreur création jobs" });
    }

    res.json({ inserted: rows.length, mode: normalizedSegment });
  } catch (e) {
    const status = e.statusCode || 500;
    console.error("❌ /admin/email/enqueue-info-all-users:", e);
    res.status(status).json({ error: e.message || "Erreur interne" });
  }
});

app.post("/admin/email/count-segment", cors(), async (req, res) => {
  try {
    assertAdmin(req);

    const { segment } = req.body || {};
    const normalizedSegment = (segment || "all").toString().toLowerCase();

    let profilesQuery = supabase
      .from("profiles")
      .select("id", { count: "exact", head: true })
      .not("email", "is", null);

    if (["free", "standard", "vip"].includes(normalizedSegment)) {
      profilesQuery = profilesQuery.eq("plan", normalizedSegment);
    }

    const { count, error } = await profilesQuery;

    if (error) {
      console.error("❌ /admin/email/count-segment:", error.message);
      return res.status(500).json({ error: "Erreur lecture profils" });
    }

    res.json({ segment: normalizedSegment, count: count || 0 });
  } catch (e) {
    const status = e.statusCode || 500;
    console.error("❌ /admin/email/count-segment (handler):", e);
    res.status(status).json({ error: e.message || "Erreur interne" });
  }
});

app.post("/admin/email/process-jobs", cors(), async (req, res) => {
  try {
    assertAdmin(req);

    const { limit } = req.body || {};
    const max = typeof limit === "number" && limit > 0 ? Math.min(limit, 100) : 50;

    const { data: jobs, error } = await supabase
      .from("email_jobs")
      .select("id, to_email, subject, template, payload")
      .eq("status", "pending")
      .order("created_at", { ascending: true })
      .limit(max);

    if (error) {
      console.error("❌ Erreur lecture email_jobs:", error.message);
      return res.status(500).json({ error: "Erreur lecture jobs" });
    }

    if (!jobs || jobs.length === 0) {
      return res.json({ processed: 0, message: "Aucun job pending" });
    }

    console.log("📧 /admin/email/process-jobs → récupération", jobs.length, "jobs pending");

    let sentCount = 0;
    const errors = [];

    for (const job of jobs) {
      try {
        let textBody = "";
        if (job.template === "INFO_ALL") {
          textBody = buildInfoAllBody({
            username: job.payload?.username,
            message: job.payload?.message,
          });
        } else {
          textBody = job.payload?.message || "";
        }

        console.log("📧 Envoi email job", job.id, "→", job.to_email);

        await sendEmailViaBrevo({
          to: job.to_email,
          subject: job.subject,
          text: textBody,
        });

        console.log("✅ Email envoyé job", job.id);

        sentCount += 1;

        await supabase
          .from("email_jobs")
          .update({ status: "sent", updated_at: new Date().toISOString() })
          .eq("id", job.id);
      } catch (err) {
        console.error("❌ Erreur envoi email pour job", job.id, ":", err.message);
        errors.push({ id: job.id, error: err.message });
        await supabase
          .from("email_jobs")
          .update({
            status: "failed",
            updated_at: new Date().toISOString(),
            error_message: err.message,
          })
          .eq("id", job.id);
      }
    }

    console.log("📧 /admin/email/process-jobs terminé →", {
      processed: jobs.length,
      sent: sentCount,
      errorsCount: errors.length,
    });

    res.json({ processed: jobs.length, sent: sentCount, errors });
  } catch (e) {
    const status = e.statusCode || 500;
    console.error("❌ /admin/email/process-jobs:", e);
    res.status(status).json({ error: e.message || "Erreur interne" });
  }
});

// ============================================================
// Expiration automatique des QR Codes (horaire)
// ============================================================
try {
  cron.schedule("0 * * * *", async () => {
    try {
      const today = new Date().toISOString().slice(0, 10);
      const { data: pastEvents, error: pastErr } = await supabase
        .from("evenements")
        .select("id")
        .lt("date", today);
      if (pastErr) {
        await logEvent({ category: "qrcode", action: "expire.scan", status: "error", context: { error: pastErr.message } });
        return;
      }
      const ids = Array.isArray(pastEvents) ? pastEvents.map((e) => e.id) : [];
      if (ids.length === 0) {
        await logEvent({ category: "qrcode", action: "expire.scan", status: "success", context: { updated: 0 } });
        return;
      }
      const { data: updated, error: upErr } = await supabase
        .from("event_qrcodes")
        .update({ status: "expired" })
        .in("event_id", ids)
        .eq("status", "active")
        .select("id");
      if (upErr) {
        await logEvent({ category: "qrcode", action: "expire.update", status: "error", context: { error: upErr.message } });
      } else {
        await logEvent({ category: "qrcode", action: "expire.update", status: "success", context: { updated: (updated?.length || 0) } });
      }
    } catch (e) {
      await logEvent({ category: "qrcode", action: "expire.cron", status: "error", context: { error: e?.message || String(e) } });
    }
  });
} catch {}

// ============================================================
// 🔑 VAPID (Web Push) — Configuration
// ============================================================
const VAPID_PUBLIC_KEY = (process.env.VAPID_PUBLIC_KEY || '').replace(/\s+/g, '');
const VAPID_PRIVATE_KEY = (process.env.VAPID_PRIVATE_KEY || '').replace(/\s+/g, '');
const VAPID_SUBJECT = process.env.VAPID_SUBJECT || "mailto:contact@onekamer.co";

if (VAPID_PUBLIC_KEY && VAPID_PRIVATE_KEY) {
  try {
    webpush.setVapidDetails(VAPID_SUBJECT, VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);
    console.log("✅ VAPID configuré (Web Push activé)");
  } catch (e) {
    console.warn("⚠️ Échec configuration VAPID:", e?.message || e);
  }
} else {
  console.warn("ℹ️ VAPID non configuré (VAPID_PUBLIC_KEY/VAPID_PRIVATE_KEY manquants)");
}

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

    const isVip = planKey === "vip";

    const session = await stripe.checkout.sessions.create({
      mode: "subscription",
      payment_method_types: ["card"],
      line_items: [{ price: finalPriceId, quantity: 1 }],
      allow_promotion_codes: true,
      success_url: `${process.env.FRONTEND_URL}/success?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${process.env.FRONTEND_URL}/cancel`,
      metadata: { userId, planKey },
      ...(isVip && {
        subscription_data: {
          trial_period_days: 30,
        },
      }),
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
// 🔔 Web Push (Option C) — Routes natives
// ============================================================

// Enregistrement abonnement Web Push
app.post("/push/subscribe", bodyParser.json(), async (req, res) => {
  if (NOTIF_PROVIDER !== "supabase_light") return res.status(200).json({ ignored: true });

  try {
    const { userId, endpoint, keys } = req.body || {};
    if (!userId || !endpoint || !keys?.p256dh || !keys?.auth) {
      return res.status(400).json({ error: "userId, endpoint, keys.p256dh et keys.auth requis" });
    }

    await supabase.from("push_subscriptions").delete().eq("endpoint", endpoint);
    const { error } = await supabase.from("push_subscriptions").insert({
      user_id: userId,
      endpoint,
      p256dh: keys.p256dh,
      auth: keys.auth,
    });
    if (error) {
      console.error("❌ Erreur insert subscription:", error.message);
      return res.status(500).json({ error: "Erreur enregistrement subscription" });
    }

    await logEvent({
      category: "notifications",
      action: "push.subscribe",
      status: "success",
      userId,
      context: { endpoint },
    });

    res.json({ success: true });
  } catch (e) {
    console.error("❌ Erreur /push/subscribe:", e);
    res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Dispatch notification: insert + envoi Web Push
app.post("/notifications/dispatch", async (req, res) => {
  if (NOTIF_PROVIDER !== "supabase_light") return res.status(200).json({ ignored: true });

  if (!VAPID_PUBLIC_KEY || !VAPID_PRIVATE_KEY) {
    console.warn("⚠️ Dispatch refusé: VAPID non configuré");
    return res.status(200).json({ success: false, reason: "vapid_not_configured" });
  }

  try {
    const { title, message, targetUserIds = [], data = {}, url = "/" } = req.body || {};
    if (!title || !message || !Array.isArray(targetUserIds) || targetUserIds.length === 0) {
      return res.status(400).json({ error: "title, message et targetUserIds requis" });
    }

    try {
      const rows = targetUserIds.map((uid) => ({ user_id: uid, title, message, type: data?.type || null, link: url }));
      const { error: insErr } = await supabase.from("notifications").insert(rows);
      if (insErr) console.warn("⚠️ Insert notifications échoué:", insErr.message);
    } catch (e) {
      console.warn("⚠️ Insert notifications (best-effort) erreur:", e?.message || e);
    }

    const { data: subs, error: subErr } = await supabase
      .from("push_subscriptions")
      .select("user_id, endpoint, p256dh, auth")
      .in("user_id", targetUserIds);
    if (subErr) console.warn("⚠️ Lecture subscriptions échouée:", subErr.message);

    const icon = "https://onekamer-media-cdn.b-cdn.net/logo/IMG_0885%202.PNG";
    const badge = "https://onekamer-media-cdn.b-cdn.net/android-chrome-72x72.png";
    const payload = (uid) => JSON.stringify({ title: title || "OneKamer", body: message, icon, badge, url, data });

    let sent = 0;
    if (Array.isArray(subs)) {
      for (const s of subs) {
        try {
          await webpush.sendNotification({ endpoint: s.endpoint, expirationTime: null, keys: { p256dh: s.p256dh, auth: s.auth } }, payload(s.user_id));
          sent++;
        } catch (e) {
          console.warn("⚠️ Échec envoi push à", s.user_id, e?.statusCode || e?.message || e);
        }
      }
    }

    await logEvent({ category: "notifications", action: "dispatch", status: "success", context: { target_count: targetUserIds.length, sent } });
    res.json({ success: true, sent });
  } catch (e) {
    console.error("❌ Erreur /notifications/dispatch:", e);
    await logEvent({ category: "notifications", action: "dispatch", status: "error", context: { error: e?.message || e } });
    res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// ============================================================
// 🔁 Aliases compatibilité pour chemins /api
// ============================================================
app.post("/api/push/subscribe", (req, res, next) => {
  console.log("🔁 Alias activé : /api/push/subscribe → /push/subscribe");
  req.url = "/push/subscribe";
  app._router.handle(req, res, next);
});

app.post("/api/notifications/dispatch", (req, res, next) => {
  console.log("🔁 Alias activé : /api/notifications/dispatch → /notifications/dispatch");
  req.url = "/notifications/dispatch";
  app._router.handle(req, res, next);
});

// Legacy Supabase webhook targets → route vers le nouveau relais Web Push
app.post("/api/supabase-notification", (req, res, next) => {
  console.log("🔁 Alias activé : /api/supabase-notification → /push/supabase-notification");
  req.url = "/push/supabase-notification";
  app._router.handle(req, res, next);
});

// Alias désinscription
app.post("/api/push/unsubscribe", (req, res, next) => {
  console.log("🔁 Alias activé : /api/push/unsubscribe → /push/unsubscribe");
  req.url = "/push/unsubscribe";
  app._router.handle(req, res, next);
});

// ============================================================
// 👥 Groupes — Demandes d'adhésion (join-request / approve / deny)
// ============================================================

// Helper d'envoi Web Push natif à une liste d'utilisateurs
async function notifyUsersNative({ targetUserIds = [], title = "OneKamer", message = "", url = "/", data = {} }) {
  if (!Array.isArray(targetUserIds) || targetUserIds.length === 0) return { sent: 0 };
  try {
    const { data: subs, error: subErr } = await supabase
      .from("push_subscriptions")
      .select("user_id, endpoint, p256dh, auth")
      .in("user_id", targetUserIds);
    if (subErr) console.warn(" Lecture subscriptions échouée:", subErr.message);

    const icon = "https://onekamer-media-cdn.b-cdn.net/logo/IMG_0885%202.PNG";
    const badge = "https://onekamer-media-cdn.b-cdn.net/android-chrome-72x72.png";
    const payload = JSON.stringify({ title, body: message, icon, badge, url, data });
    let sent = 0;
    if (Array.isArray(subs)) {
      for (const s of subs) {
        try {
          await webpush.sendNotification(
            { endpoint: s.endpoint, expirationTime: null, keys: { p256dh: s.p256dh, auth: s.auth } },
            payload
          );
          sent++;
        } catch (e) {
          console.warn(" Échec envoi push à", s.user_id, e?.statusCode || e?.message || e);
        }
      }
    }
    return { sent };
  } catch (e) {
    console.warn(" notifyUsersNative error:", e?.message || e);
    return { sent: 0 };
  }
}

// Créer une demande d’adhésion
app.post("/groups/:groupId/join-request", bodyParser.json(), async (req, res) => {
  try {
    const groupId = req.params.groupId;
    const { requesterId } = req.body || {};
    if (!groupId || !requesterId) return res.status(400).json({ error: "groupId et requesterId requis" });

    // Vérifier groupe + fondateur
    const { data: grp, error: gErr } = await supabase
      .from("groupes")
      .select("id, fondateur_id, est_prive")
      .eq("id", groupId)
      .maybeSingle();
    if (gErr || !grp) return res.status(404).json({ error: "groupe introuvable" });

    // Refuser si déjà membre
    const { data: mem } = await supabase
      .from("groupes_membres")
      .select("id")
      .eq("groupe_id", groupId)
      .eq("user_id", requesterId)
      .maybeSingle();
    if (mem) return res.status(200).json({ alreadyMember: true });

    // Upsert demande pending unique
    const { data: existing } = await supabase
      .from("group_join_requests")
      .select("id, status")
      .eq("group_id", groupId)
      .eq("requester_id", requesterId)
      .eq("status", "pending")
      .maybeSingle();
    if (existing) return res.json({ ok: true, requestId: existing.id, status: existing.status });

    const { data: inserted, error: insErr } = await supabase
      .from("group_join_requests")
      .insert({ group_id: groupId, requester_id: requesterId, status: "pending" })
      .select("id")
      .single();
    if (insErr) return res.status(500).json({ error: insErr.message });

    // Notifier le fondateur
    const founderId = grp.fondateur_id;
    await notifyUsersNative({
      targetUserIds: [founderId],
      title: "Demande d'adhésion",
      message: "Un utilisateur souhaite rejoindre votre groupe",
      url: `${process.env.FRONTEND_URL}/groupes/${groupId}?tab=demandes`,
      data: { type: "group_join_request", groupId, requesterId }
    });

    return res.json({ ok: true, requestId: inserted.id });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Approuver une demande
app.post("/groups/requests/:requestId/approve", bodyParser.json(), async (req, res) => {
  try {
    const requestId = req.params.requestId;
    const { actorId } = req.body || {};
    if (!requestId || !actorId) return res.status(400).json({ error: "requestId et actorId requis" });

    const { data: reqRow, error: rErr } = await supabase
      .from("group_join_requests")
      .select("id, group_id, requester_id, status")
      .eq("id", requestId)
      .maybeSingle();
    if (rErr || !reqRow) return res.status(404).json({ error: "demande introuvable" });
    if (reqRow.status !== "pending") return res.status(400).json({ error: "déjà décidé" });

    const { data: grp, error: gErr } = await supabase
      .from("groupes")
      .select("fondateur_id")
      .eq("id", reqRow.group_id)
      .maybeSingle();
    if (gErr || !grp) return res.status(404).json({ error: "groupe introuvable" });
    if (grp.fondateur_id !== actorId) return res.status(403).json({ error: "forbidden" });

    const { error: upErr } = await supabase
      .from("group_join_requests")
      .update({ status: "approved", decided_at: new Date().toISOString(), decided_by: actorId })
      .eq("id", requestId);
    if (upErr) return res.status(500).json({ error: upErr.message });

    await supabase.from("groupes_membres").insert({ groupe_id: reqRow.group_id, user_id: reqRow.requester_id, is_admin: false, role: "membre" });

    await notifyUsersNative({
      targetUserIds: [reqRow.requester_id],
      title: "Demande approuvée",
      message: "Vous avez été ajouté au groupe",
      url: `${process.env.FRONTEND_URL}/groupes/${reqRow.group_id}`,
      data: { type: "group_join_result", groupId: reqRow.group_id, status: "approved" }
    });
    return res.json({ ok: true });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Refuser une demande
app.post("/groups/requests/:requestId/deny", bodyParser.json(), async (req, res) => {
  try {
    const requestId = req.params.requestId;
    const { actorId } = req.body || {};
    if (!requestId || !actorId) return res.status(400).json({ error: "requestId et actorId requis" });

    const { data: reqRow, error: rErr } = await supabase
      .from("group_join_requests")
      .select("id, group_id, requester_id, status")
      .eq("id", requestId)
      .maybeSingle();
    if (rErr || !reqRow) return res.status(404).json({ error: "demande introuvable" });
    if (reqRow.status !== "pending") return res.status(400).json({ error: "déjà décidé" });

    const { data: grp, error: gErr } = await supabase
      .from("groupes")
      .select("fondateur_id")
      .eq("id", reqRow.group_id)
      .maybeSingle();
    if (gErr || !grp) return res.status(404).json({ error: "groupe introuvable" });
    if (grp.fondateur_id !== actorId) return res.status(403).json({ error: "forbidden" });

    const { error: upErr } = await supabase
      .from("group_join_requests")
      .update({ status: "denied", decided_at: new Date().toISOString(), decided_by: actorId })
      .eq("id", requestId);
    if (upErr) return res.status(500).json({ error: upErr.message });

    await notifyUsersNative({
      targetUserIds: [reqRow.requester_id],
      title: "Demande refusée",
      message: "Votre demande a été refusée",
      url: `${process.env.FRONTEND_URL}/groupes/${reqRow.group_id}`,
      data: { type: "group_join_result", groupId: reqRow.group_id, status: "denied" }
    });
    return res.json({ ok: true });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Aliases /api
app.post("/api/groups/:groupId/join-request", (req, res, next) => { req.url = `/groups/${req.params.groupId}/join-request`; app._router.handle(req, res, next); });
app.post("/api/groups/requests/:requestId/approve", (req, res, next) => { req.url = `/groups/requests/${req.params.requestId}/approve`; app._router.handle(req, res, next); });
app.post("/api/groups/requests/:requestId/deny", (req, res, next) => { req.url = `/groups/requests/${req.params.requestId}/deny`; app._router.handle(req, res, next); });

// ============================================================
// 📥 Notifications API (liste + lecture) — PROD
// ============================================================

// Liste paginée des notifications pour un utilisateur
// Query: userId (requis), limit (def 20), cursor (ISO date: created_at < cursor)
app.get("/notifications", async (req, res) => {
  try {
    const userId = req.query.userId;
    const limit = Math.min(parseInt(req.query.limit || "20", 10), 50);
    const cursor = req.query.cursor; // ISO date string

    if (!userId) return res.status(400).json({ error: "userId requis" });

    let query = supabase
      .from("notifications")
      .select("id, created_at, title, message, type, link, is_read")
      .eq("user_id", userId)
      .order("created_at", { ascending: false })
      .limit(limit + 1);

    if (cursor) {
      query = query.lt("created_at", cursor);
    }

    const { data, error } = await query;
    if (error) throw new Error(error.message);

    const hasMore = data && data.length > limit;
    const items = hasMore ? data.slice(0, limit) : data || [];
    const nextCursor = hasMore ? items[items.length - 1]?.created_at : null;

    const { count: unreadCount, error: cntErr } = await supabase
      .from("notifications")
      .select("id", { count: "exact", head: true })
      .eq("user_id", userId)
      .eq("is_read", false);
    if (cntErr) console.warn("⚠️ unreadCount error:", cntErr.message);

    res.json({
      items: items?.map((n) => ({
        id: n.id,
        created_at: n.created_at,
        title: n.title,
        body: n.message,
        type: n.type,
        deeplink: n.link || "/",
        is_read: !!n.is_read,
      })) || [],
      nextCursor,
      hasMore,
      unreadCount: typeof unreadCount === "number" ? unreadCount : 0,
    });
  } catch (e) {
    console.error("❌ GET /notifications:", e);
    res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Marquer une notification comme lue
// Body: { userId, id }
app.post("/notifications/mark-read", bodyParser.json(), async (req, res) => {
  try {
    const { userId, id } = req.body || {};
    if (!userId || !id) return res.status(400).json({ error: "userId et id requis" });

    const { error } = await supabase
      .from("notifications")
      .update({ is_read: true })
      .eq("id", id)
      .eq("user_id", userId);
    if (error) throw new Error(error.message);

    res.json({ success: true });
  } catch (e) {
    console.error("❌ POST /notifications/mark-read:", e);
    res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Tout marquer comme lu pour un utilisateur
// Body: { userId }
app.post("/notifications/mark-all-read", bodyParser.json(), async (req, res) => {
  try {
    const { userId } = req.body || {};
    if (!userId) return res.status(400).json({ error: "userId requis" });

    const { error } = await supabase
      .from("notifications")
      .update({ is_read: true })
      .eq("user_id", userId)
      .eq("is_read", false);
    if (error) throw new Error(error.message);

    res.json({ success: true });
  } catch (e) {
    console.error("❌ POST /notifications/mark-all-read:", e);
    res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Aliases /api
app.get("/api/notifications", (req, res, next) => {
  console.log("🔁 Alias activé : /api/notifications → /notifications");
  req.url = "/notifications";
  app._router.handle(req, res, next);
});

app.post("/api/notifications/mark-read", (req, res, next) => {
  console.log("🔁 Alias activé : /api/notifications/mark-read → /notifications/mark-read");
  req.url = "/notifications/mark-read";
  app._router.handle(req, res, next);
});

app.post("/api/notifications/mark-all-read", (req, res, next) => {
  console.log("🔁 Alias activé : /api/notifications/mark-all-read → /notifications/mark-all-read");
  req.url = "/notifications/mark-all-read";
  app._router.handle(req, res, next);
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
      action: "email.notify",
      status: "error",
      userId: userId || null,
      context: { reason: "missing fields", body: req.body },
    });
    return res.status(400).json({ error: "Informations incomplètes pour la notification." });
  }

  try {
    const numericAmount = Number(amount);
    const safeAmount = Number.isFinite(numericAmount) ? numericAmount : 0;
    const withdrawalEmail = process.env.WITHDRAWAL_ALERT_EMAIL || "contact@onekamer.co";

    const text = [
      "Nouvelle demande de retrait OK COINS",
      "",
      `Utilisateur : ${username}`,
      `Email : ${email}`,
      `ID utilisateur : ${userId}`,
      `Montant demandé : ${safeAmount.toLocaleString("fr-FR")} pièces`,
      `Date : ${new Date().toLocaleString("fr-FR")}`,
      "",
      "— Notification automatique OneKamer.co",
    ].join("\n");

    await sendEmailViaBrevo({
      to: withdrawalEmail,
      subject: "Nouvelle demande de retrait OK COINS",
      text,
    });

    console.log("📧 Notification retrait OK COINS envoyée par email.");
    await logEvent({
      category: "withdrawal",
      action: "email.notify",
      status: "success",
      userId,
      context: { to: withdrawalEmail, amount: safeAmount },
    });

    // 🔔 Notification push admin (système natif supabase_light)
    await sendAdminWithdrawalPush(req, { username, amount: safeAmount });

    res.json({ success: true });
  } catch (err) {
    console.error("❌ Erreur notification retrait par email :", err);
    await logEvent({
      category: "withdrawal",
      action: "email.notify",
      status: "error",
      userId,
      context: { error: err?.message || err },
    });
    res.status(500).json({ error: "Échec notification email" });
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
        included_segments: ["All"], // Tous les abonnés
        url: "https://onekamer.co",  // Lien cliquable optionnel
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
// 🔁 Alias de compatibilité : /notifications/onesignal
// (utilisé par le front Horizon / Codex)
// ============================================================
app.post("/notifications/onesignal", (req, res, next) => {
  console.log("🔁 Alias activé : /notifications/onesignal → /send-notification");
  req.url = "/send-notification";
  app._router.handle(req, res, next);
});

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
