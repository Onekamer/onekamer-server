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
import crypto from "crypto";
import uploadRoute from "./api/upload.js";
import partenaireDefaultsRoute from "./api/fix-partenaire-images.js";
import fixAnnoncesImagesRoute from "./api/fix-annonces-images.js";
import fixEvenementsImagesRoute from "./api/fix-evenements-images.js";
import pushRouter from "./api/push.js";
import webpush from "web-push";
import admin from "firebase-admin";
import apn from "apn";
import qrcodeRouter from "./api/qrcode.js";
import iapRouter from "./api/iap.js";
import cron from "node-cron";
import { createFxService } from "./utils/fx.js";
import PDFDocument from "pdfkit";

// ✅ Correction : utiliser le fetch natif de Node 18+ (pas besoin d'import)
const fetch = globalThis.fetch;
// =======================================================
// ✅ CONFIGURATION CORS — OneKamer Render + Horizon
// =======================================================
const app = express();
app.set("trust proxy", 1);
const NOTIF_PROVIDER = process.env.NOTIFICATIONS_PROVIDER || "onesignal";
// 🔹 Récupération et gestion de plusieurs origines depuis l'environnement
const allowedOrigins = process.env.CORS_ORIGIN
  ? process.env.CORS_ORIGIN.split(",").map(origin => origin.trim())
  : [
      "https://onekamer.co",                        // Horizon (production)
      "https://onekamer-front-render.onrender.com", // Render (test/labo)
       // ✅ Capacitor / Ionic WebView
      "capacitor://localhost",
      "ionic://localhost",
      "http://localhost",
      "https://localhost",
    ];

const corsOptions = {
  origin: function (origin, callback) {
    if (!origin) return callback(null, true);

    if (allowedOrigins.includes(origin)) {
      return callback(null, true);
    }
    console.warn(`🚫 CORS refusé pour l'origine : ${origin}`);
    return callback(new Error("Non autorisé par CORS"));
  },
  methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
  // ✅ IMPORTANT: ne pas fixer allowedHeaders, sinon tu te fais piéger
  credentials: true,
  optionsSuccessStatus: 204,
  };

app.use(cors(corsOptions));
app.options("*", cors(corsOptions)); // ✅ Preflight

console.log("✅ CORS actif pour :", allowedOrigins.join(", "));

// Versions courantes des chartes
const CURRENT_APP_TERMS_VERSION = "2026-01-29";
const CURRENT_VENDOR_TERMS_VERSION = "2026-01-29";
const CURRENT_BUYER_TERMS_VERSION = "2026-01-29";

// 1) Stripe webhook RAW AVANT tout parser JSON
app.post("/webhook", express.raw({ type: "application/json" }), stripeWebhookHandler);

// 2) Parsers JSON / urlencoded pour le reste
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// 3) Health check
app.get("/health", (_req, res) => res.status(200).send("ok"));

// Stripe publishable key
app.get("/api/stripe/config", (_req, res) => {
  const pk = process.env.STRIPE_PUBLISHABLE_KEY;
  if (!pk) return res.status(500).json({ error: "publishable_key_missing" });
  return res.json({ publishableKey: pk });
});

// Admin: ré-émission du PDF d'une facture (même numéro) — écrase le fichier existant
app.post("/api/admin/market/invoices/:invoiceId/rebuild-pdf", bodyParser.json(), async (req, res) => {
  try {
    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const { invoiceId } = req.params;
    if (!invoiceId) return res.status(400).json({ error: "invoiceId requis" });

    const sb = getSupabaseClient();
    const { data: inv, error: iErr } = await sb
      .from("market_invoices")
      .select("id, partner_id, number, period_start, period_end, currency, vat_scheme, vat_rate, vat_note, total_ht, total_tva, total_ttc, issued_at, pdf_bucket, pdf_path")
      .eq("id", invoiceId)
      .maybeSingle();
    if (iErr) return res.status(500).json({ error: iErr.message || "invoice_read_failed" });
    if (!inv) return res.status(404).json({ error: "invoice_not_found" });

    const { data: partner, error: pErr } = await sb
      .from("partners_market")
      .select("id, display_name, legal_name, billing_address_line1, billing_address_line2, billing_city, billing_postcode, billing_region, billing_country_code, billing_email, country_code, vat_number, vat_validation_status")
      .eq("id", inv.partner_id)
      .maybeSingle();
    if (pErr) return res.status(500).json({ error: pErr.message || "partner_read_failed" });
    if (!partner) return res.status(404).json({ error: "partner_not_found" });

    const { data: linesDb, error: lErr } = await sb
      .from("market_invoice_lines")
      .select("id, label, service_fee_ht, vat_rate, vat_amount, total_ttc, currency")
      .eq("invoice_id", invoiceId)
      .order("id", { ascending: true });
    if (lErr) return res.status(500).json({ error: lErr.message || "invoice_lines_read_failed" });

    const lines = (Array.isArray(linesDb) ? linesDb : []).map((ln) => ({
      label: ln.label,
      service_fee_ht: Math.round(Number(ln.service_fee_ht || 0) * 100),
      vat_rate: Number(ln.vat_rate || 0),
      vat_amount: Math.round(Number(ln.vat_amount || 0) * 100),
      total_ttc: Math.round(Number(ln.total_ttc || 0) * 100),
      currency: String(ln.currency || inv.currency || 'EUR').toUpperCase(),
    }));

    const invoiceForPdf = {
      number: inv.number,
      period_start: String(inv.period_start).slice(0, 10),
      period_end: String(inv.period_end).slice(0, 10),
      total_ht_cents: Math.round(Number(inv.total_ht || 0) * 100),
      total_tva_cents: Math.round(Number(inv.total_tva || 0) * 100),
      total_ttc_cents: Math.round(Number(inv.total_ttc || 0) * 100),
      vat_note: inv.vat_note || null,
      issued_at: inv.issued_at || new Date().toISOString(),
    };

    const pdfBuffer = await buildInvoicePdfBuffer({ invoice: invoiceForPdf, partner, lines });

    let bucket = String(inv.pdf_bucket || '').trim() || 'invoices';
    let path = String(inv.pdf_path || '').trim();
    if (!path) {
      const year = new Date(String(inv.period_start || '').slice(0,10)).getUTCFullYear();
      const label = String(inv.period_start || '').slice(0,7) || `${year}-01`;
      path = `${inv.partner_id}/${year}/${label}/${inv.number}.pdf`;
    }

    try { await sb.storage.createBucket(bucket, { public: false }); } catch {}
    await uploadInvoicePdf({ bucket, path, buffer: pdfBuffer });

    if (!inv.pdf_bucket || !inv.pdf_path) {
      await sb
        .from("market_invoices")
        .update({ pdf_bucket: bucket, pdf_path: path })
        .eq("id", invoiceId);
    }

    const { data: signed } = await sb.storage.from(bucket).createSignedUrl(path, 120);
    return res.json({ ok: true, url: signed?.signedUrl || null, bucket, path });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Configuration chartes (versions courantes)
app.get("/api/terms/config", async (_req, res) => {
  try {
    return res.json({
      app: { version: CURRENT_APP_TERMS_VERSION },
      marketplace: {
        vendor: { version: CURRENT_VENDOR_TERMS_VERSION },
        buyer: { version: CURRENT_BUYER_TERMS_VERSION },
      },
    });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Broadcast notification: segment -> userIds -> dispatch
app.post("/notifications/broadcast", async (req, res) => {
  if (NOTIF_PROVIDER !== "supabase_light") return res.status(200).json({ ignored: true });

  try {
    const { title, message, data = {}, url = "/", segment = "subscribed_users" } = req.body || {};
    if (!title || !message) return res.status(400).json({ error: "title et message requis" });

    // 1) Résoudre le segment vers une liste d'utilisateurs
    let userIds = [];
    if (!segment || segment === "subscribed_users") {
      try {
        const { data: webSubs } = await supabase
          .from("push_subscriptions")
          .select("user_id")
          .not("user_id", "is", null)
          .limit(5000);
        const { data: nativeTokens } = await supabase
          .from("device_push_tokens")
          .select("user_id")
          .eq("enabled", true)
          .not("user_id", "is", null)
          .limit(5000);
        const set = new Set();
        (webSubs || []).forEach((r) => { if (r?.user_id) set.add(String(r.user_id)); });
        (nativeTokens || []).forEach((r) => { if (r?.user_id) set.add(String(r.user_id)); });
        userIds = Array.from(set);
      } catch (e) {
        console.warn("⚠️ broadcast.resolve_segment_error:", e?.message || e);
      }
    }

    if (!Array.isArray(userIds) || userIds.length === 0) {
      return res.status(200).json({ success: false, reason: "no_targets" });
    }

    // 2) Relayer via le dispatcher standard (web + natif)
    await localDispatchNotification({ title, message, targetUserIds: userIds, url, data });
    return res.json({ success: true, target_count: userIds.length });
  } catch (e) {
    console.error("❌ Erreur /notifications/broadcast:", e);
    await logEvent({ category: "notifications", action: "broadcast", status: "error", context: { error: e?.message || e } });
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Accepter la charte acheteur pour une commande (avant paiement)
app.post("/api/market/orders/:orderId/terms/buyer", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { orderId } = req.params;
    if (!orderId) return res.status(400).json({ error: "orderId requis" });

    const { data: order, error: oErr } = await supabase
      .from("partner_orders")
      .select("id, customer_user_id, status, has_accepted_marketplace_terms_buyers")
      .eq("id", orderId)
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commande" });
    if (!order) return res.status(404).json({ error: "order_not_found" });
    if (String(order.customer_user_id) !== String(guard.userId)) return res.status(403).json({ error: "forbidden" });

    const s = String(order.status || '').toLowerCase();
    if (s !== 'pending') return res.status(400).json({ error: "order_not_pending" });
    if (order.has_accepted_marketplace_terms_buyers === true) return res.json({ ok: true, already: true });

    const now = new Date().toISOString();
    const { error: upErr } = await supabase
      .from("partner_orders")
      .update({ has_accepted_marketplace_terms_buyers: true, updated_at: now })
      .eq("id", orderId);
    if (upErr) return res.status(500).json({ error: upErr.message || "order_update_failed" });
    return res.json({ ok: true });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Accepter la charte générale de l'app
app.post("/api/terms/app/accept", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const now = new Date().toISOString();
    const { error } = await supabase
      .from("profiles")
      .update({ has_accepted_charte: true, chart_terms_version: CURRENT_APP_TERMS_VERSION, updated_at: now })
      .eq("id", guard.userId);
    if (error) return res.status(500).json({ error: error.message || "profile_update_failed" });
    return res.json({ ok: true, version: CURRENT_APP_TERMS_VERSION });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Top donateurs OK COINS (lecture publique via service-role)
app.get("/api/okcoins/top-donors", async (req, res) => {
  try {
    const limit = Math.min(Math.max(parseInt(req.query?.limit, 10) || 3, 1), 50);
    const { data, error } = await supabase
      .from("okcoins_users_balance")
      .select("user_id, donor_level, points_total, profiles(username, avatar_url)")
      .order("points_total", { ascending: false })
      .limit(limit);
    if (error) return res.status(500).json({ error: error.message || "Erreur lecture top donateurs" });
    return res.json({ items: Array.isArray(data) ? data : [], limit });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Recommandations Partenaires
app.get("/api/partners/recommendations", async (req, res) => {
  try {
    const idsRaw = String(req.query?.ids || "").trim();
    if (!idsRaw) return res.json({ items: [] });
    const ids = idsRaw.split(",").map((s) => s.trim()).filter(Boolean);
    if (ids.length === 0) return res.json({ items: [] });

    const { data: recs, error } = await supabase
      .from("partenaires_recommendations")
      .select("partner_id, user_id")
      .in("partner_id", ids);
    if (error) return res.status(500).json({ error: error.message || "Erreur lecture recommandations" });

    let meId = null;
    const authHeader = req.headers["authorization"] || "";
    const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : null;
    if (token) {
      try {
        const supabaseAuth = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);
        const { data: u, error: uErr } = await supabaseAuth.auth.getUser(token);
        if (!uErr && u?.user?.id) meId = String(u.user.id);
      } catch {}
    }

    const counts = new Map();
    const mine = new Set();
    for (const r of Array.isArray(recs) ? recs : []) {
      const pid = String(r.partner_id);
      counts.set(pid, (counts.get(pid) || 0) + 1);
      if (meId && String(r.user_id) === meId) mine.add(pid);
    }

    const items = ids.map((id) => ({
      partner_id: id,
      count: counts.get(String(id)) || 0,
      recommended_by_me: mine.has(String(id)),
    }));
    try {
      res.set("Cache-Control", "no-store");
      res.set("Vary", "Authorization");
    } catch {}
    return res.json({ items });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.post("/api/partners/:id/recommend", async (req, res) => {
  try {
    const authHeader = req.headers["authorization"] || "";
    const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : null;
    if (!token) return res.status(401).json({ error: "unauthorized" });

    const supabaseAuth = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);
    const { data: u, error: uErr } = await supabaseAuth.auth.getUser(token);
    if (uErr || !u?.user?.id) return res.status(401).json({ error: "invalid_token" });
    const userId = String(u.user.id);

    const { id } = req.params;
    if (!id) return res.status(400).json({ error: "partner_id_requis" });

    const { data: exists, error: exErr } = await supabase
      .from("partenaires_recommendations")
      .select("id")
      .eq("partner_id", id)
      .eq("user_id", userId)
      .maybeSingle();
    if (exErr) return res.status(500).json({ error: exErr.message || "Erreur lecture reco" });

    let action = "added";
    if (exists?.id) {
      const { error: delErr } = await supabase
        .from("partenaires_recommendations")
        .delete()
        .eq("id", exists.id);
      if (delErr) return res.status(500).json({ error: delErr.message || "Erreur suppression reco" });
      action = "removed";
    } else {
      const { error: insErr } = await supabase
        .from("partenaires_recommendations")
        .insert({ partner_id: id, user_id: userId });
      if (insErr) return res.status(500).json({ error: insErr.message || "Erreur ajout reco" });
    }

    const { count: c, error: cntErr } = await supabase
      .from("partenaires_recommendations")
      .select("id", { count: "exact", head: true })
      .eq("partner_id", id);
    if (cntErr) return res.status(500).json({ error: cntErr.message || "Erreur comptage" });
    return res.json({ action, count: c || 0 });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Soft delete d'un utilisateur par un admin
app.post("/api/admin/users/:id/soft-delete", async (req, res) => {
  try {
    const { id } = req.params;
    if (!id) return res.status(400).json({ error: "id requis" });

    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const { data: prof, error: pErr } = await supabase
      .from("profiles")
      .select("id, username, full_name, email, is_deleted, deleted_at")
      .eq("id", id)
      .maybeSingle();
    if (pErr) return res.status(500).json({ error: pErr.message || "Erreur lecture profil" });
    if (!prof) return res.status(404).json({ error: "Utilisateur introuvable" });

    if (prof.is_deleted === true) {
      return res.json({
        item: prof,
        email_notice: false,
      });
    }

    const now = new Date().toISOString();
    const { data: updated, error: uErr } = await supabase
      .from("profiles")
      .update({ is_deleted: true, deleted_at: now, updated_at: now })
      .eq("id", id)
      .select("id, username, full_name, email, is_deleted, deleted_at")
      .maybeSingle();
    if (uErr) return res.status(500).json({ error: uErr.message || "Erreur mise à jour profil" });

    let toEmail = String(updated?.email || "").trim() || null;
    if (!toEmail && supabase?.auth?.admin?.getUserById) {
      try {
        const { data: uData, error: aErr } = await supabase.auth.admin.getUserById(id);
        if (!aErr) toEmail = String(uData?.user?.email || "").trim() || null;
      } catch {}
    }

    let emailSent = false;
    if (toEmail) {
      try {
        const subject = "Confirmation de suppression de compte";
        const text = [
          `Bonjour ${updated?.username || updated?.full_name || ""}`.trim(),
          "\n",
          "Nous confirmons la suppression de votre compte OneKamer.",
          "Si vous n'êtes pas à l'origine de cette action, contactez immédiatement le support.",
          "\n",
          "— L'équipe OneKamer",
        ].join("\n");
        await sendEmailViaBrevo({ to: toEmail, subject, text });
        emailSent = true;
      } catch (e) {
        console.error("❌ Email suppression compte:", e?.message || e);
      }
    }

    // Supprimer les demandes de suppression enregistrées (si présentes)
    let deletedLogs = 0;
    try {
      const { count } = await supabase
        .from("account_deletion_logs")
        .delete()
        .eq("deleted_user_id", id)
        .select("id", { count: "exact" });
      deletedLogs = count || 0;
    } catch {}

    return res.json({ item: updated, email_notice: emailSent, deleted_logs: deletedLogs });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// ============================================================
// 🛠️ Admin Support Center: support_requests, shop_reports, deletions
// ============================================================

app.get("/api/admin/support/requests", async (req, res) => {
  try {
    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const typeRaw = req.query?.type ? String(req.query.type).toLowerCase() : "";
    const statusRaw = req.query?.status ? String(req.query.status).toLowerCase() : "";
    const mapStatusFilter = (s) => {
      if (s === "in_review") return "open";
      if (s === "resolved") return "closed";
      return s;
    };
    const statusDb = mapStatusFilter(statusRaw);
    const limit = Math.min(Math.max(parseInt(req.query?.limit, 10) || 50, 1), 200);
    const offset = Math.max(parseInt(req.query?.offset, 10) || 0, 0);

    let q = supabase
      .from("support_requests")
      .select("id, user_id, type, target_user_id, category, message, status, created_at")
      .order("created_at", { ascending: false })
      .range(offset, offset + limit - 1);

    if (typeRaw) q = q.eq("type", typeRaw);
    if (statusDb) q = q.eq("status", statusDb);

    const { data, error, count } = await q;
    if (error) return res.status(500).json({ error: error.message || "Erreur lecture support_requests" });

    const items = Array.isArray(data) ? data : [];
    const userIds = new Set();
    for (const it of items) {
      if (it?.user_id) userIds.add(String(it.user_id));
      if (it?.target_user_id) userIds.add(String(it.target_user_id));
    }

    const ids = Array.from(userIds.values());
    const profById = new Map();
    const emailById = {};

    if (ids.length > 0) {
      const { data: profs, error: pErr } = await supabase
        .from("profiles")
        .select("id, username, full_name, email, is_deleted, deleted_at")
        .in("id", ids);
      if (pErr) return res.status(500).json({ error: pErr.message || "Erreur lecture profils" });
      (profs || []).forEach((p) => {
        if (p?.id) profById.set(String(p.id), p);
      });

      if (supabase?.auth?.admin?.getUserById) {
        await Promise.all(
          ids.map(async (uid) => {
            try {
              const { data: uData, error: uErr } = await supabase.auth.admin.getUserById(uid);
              if (!uErr) {
                const email = String(uData?.user?.email || "").trim();
                if (email) emailById[String(uid)] = email;
              }
            } catch {
              // ignore
            }
          })
        );
      }
    }

    const enriched = items.map((it) => {
      const uid = it?.user_id ? String(it.user_id) : null;
      const tid = it?.target_user_id ? String(it.target_user_id) : null;
      const up = uid ? profById.get(uid) || {} : {};
      const tp = tid ? profById.get(tid) || {} : {};
      return {
        ...it,
        user_username: up.username || null,
        user_full_name: up.full_name || null,
        user_email: uid ? emailById[uid] || null : null,
        target_username: tp.username || null,
        target_full_name: tp.full_name || null,
        target_email: tid ? emailById[tid] || null : null,
      };
    });

    return res.json({ items: enriched, limit, offset, total: count ?? null });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.patch("/api/admin/support/requests/:id", bodyParser.json(), async (req, res) => {
  try {
    const { id } = req.params;
    if (!id) return res.status(400).json({ error: "id requis" });

    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const input = String(req.body?.status || "").toLowerCase();
    const mapStatus = (s) => {
      if (s === "in_review" || s === "open") return "open";
      if (s === "resolved" || s === "closed") return "closed";
      if (s === "new") return "new";
      if (s === "pending") return "pending";
      return null;
    };
    const next = mapStatus(input);
    if (!next) return res.status(400).json({ error: "statut invalide" });

    const { error, data } = await supabase
      .from("support_requests")
      .update({ status: next })
      .eq("id", id)
      .select("id, status")
      .maybeSingle();
    if (error) return res.status(500).json({ error: error.message || "Erreur update support_request" });
    if (!data) return res.status(404).json({ error: "not_found" });
    return res.json({ item: data });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.get("/api/admin/shop-reports", async (req, res) => {
  try {
    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const statusRaw = req.query?.status ? String(req.query.status).toLowerCase() : "";
    const limit = Math.min(Math.max(parseInt(req.query?.limit, 10) || 50, 1), 200);
    const offset = Math.max(parseInt(req.query?.offset, 10) || 0, 0);

    let q = supabase
      .from("marketplace_shop_reports")
      .select("id, shop_id, reporter_id, reason, details, status, created_at")
      .order("created_at", { ascending: false })
      .range(offset, offset + limit - 1);
    if (statusRaw) q = q.eq("status", statusRaw);

    const { data, error, count } = await q;
    if (error) return res.status(500).json({ error: error.message || "Erreur lecture shop_reports" });

    const items = Array.isArray(data) ? data : [];
    const reporterIds = Array.from(new Set(items.map((i) => i?.reporter_id).filter(Boolean).map(String)));
    const profById = new Map();
    const emailById = {};
    if (reporterIds.length > 0) {
      const { data: profs, error: pErr } = await supabase
        .from("profiles")
        .select("id, username, full_name")
        .in("id", reporterIds);
      if (pErr) return res.status(500).json({ error: pErr.message || "Erreur lecture profils" });
      (profs || []).forEach((p) => {
        if (p?.id) profById.set(String(p.id), p);
      });
      if (supabase?.auth?.admin?.getUserById) {
        await Promise.all(
          reporterIds.map(async (uid) => {
            try {
              const { data: uData, error: uErr } = await supabase.auth.admin.getUserById(uid);
              if (!uErr) {
                const email = String(uData?.user?.email || "").trim();
                if (email) emailById[String(uid)] = email;
              }
            } catch {
              // ignore
            }
          })
        );
      }
    }

    const enriched = items.map((it) => {
      const rid = it?.reporter_id ? String(it.reporter_id) : null;
      const rp = rid ? profById.get(rid) || {} : {};
      return {
        ...it,
        reporter_username: rp.username || null,
        reporter_full_name: rp.full_name || null,
        reporter_email: rid ? emailById[rid] || null : null,
      };
    });

    return res.json({ items: enriched, limit, offset, total: count ?? null });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.patch("/api/admin/shop-reports/:id", bodyParser.json(), async (req, res) => {
  try {
    const { id } = req.params;
    if (!id) return res.status(400).json({ error: "id requis" });

    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const allowed = new Set(["open", "closed"]);
    const next = String(req.body?.status || "").toLowerCase();
    if (!allowed.has(next)) return res.status(400).json({ error: "statut invalide" });

    const { data, error } = await supabase
      .from("marketplace_shop_reports")
      .update({ status: next })
      .eq("id", id)
      .select("id, status")
      .maybeSingle();
    if (error) return res.status(500).json({ error: error.message || "Erreur update shop_report" });
    if (!data) return res.status(404).json({ error: "not_found" });
    return res.json({ item: data });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.get("/api/admin/account-deletions", async (req, res) => {
  try {
    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const limit = Math.min(Math.max(parseInt(req.query?.limit, 10) || 100, 1), 500);
    const offset = Math.max(parseInt(req.query?.offset, 10) || 0, 0);
    const { data, error } = await supabase
      .from("account_deletion_logs")
      .select("id, deleted_user_id, reason, created_at")
      .order("created_at", { ascending: false })
      .range(offset, offset + limit - 1);
    if (error) return res.status(500).json({ error: error.message || "Erreur lecture deletions" });

    const items = Array.isArray(data) ? data : [];
    const ids = Array.from(new Set(items.map((i) => i?.deleted_user_id).filter(Boolean).map(String)));
    const profById = new Map();
    const emailById = {};
    if (ids.length > 0) {
      const { data: profs, error: pErr } = await supabase
        .from("profiles")
        .select("id, username, full_name")
        .in("id", ids);
      if (pErr) return res.status(500).json({ error: pErr.message || "Erreur lecture profils" });
      (profs || []).forEach((p) => {
        if (p?.id) profById.set(String(p.id), p);
      });
      if (supabase?.auth?.admin?.getUserById) {
        await Promise.all(
          ids.map(async (uid) => {
            try {
              const { data: uData, error: uErr } = await supabase.auth.admin.getUserById(uid);
              if (!uErr) {
                const email = String(uData?.user?.email || "").trim();
                if (email) emailById[String(uid)] = email;
              }
            } catch {
              // ignore
            }
          })
        );
      }
    }

    const enriched = items.map((it) => {
      const uid = it?.deleted_user_id ? String(it.deleted_user_id) : null;
      const p = uid ? profById.get(uid) || {} : {};
      return {
        ...it,
        username: p.username || null,
        full_name: p.full_name || null,
        email: uid ? (p.email || emailById[uid] || null) : null,
        is_deleted: p?.is_deleted ?? null,
        deleted_at: p?.deleted_at ?? null,
      };
    });

    return res.json({ items: enriched, limit, offset });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.post("/api/partner/connect/login-link", bodyParser.json(), async (req, res) => {
  try {
    const { partnerId } = req.body || {};
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });

    const auth = await requirePartnerOwner({ req, partnerId });
    if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

    const accountId = auth.partner?.stripe_connect_account_id ? String(auth.partner.stripe_connect_account_id) : null;
    if (!accountId) return res.status(400).json({ error: "stripe_connect_account_id manquant" });

    const frontendBase = String(process.env.FRONTEND_URL || "https://onekamer.co").replace(/\/$/, "");
    const redirectUrl = `${frontendBase}/compte`;

    const link = await stripe.accounts.createLoginLink(accountId, { redirect_url: redirectUrl });
    return res.json({ url: link?.url || null });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.use("/api", uploadRoute);
app.use("/api", partenaireDefaultsRoute);
app.use("/api", fixAnnoncesImagesRoute);
app.use("/api", fixEvenementsImagesRoute);
app.use("/api", pushRouter);
app.use("/api", qrcodeRouter);
app.use("/api", iapRouter);

// ============================================================
// 🔑 Stripe
// ============================================================
const stripe = new Stripe(process.env.STRIPE_SECRET_KEY, {
  apiVersion: "2024-06-20",
});

// ============================================================
// 🔑 Supabase
// ============================================================
let supabaseClient = null;
function getSupabaseClient() {
  if (supabaseClient) return supabaseClient;
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) {
    throw new Error("SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY manquante");
  }
  supabaseClient = createClient(url, key);
  return supabaseClient;
}

const supabase = {
  from: (...args) => getSupabaseClient().from(...args),
  rpc: (...args) => getSupabaseClient().rpc(...args),
};

const fxService = createFxService({ supabase, fetchImpl: fetch });

const EU_COUNTRIES = new Set(["AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR","DE","GR","HU","IE","IT","LV","LT","LU","MT","NL","PL","PT","RO","SK","SI","ES","SE"]);

function computeVatSchemeForPartner(p) {
  const cc = String(p?.billing_country_code || p?.country_code || "").toUpperCase();
  const vatOk = String(p?.vat_validation_status || "").toLowerCase() === "valid" && !!p?.vat_number;
  if (!cc) return { scheme: "default_20", rate: 20.0, note: null };
  if (cc === "FR") return { scheme: "fr_20", rate: 20.0, note: null };
  if (EU_COUNTRIES.has(cc)) {
    if (vatOk) return { scheme: "eu_reverse_charge", rate: 0.0, note: "Autoliquidation de la TVA — Article 196 de la Directive 2006/112/CE. TVA due par le preneur." };
    return { scheme: "eu_no_vatid_fr_20", rate: 20.0, note: null };
  }
  return { scheme: "non_eu_out_of_scope", rate: 0.0, note: "Hors champ de la TVA — Article 259-1 du CGI." };
}

function parseMonthPeriod(periodStr) {
  if (!periodStr || !/^\d{4}-\d{2}$/.test(periodStr)) {
    const d = new Date();
    d.setUTCMonth(d.getUTCMonth() - 1, 1);
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, "0");
    const start = new Date(Date.UTC(y, d.getUTCMonth(), 1, 0, 0, 0));
    const end = new Date(Date.UTC(y, d.getUTCMonth() + 1, 0, 23, 59, 59));
    return { start, end, label: `${y}-${m}` };
  }
  const [yStr, mStr] = periodStr.split("-");
  const y = parseInt(yStr, 10);
  const m = parseInt(mStr, 10) - 1;
  const start = new Date(Date.UTC(y, m, 1, 0, 0, 0));
  const end = new Date(Date.UTC(y, m + 1, 0, 23, 59, 59));
  return { start, end, label: `${yStr}-${mStr}` };
}

async function generateNextInvoiceNumber({ supabase, year }) {
  const prefix = `OKI-${year}-`;
  const { data: rows } = await supabase
    .from("market_invoices")
    .select("number")
    .ilike("number", `${prefix}%`)
    .order("number", { ascending: false })
    .limit(1);
  let last = 0;
  if (Array.isArray(rows) && rows.length > 0) {
    const num = String(rows[0].number || "");
    const m = num.match(/OKI-\d{4}-(\d{6})$/);
    if (m && m[1]) last = parseInt(m[1], 10) || 0;
  }
  return function nextCandidate() {
    last += 1;
    return `${prefix}${String(last).padStart(6, "0")}`;
  };
}

async function ensurePartnerAccess(req, partnerId) {
  try {
    const admin = await verifyIsAdminJWT(req);
    if (admin?.ok) return { ok: true };
  } catch {}
  const guard = await requireUserJWT(req);
  if (!guard.ok) return { ok: false, status: guard.status, error: guard.error };
  const { data: p, error } = await supabase
    .from("partners_market")
    .select("id, owner_user_id")
    .eq("id", partnerId)
    .maybeSingle();
  if (error) return { ok: false, status: 500, error: error.message };
  if (!p) return { ok: false, status: 404, error: "partner_not_found" };
  if (String(p.owner_user_id) !== String(guard.userId)) return { ok: false, status: 403, error: "forbidden" };
  return { ok: true };
}

function formatMoneyCents(amountCents) {
  const v = Number(amountCents || 0) / 100;
  return v.toFixed(2);
}

async function buildInvoicePdfBuffer({ invoice, partner, lines }) {
  return await new Promise(async (resolve, reject) => {
    try {
      const doc = new PDFDocument({ size: "A4", margin: 50, bufferPages: true });
      const chunks = [];
      doc.on("data", (b) => chunks.push(b));
      doc.on("end", () => resolve(Buffer.concat(chunks)));
      // En‑tête simplifié; le header/footer final sera dessiné pour chaque page après le contenu
      doc.fillColor("#000");
      try { doc.y = 170; } catch {}
      doc.fontSize(20).text("FACTURE", { align: "right" });
      doc.moveDown(0.5);
      doc.fontSize(10).text(`N°: ${invoice.number}`, { align: "right" });
      doc.text(`Date: ${new Date(invoice.issued_at || Date.now()).toLocaleDateString("fr-FR")}`, { align: "right" });
      doc.text(`Période: ${invoice.period_start} → ${invoice.period_end}`, { align: "right" });
      doc.moveDown(1);
      doc.fontSize(12).font("Helvetica-Bold").text(partner.legal_name || partner.display_name || "Partenaire marketplace");
      doc.fontSize(10).font("Helvetica");
      const billingLines = [
        partner.billing_address_line1,
        partner.billing_address_line2,
        [partner.billing_postcode, partner.billing_city].filter(Boolean).join(" ").trim() || null,
        partner.billing_region || null,
        partner.billing_country_code || null,
      ].filter(Boolean);
      if (billingLines.length) doc.text(billingLines.join("\n"));
      if (partner.vat_number) doc.text(`N° TVA: ${partner.vat_number}`);
      if (partner.billing_email) doc.text(`Email facturation: ${partner.billing_email}`);
      doc.moveDown(1);
      doc.font("Helvetica-Bold").text("Description", 50, doc.y, { continued: true });
      doc.text("HT (€)", 350, undefined, { width: 70, align: "right", continued: true });
      doc.text("TVA (€)", 420, undefined, { width: 60, align: "right", continued: true });
      doc.text("TTC (€)", 480, undefined, { width: 60, align: "right" });
      doc.font("Helvetica");
      lines.forEach((ln) => {
        doc.text(ln.label, 50, doc.y + 8, { continued: true });
        doc.text(formatMoneyCents(ln.service_fee_ht), 350, undefined, { width: 70, align: "right", continued: true });
        doc.text(formatMoneyCents(ln.vat_amount), 420, undefined, { width: 60, align: "right", continued: true });
        doc.text(formatMoneyCents(ln.total_ttc), 480, undefined, { width: 60, align: "right" });
      });
      doc.moveDown(1.5);
      doc.font("Helvetica-Bold").text(`TOTAL HT: ${formatMoneyCents(invoice.total_ht_cents)} €`, { align: "right" });
      doc.text(`TOTAL TVA: ${formatMoneyCents(invoice.total_tva_cents)} €`, { align: "right" });
      doc.text(`TOTAL TTC: ${formatMoneyCents(invoice.total_ttc_cents)} €`, { align: "right" });
      doc.font("Helvetica");
      if (invoice.vat_note) doc.moveDown(1).fontSize(9).text(invoice.vat_note, { align: "left" });

      // Dessiner header/footer et pagination sur chaque page
      try {
        // Charger le logo (PNG) depuis BunnyCDN
        const logoUrl = "https://onekamer-media-cdn.b-cdn.net/logo/IMG_0885%202.PNG";
        let logoBuf = null;
        try {
          const r = await fetch(logoUrl);
          if (r && r.ok) {
            const ab = await r.arrayBuffer();
            logoBuf = Buffer.from(ab);
          }
        } catch {}

        const range = doc.bufferedPageRange();
        for (let i = 0; i < range.count; i++) {
          doc.switchToPage(range.start + i);
          const { width, height, margins } = doc.page;

          // Header: logo à gauche + texte ONEKAMER + ligne verte (ne pas traverser le logo)
          const headerY = 28;
          const logoW = 80; // légère réduction pour éviter chevauchement
          const gap = 12;
          const brandText = "ONEKAMER";
          const brandFontSize = 14;
          if (logoBuf) {
            try { doc.image(logoBuf, margins.left, headerY, { width: logoW }); } catch {}
          }
          // Libellé ONEKAMER à côté du logo
          try {
            doc.font("Helvetica-Bold").fontSize(brandFontSize).fillColor("#000");
            const brandX = margins.left + logoW + gap;
            const brandY = headerY + 30; // abaissé de ~4px
            doc.text(brandText, brandX, brandY);
          } catch {}
          // Ligne verte pleine largeur depuis la marge gauche, sous le logo
          const headerLineY = headerY + logoW + 8;
          const lineStartX = margins.left;
          doc.save()
            .lineWidth(2)
            .strokeColor("#2BA84A")
            .moveTo(lineStartX, headerLineY)
            .lineTo(width - margins.right, headerLineY)
            .stroke()
            .restore();

          // Footer: ligne verte, infos légales, barres couleurs, pagination
          const footerTopY = height - margins.bottom - 56; // abaissé de ~4px
          const footerLineY = footerTopY - 10;
          doc.save()
            .lineWidth(2)
            .strokeColor("#2BA84A")
            .moveTo(margins.left, footerLineY)
            .lineTo(width - margins.right, footerLineY)
            .stroke()
            .restore();

          // Infos légales centrées
          const legalBlockY = footerTopY;
          const legalW = width - margins.left - margins.right;
          doc.fontSize(8).fillColor("#000");
          doc.text("ONEKAMER SAS", margins.left, legalBlockY, { width: legalW, align: "center" });
          doc.text("60 Rue François 1er - 75008 Paris", margins.left, undefined, { width: legalW, align: "center" });
          doc.text("Email : contact@onekamer.co", margins.left, undefined, { width: legalW, align: "center" });
          doc.text("SAS au capital social de 2,00€", margins.left, undefined, { width: legalW, align: "center" });
          doc.text("SIREN 991 019 720 — TVA FR 54991019720", margins.left, undefined, { width: legalW, align: "center" });

          // Pagination en bas droite (au-dessus des barres)
          const pageNumText = `Page ${i + 1}/${range.count}`;
          doc.fontSize(8).fillColor("#000").text(pageNumText, width - margins.right - 60, height - margins.bottom - 24, { width: 60, align: "right" });

          // Barres couleurs en bas — 3 segments juxtaposés: vert, puis rouge, puis jaune
          const barH = 10;
          const barY = height - barH;
          const segW = Math.floor(width / 3);
          const lastW = width - segW * 2; // pour absorber l'arrondi
          // Vert (gauche)
          doc.save().rect(0, barY, segW, barH).fill("#2BA84A").restore();
          // Rouge (milieu)
          doc.save().rect(segW, barY, segW, barH).fill("#D62828").restore();
          // Jaune (droite)
          doc.save().rect(segW * 2, barY, lastW, barH).fill("#FFC107").restore();
        }
      } catch {}

      doc.end();
    } catch (e) {
      reject(e);
    }
  });
}

async function uploadInvoicePdf({ bucket = "invoices", path, buffer }) {
  const sb = getSupabaseClient();
  const { error } = await sb.storage.from(bucket).upload(path, buffer, { contentType: "application/pdf", upsert: true });
  if (error) throw new Error(error.message || "storage_upload_failed");
  return { bucket, path };
}

// ============================================================
// 👍 Intérêts Annonces / Événements (API only, sans fallback front)
// ============================================================
app.post("/api/annonces/:annonceId/interest", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });
    const userId = guard.userId;
    const { annonceId } = req.params;
    if (!annonceId) return res.status(400).json({ error: "annonceId requis" });

    const { data: existing } = await supabase
      .from("annonces_interests")
      .select("id")
      .eq("annonce_id", annonceId)
      .eq("user_id", userId)
      .maybeSingle();

    if (existing) {
      await supabase.from("annonces_interests").delete().eq("id", existing.id);
    } else {
      await supabase.from("annonces_interests").insert({ annonce_id: annonceId, user_id: userId });
    }

    const { count } = await supabase
      .from("annonces_interests")
      .select("id", { count: "exact", head: true })
      .eq("annonce_id", annonceId);
    const interestsCount = Number(count || 0);
    await supabase.from("annonces").update({ interests_count: interestsCount }).eq("id", annonceId);

    if (!existing && interestsCount > 0 && interestsCount % 5 === 0) {
      const { data: annonceRow } = await supabase
        .from("annonces")
        .select("id, user_id, titre")
        .eq("id", annonceId)
        .maybeSingle();
      const authorId = annonceRow?.user_id;
      if (authorId && String(authorId) !== String(userId)) {
        const title = "Annonces";
        const message = `Votre annonce "${annonceRow?.titre || ""}" a atteint ${interestsCount} intéressés.`;
        const url = `/annonces?annonceId=${encodeURIComponent(String(annonceId))}`;
        try {
          await localDispatchNotification({ title, message, targetUserIds: [authorId], url, data: { type: "annonce_interest_milestone", annonceId, milestone: interestsCount } });
        } catch {}
      }
    }

    return res.json({ interested: !existing, interests_count: interestsCount });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.get("/api/annonces/:annonceId/interest/status", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });
    const userId = guard.userId;
    const { annonceId } = req.params;
    if (!annonceId) return res.status(400).json({ error: "annonceId requis" });

    const { data: row } = await supabase
      .from("annonces_interests")
      .select("id")
      .eq("annonce_id", annonceId)
      .eq("user_id", userId)
      .maybeSingle();

    const { count } = await supabase
      .from("annonces_interests")
      .select("id", { count: "exact", head: true })
      .eq("annonce_id", annonceId);
    const interestsCount = Number(count || 0);

    return res.json({ interested: !!row, interests_count: interestsCount });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.post("/api/evenements/:eventId/interest", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });
    const userId = guard.userId;
    const { eventId } = req.params;
    if (!eventId) return res.status(400).json({ error: "eventId requis" });

    const { data: existing } = await supabase
      .from("evenements_interests")
      .select("id")
      .eq("event_id", eventId)
      .eq("user_id", userId)
      .maybeSingle();

    if (existing) {
      await supabase.from("evenements_interests").delete().eq("id", existing.id);
    } else {
      await supabase.from("evenements_interests").insert({ event_id: eventId, user_id: userId });
    }

    const { count } = await supabase
      .from("evenements_interests")
      .select("id", { count: "exact", head: true })
      .eq("event_id", eventId);
    const interestsCount = Number(count || 0);
    await supabase.from("evenements").update({ interests_count: interestsCount }).eq("id", eventId);

    if (!existing && interestsCount > 0 && interestsCount % 5 === 0) {
      const { data: eventRow } = await supabase
        .from("evenements")
        .select("id, user_id, title")
        .eq("id", eventId)
        .maybeSingle();
      const authorId = eventRow?.user_id;
      if (authorId && String(authorId) !== String(userId)) {
        const title = "Événements";
        const message = `Votre événement "${eventRow?.title || ""}" a atteint ${interestsCount} intéressés.`;
        const url = `/evenements?eventId=${encodeURIComponent(String(eventId))}`;
        try {
          await localDispatchNotification({ title, message, targetUserIds: [authorId], url, data: { type: "evenement_interest_milestone", eventId, milestone: interestsCount } });
        } catch {}
      }
    }

    return res.json({ interested: !existing, interests_count: interestsCount });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.get("/api/evenements/:eventId/interest/status", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });
    const userId = guard.userId;
    const { eventId } = req.params;
    if (!eventId) return res.status(400).json({ error: "eventId requis" });

    const { data: row } = await supabase
      .from("evenements_interests")
      .select("id")
      .eq("event_id", eventId)
      .eq("user_id", userId)
      .maybeSingle();

    const { count } = await supabase
      .from("evenements_interests")
      .select("id", { count: "exact", head: true })
      .eq("event_id", eventId);
    const interestsCount = Number(count || 0);

    return res.json({ interested: !!row, interests_count: interestsCount });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.get("/api/market/fx-rate", async (req, res) => {
  try {
    const from = String(req.query.from || "").trim().toUpperCase();
    const to = String(req.query.to || "").trim().toUpperCase();
    if (!from || !to) return res.status(400).json({ error: "from et to requis" });

    const rate = await fxService.getRate(from, to);
    return res.json({ from, to, rate });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "fx_error" });
  }
});

// Admin Marketplace - Commandes (align LAB)
app.get("/api/admin/market/orders", async (req, res) => {
  try {
    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const partnerId = req.query.partnerId ? String(req.query.partnerId).trim() : "";
    const statusFilter = req.query.status ? String(req.query.status).trim().toLowerCase() : "";
    const fulfillment = req.query.fulfillment ? String(req.query.fulfillment).trim().toLowerCase() : "";
    const orderNumberRaw = req.query.orderNumber ? String(req.query.orderNumber).trim() : "";
    const search = req.query.search ? String(req.query.search).trim() : "";
    const limitRaw = req.query.limit;
    const offsetRaw = req.query.offset;
    const limit = Math.min(Math.max(parseInt(limitRaw, 10) || 50, 1), 200);
    const offset = Math.max(parseInt(offsetRaw, 10) || 0, 0);

    let q = supabase
      .from("partner_orders")
      .select(
        "id, partner_id, customer_user_id, order_number, status, fulfillment_status, payout_release_at, charge_currency, charge_amount_total, platform_fee_amount, partner_amount, created_at, updated_at",
        { count: "exact" }
      )
      .order("created_at", { ascending: false });

    if (partnerId) q = q.eq("partner_id", partnerId);

    if (statusFilter && statusFilter !== "all") {
      const allowed = ["pending", "paid", "refunded", "disputed", "failed", "canceled", "cancelled"];
      if (!allowed.includes(statusFilter)) return res.status(400).json({ error: "invalid_status" });
      q = q.eq("status", statusFilter);
    }

    if (fulfillment) {
      const allowedF = ["sent_to_seller", "preparing", "shipping", "delivered", "completed"];
      if (!allowedF.includes(fulfillment)) return res.status(400).json({ error: "invalid_fulfillment" });
      q = q.eq("fulfillment_status", fulfillment);
    }

    let orderNumber = null;
    if (orderNumberRaw && /^\d+$/.test(orderNumberRaw)) orderNumber = parseInt(orderNumberRaw, 10);
    if (!orderNumber && search && /^\d+$/.test(search)) orderNumber = parseInt(search, 10);
    if (Number.isInteger(orderNumber)) q = q.eq("order_number", orderNumber);

    const { data: rows, error, count } = await q.range(offset, offset + limit - 1);
    if (error) return res.status(500).json({ error: error.message || "Erreur lecture commandes" });

    const partnerIds = Array.isArray(rows) ? Array.from(new Set(rows.map((r) => r?.partner_id).filter(Boolean))) : [];
    const partnerById = new Map();
    if (partnerIds.length > 0) {
      const { data: partners } = await supabase.from("partners_market").select("id, display_name").in("id", partnerIds);
      (partners || []).forEach((p) => partnerById.set(String(p.id), p));
    }

    const orders = (rows || []).map((o) => ({
      ...o,
      partner_display_name: partnerById.get(String(o.partner_id))?.display_name || null,
    }));

    return res.json({ orders, count: typeof count === "number" ? count : null, limit, offset });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.get("/api/admin/market/orders/:orderId", async (req, res) => {
  try {
    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const { orderId } = req.params;
    if (!orderId) return res.status(400).json({ error: "orderId requis" });

    const { data: order, error: oErr } = await supabase
      .from("partner_orders")
      .select("id, partner_id, customer_user_id, order_number, status, fulfillment_status, payout_release_at, charge_currency, charge_amount_total, platform_fee_amount, partner_amount, destination_account, checkout_session_id, payment_intent_id, transfer_id, transfer_status, transferred_at, transfer_error, created_at, updated_at")
      .eq("id", orderId)
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commande" });
    if (!order) return res.status(404).json({ error: "order_not_found" });

    const { data: items, error: iErr } = await supabase
      .from("partner_order_items")
      .select("id, item_id, title_snapshot, unit_base_price_amount, quantity, total_base_amount")
      .eq("order_id", orderId)
      .order("created_at", { ascending: true });
    if (iErr) return res.status(500).json({ error: iErr.message || "Erreur lecture lignes commande" });

    const { data: partner } = await supabase
      .from("partners_market")
      .select("id, display_name, owner_user_id")
      .eq("id", order.partner_id)
      .maybeSingle();

    return res.json({ order, items: items || [], partner });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.patch("/api/admin/market/orders/:orderId/status", bodyParser.json(), async (req, res) => {
  try {
    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const { orderId } = req.params;
    const { status, reason } = req.body || {};
    if (!orderId) return res.status(400).json({ error: "orderId requis" });

    const st = String(status || "").toLowerCase();
    const allowed = ["refunded", "disputed"];
    if (!allowed.includes(st)) return res.status(400).json({ error: "invalid_status" });

    const { data: order, error: oErr } = await supabase
      .from("partner_orders")
      .select("id, partner_id, customer_user_id, status")
      .eq("id", orderId)
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commande" });
    if (!order) return res.status(404).json({ error: "order_not_found" });

    if (String(order.status || "").toLowerCase() !== st) {
      const { error: uErr } = await supabase
        .from("partner_orders")
        .update({ status: st, updated_at: new Date().toISOString() })
        .eq("id", orderId);
      if (uErr) return res.status(500).json({ error: uErr.message || "Erreur mise à jour statut" });
    }

    let sellerId = null;
    try {
      const { data: partner } = await supabase.from("partners_market").select("id, owner_user_id").eq("id", order.partner_id).maybeSingle();
      sellerId = partner?.owner_user_id ? String(partner.owner_user_id) : null;
    } catch {}

    const buyerId = order?.customer_user_id ? String(order.customer_user_id) : null;
    const titleMap = { refunded: "a été remboursée", disputed: "a été mise en litige" };
    const code = await getOrderDisplayCode(orderId);
    const msg = `Commande n°${code} ${titleMap[st] || ''}`.trim() + `\nCliquez pour voir le statut de la commande.`;

    try {
      if (buyerId) {
        await sendSupabaseLightPush(req, {
          title: "Marketplace",
          message: msg,
          targetUserIds: [buyerId],
          data: { type: "market_order_admin_status", orderId, status: st, orderNumber: code },
          url: `/market/orders/${orderId}`,
        });
      }
    } catch {}

    try {
      if (sellerId) {
        await sendSupabaseLightPush(req, {
          title: "Marketplace",
          message: msg,
          targetUserIds: [sellerId],
          data: { type: "market_order_admin_status", orderId, status: st, orderNumber: code },
          url: `/market/orders/${orderId}`,
        });
      }
    } catch {}

    return res.json({ ok: true, orderId, status: st });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.patch("/api/admin/market/orders/:orderId/fulfillment", bodyParser.json(), async (req, res) => {
  try {
    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const { orderId } = req.params;
    const { fulfillment_status, reason } = req.body || {};
    if (!orderId) return res.status(400).json({ error: "orderId requis" });

    const fs = String(fulfillment_status || "").toLowerCase();
    if (fs !== "completed") return res.status(400).json({ error: "invalid_fulfillment" });

    const { data: order, error: oErr } = await supabase
      .from("partner_orders")
      .select("id, partner_id, customer_user_id, fulfillment_status")
      .eq("id", orderId)
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commande" });
    if (!order) return res.status(404).json({ error: "order_not_found" });

    if (String(order.fulfillment_status || "").toLowerCase() !== "completed") {
      const nowIso = new Date().toISOString();
      const { error: uErr } = await supabase
        .from("partner_orders")
        .update({ fulfillment_status: "completed", fulfillment_updated_at: nowIso, updated_at: nowIso })
        .eq("id", orderId);
      if (uErr) return res.status(500).json({ error: uErr.message || "Erreur mise à jour commande" });
    }

    let sellerId = null;
    try {
      const { data: partner } = await supabase.from("partners_market").select("id, owner_user_id").eq("id", order.partner_id).maybeSingle();
      sellerId = partner?.owner_user_id ? String(partner.owner_user_id) : null;
    } catch {}
    const buyerId = order?.customer_user_id ? String(order.customer_user_id) : null;

    const code = await getOrderDisplayCode(orderId);
    const title = "Marketplace";
    const message = `Commande n°${code} a été terminée par l’admin\nCliquez pour voir le statut de la commande.`;
    try {
      if (buyerId) {
        await sendSupabaseLightPush(req, {
          title,
          message,
          targetUserIds: [buyerId],
          url: `/market/orders/${orderId}`,
          data: { type: "market_order_admin_fulfillment", orderId, fulfillment_status: "completed", orderNumber: code },
        });
      }
    } catch {}

    try {
      if (sellerId) {
        await sendSupabaseLightPush(req, {
          title,
          message,
          targetUserIds: [sellerId],
          url: `/market/orders/${orderId}`,
          data: { type: "market_order_admin_fulfillment", orderId, fulfillment_status: "completed", orderNumber: code },
        });
      }
    } catch {}

    return res.json({ ok: true, orderId, fulfillment_status: "completed" });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.post("/api/admin/market/orders/:orderId/refund", bodyParser.json(), async (req, res) => {
  try {
    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const { orderId } = req.params;
    if (!orderId) return res.status(400).json({ error: "orderId requis" });

    const amountRaw = req.body?.amount_minor;
    const reason = req.body?.reason ? String(req.body.reason) : null;

    const { data: order, error: oErr } = await supabase
      .from("partner_orders")
      .select("id, partner_id, customer_user_id, charge_amount_total, charge_currency, status")
      .eq("id", orderId)
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commande" });
    if (!order) return res.status(404).json({ error: "order_not_found" });

    const nowIso = new Date().toISOString();
    const { error: uErr } = await supabase
      .from("partner_orders")
      .update({ status: "refunded", updated_at: nowIso })
      .eq("id", orderId);
    if (uErr) return res.status(500).json({ error: uErr.message || "Erreur mise à jour statut" });

    const buyerId = order?.customer_user_id ? String(order.customer_user_id) : null;
    let sellerId = null;
    try {
      const { data: partner } = await supabase
        .from("partners_market")
        .select("id, owner_user_id")
        .eq("id", order.partner_id)
        .maybeSingle();
      sellerId = partner?.owner_user_id ? String(partner.owner_user_id) : null;
    } catch {}

    const amountMinor = Number.isFinite(parseInt(amountRaw, 10)) ? parseInt(amountRaw, 10) : null;
    const cur = String(order.charge_currency || "").toUpperCase();
    const amtStr = amountMinor != null ? `${(amountMinor / 100).toFixed(2)} ${cur || ""}`.trim() : null;
    const code = await getOrderDisplayCode(orderId);
    const title = "Marketplace";
    const message = `Commande n°${code} a été remboursée\nCliquez pour voir le statut de la commande.`;

    try {
      if (buyerId) {
        await sendSupabaseLightPush(req, {
          title,
          message,
          targetUserIds: [buyerId],
          url: `/market/orders/${orderId}`,
          data: { type: "market_order_admin_refund", orderId, amount_minor: amountMinor, orderNumber: code },
        });
      }
    } catch {}

    try {
      if (sellerId) {
        await sendSupabaseLightPush(req, {
          title,
          message,
          targetUserIds: [sellerId],
          url: `/market/orders/${orderId}`,
          data: { type: "market_order_admin_refund", orderId, amount_minor: amountMinor, orderNumber: code },
        });
      }
    } catch {}

    return res.json({ ok: true, orderId, status: "refunded" });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Obtenir (ou régénérer) une session de paiement Stripe Checkout pour une commande acheteur
app.get("/api/market/orders/:orderId/pay", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { orderId } = req.params;
    if (!orderId) return res.status(400).json({ error: "orderId requis" });

    const { data: order, error: oErr } = await supabase
      .from("partner_orders")
      .select("id, partner_id, customer_user_id, status, delivery_mode, charge_currency, charge_amount_total, platform_fee_amount")
      .eq("id", orderId)
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commande" });
    if (!order) return res.status(404).json({ error: "order_not_found" });
    if (order.customer_user_id !== guard.userId) return res.status(403).json({ error: "forbidden" });

    const rawStatus = String(order.status || '').toLowerCase();
    if (rawStatus === 'paid') return res.status(400).json({ error: "order_already_paid" });
    if (rawStatus === 'cancelled' || rawStatus === 'canceled') return res.status(400).json({ error: "order_cancelled" });

    // 1) Réutiliser une session Stripe ouverte si disponible
    let lastSessionId = null;
    try {
      const { data: pays } = await supabase
        .from("partner_order_payments")
        .select("stripe_checkout_session_id, provider")
        .eq("order_id", orderId)
        .eq("provider", "stripe")
        .order("created_at", { ascending: false })
        .limit(1);
      if (Array.isArray(pays) && pays[0]?.stripe_checkout_session_id) {
        lastSessionId = String(pays[0].stripe_checkout_session_id);
      }
    } catch {}

    if (lastSessionId) {
      try {
        const session = await stripe.checkout.sessions.retrieve(lastSessionId);
        const okOrder = String(session?.metadata?.market_order_id || '') === String(orderId);
        const isOpen = String(session?.status || '').toLowerCase() === 'open';
        if (okOrder && isOpen && session?.url) {
          return res.json({ url: session.url });
        }
      } catch {}
    }

    // 2) Créer une nouvelle session Stripe Checkout
    const { data: partner, error: pErr } = await supabase
      .from("partners_market")
      .select("id, status, payout_status, is_open, stripe_connect_account_id")
      .eq("id", order.partner_id)
      .maybeSingle();
    if (pErr) return res.status(500).json({ error: pErr.message || "Erreur lecture partenaire" });
    if (!partner) return res.status(404).json({ error: "partner_not_found" });

    const currency = String(order.charge_currency || "").toLowerCase();
    const unitAmount = Number(order.charge_amount_total);
    if (!currency || !Number.isFinite(unitAmount) || unitAmount <= 0) {
      return res.status(400).json({ error: "order_amount_invalid" });
    }

    const destinationAccount = partner?.stripe_connect_account_id ? String(partner.stripe_connect_account_id) : null;
    if (!destinationAccount) return res.status(400).json({ error: "partner_connect_account_missing" });

    let applicationFeeAmount = Number(order.platform_fee_amount);
    if (!Number.isFinite(applicationFeeAmount) || applicationFeeAmount < 0) applicationFeeAmount = 0;
    if (applicationFeeAmount > unitAmount) return res.status(400).json({ error: "order_fee_too_high" });

    const frontendBase = String(process.env.FRONTEND_URL || "").replace(/\/$/, "");
    if (!frontendBase) return res.status(500).json({ error: "FRONTEND_URL manquant" });

    const sessionStripe = await stripe.checkout.sessions.create({
      payment_method_types: ["card"],
      payment_intent_data: {
        application_fee_amount: applicationFeeAmount,
        transfer_data: { destination: destinationAccount },
      },
      billing_address_collection: "required",
      phone_number_collection: { enabled: true },
      line_items: [
        {
          price_data: {
            currency,
            product_data: { name: "Commande Partenaire — OneKamer" },
            unit_amount: unitAmount,
          },
          quantity: 1,
        },
      ],
      mode: "payment",
      success_url: `${frontendBase}/paiement-success?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${frontendBase}/paiement-annule`,
      metadata: { market_order_id: orderId, partner_id: order.partner_id, customer_user_id: guard.userId },
    });

    await supabase.from("partner_order_payments").insert({
      order_id: orderId,
      provider: "stripe",
      stripe_checkout_session_id: sessionStripe.id,
      status: "pending",
    });

    await supabase.from("partner_orders").update({ status: "pending" }).eq("id", orderId);

    return res.json({ url: sessionStripe.url });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Annuler une commande (par acheteur) si non payée
app.post("/api/market/orders/:orderId/cancel", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { orderId } = req.params;
    if (!orderId) return res.status(400).json({ error: "orderId requis" });

    const { data: order, error: oErr } = await supabase
      .from("partner_orders")
      .select("id, customer_user_id, status")
      .eq("id", orderId)
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commande" });
    if (!order) return res.status(404).json({ error: "order_not_found" });
    if (order.customer_user_id !== guard.userId) return res.status(403).json({ error: "forbidden" });

    const s = String(order.status || '').toLowerCase();
    if (s === 'paid') return res.status(400).json({ error: "order_already_paid" });

    const { error } = await supabase
      .from("partner_orders")
      .update({ status: "cancelled", fulfillment_status: "completed", fulfillment_updated_at: new Date().toISOString(), updated_at: new Date().toISOString() })
      .eq("id", orderId);
    if (error) return res.status(500).json({ error: error.message || "Erreur annulation" });

    return res.json({ success: true });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.post("/api/events/:eventId/intent", async (req, res) => {
  const { eventId } = req.params;

  try {
    const authHeader = req.headers["authorization"] || "";
    const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : null;
    if (!token) return res.status(401).json({ error: "unauthorized" });

    const supabaseAuth = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);
    const { data: userData, error: userErr } = await supabaseAuth.auth.getUser(token);
    if (userErr || !userData?.user) return res.status(401).json({ error: "invalid_token" });

    const userId = userData.user.id;
    const { payment_mode } = req.body || {};
    const paymentMode = payment_mode === "deposit" ? "deposit" : "full";

    if (!eventId) return res.status(400).json({ error: "eventId requis" });

    const { data: ev, error: evErr } = await supabase
      .from("evenements")
      .select("id, title, price_amount, currency, deposit_percent")
      .eq("id", eventId)
      .maybeSingle();
    if (evErr) throw new Error(evErr.message);
    if (!ev) return res.status(404).json({ error: "event_not_found" });

    const amountTotal = typeof ev.price_amount === "number" ? ev.price_amount : 0;
    const currency = ev.currency ? String(ev.currency).toLowerCase() : null;
    const depositPercent = typeof ev.deposit_percent === "number" ? ev.deposit_percent : null;

    if (!currency || !["eur", "usd", "cad", "xaf"].includes(currency)) {
      return res.status(400).json({ error: "currency_invalid" });
    }
    if (!amountTotal || amountTotal <= 0) {
      return res.status(400).json({ error: "event_not_payable" });
    }

    const { data: pay, error: payErr } = await supabase
      .from("event_payments")
      .select("amount_total, amount_paid, status")
      .eq("event_id", eventId)
      .eq("user_id", userId)
      .maybeSingle();
    if (payErr) throw new Error(payErr.message);

    const alreadyPaid = typeof pay?.amount_paid === "number" ? pay.amount_paid : 0;
    const remaining = Math.max(amountTotal - alreadyPaid, 0);
    if (remaining <= 0) {
      return res.status(200).json({ alreadyPaid: true, message: "Déjà payé" });
    }

    let amountToPay = remaining;
    if (paymentMode === "deposit") {
      if (!depositPercent || depositPercent <= 0) {
        return res.status(400).json({ error: "deposit_not_enabled" });
      }
      const depositAmount = Math.max(1, Math.round((amountTotal * depositPercent) / 100));
      amountToPay = Math.min(depositAmount, remaining);
    }

    const { error: upErr } = await supabase
      .from("event_payments")
      .upsert(
        {
          event_id: eventId,
          user_id: userId,
          status: pay?.status || "unpaid",
          amount_total: amountTotal,
          amount_paid: alreadyPaid,
          currency,
          updated_at: new Date().toISOString(),
        },
        { onConflict: "event_id,user_id" }
      );
    if (upErr) throw new Error(upErr.message);

    const pi = await stripe.paymentIntents.create({
      amount: amountToPay,
      currency,
      automatic_payment_methods: { enabled: true },
      metadata: { type: "event_payment", eventId: String(eventId), userId, paymentMode },
    });

    await logEvent({
      category: "event_payment",
      action: "pi.create",
      status: "success",
      userId,
      context: { eventId, paymentMode, amountToPay, amountTotal, currency, payment_intent_id: pi.id },
    });

    return res.json({ clientSecret: pi.client_secret });
  } catch (e) {
    console.error("❌ POST /api/events/:eventId/intent:", e);
    await logEvent({
      category: "event_payment",
      action: "pi.create",
      status: "error",
      userId: null,
      context: { eventId: req.params?.eventId || null, error: e?.message || String(e) },
    });
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.post("/api/okcoins/intent", bodyParser.json(), async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { packId } = req.body || {};
    if (!packId) return res.status(400).json({ error: "packId requis" });

    const { data: pack, error: packErr } = await supabase
      .from("okcoins_packs")
      .select("pack_name, price_eur, is_active")
      .eq("id", packId)
      .maybeSingle();
    if (packErr) return res.status(500).json({ error: packErr.message || "Erreur pack" });
    if (!pack || pack.is_active !== true) return res.status(404).json({ error: "pack_inactive" });

    const amount = Math.round(Number(pack.price_eur || 0) * 100);
    if (!Number.isFinite(amount) || amount <= 0) return res.status(400).json({ error: "amount_invalid" });

    const pi = await stripe.paymentIntents.create({
      amount,
      currency: "eur",
      automatic_payment_methods: { enabled: true },
      metadata: { type: "okcoins_pack", packId: String(packId), userId: guard.userId },
    });

    await logEvent({
      category: "okcoins",
      action: "pi.create",
      status: "success",
      userId: guard.userId,
      context: { packId, payment_intent_id: pi.id },
    });

    return res.json({ clientSecret: pi.client_secret });
  } catch (e) {
    await logEvent({ category: "okcoins", action: "pi.create", status: "error", userId: null, context: { error: e?.message || String(e) } });
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// ============================================================
// 6️⃣ OK COINS — Ledger & Withdrawals API (PROD)
// ============================================================

async function getUserOkcBalanceSnapshot(userId) {
  const safeId = String(userId || "").trim();
  if (!safeId) return { coins_balance: 0, points_total: 0, pending: 0, available: 0 };

  const { data: bal } = await supabase
    .from("okcoins_users_balance")
    .select("coins_balance, points_total")
    .eq("user_id", safeId)
    .maybeSingle();

  const coins_balance = Number(bal?.coins_balance || 0);
  const points_total = Number(bal?.points_total || 0);

  const { data: wdRows } = await supabase
    .from("okcoins_withdrawals")
    .select("amount, status")
    .eq("user_id", safeId)
    .in("status", ["requested", "processing"]);

  const pending = Array.isArray(wdRows) ? wdRows.reduce((acc, r) => acc + Number(r?.amount || 0), 0) : 0;
  const available = Math.max(0, coins_balance - pending);
  return { coins_balance, points_total, pending, available };
}

// Décerner automatiquement les badges OK Coins atteints (idempotent)
async function awardOkCoinsBadges(userId) {
  try {
    const safeId = String(userId || '').trim();
    if (!safeId) return;

    // 1) Lire les points actuels
    const { data: bal } = await supabase
      .from('okcoins_users_balance')
      .select('points_total')
      .eq('user_id', safeId)
      .maybeSingle();
    const points = Number(bal?.points_total || 0);
    if (!Number.isFinite(points)) return;

    // 2) Badges OK Coins éligibles
    const { data: allBadges } = await supabase
      .from('badges_ok_coins')
      .select('id, points_required')
      .order('points_required', { ascending: true });
    const eligible = (allBadges || [])
      .filter((b) => Number(b?.points_required || 0) <= points)
      .map((b) => b.id);
    if (!eligible.length) return;

    // 3) Badges déjà attribués
    const { data: existing } = await supabase
      .from('users_badge')
      .select('badge_id')
      .eq('user_id', safeId)
      .in('badge_id', eligible);
    const existingSet = new Set((existing || []).map((r) => r.badge_id));

    // 4) Insérer uniquement les manquants
    const nowIso = new Date().toISOString();
    const toInsert = eligible
      .filter((id) => !existingSet.has(id))
      .map((badge_id) => ({ user_id: safeId, badge_id, unlocked_at: nowIso }));
    if (toInsert.length) {
      const { error: insErr } = await supabase.from('users_badge').insert(toInsert);
      if (insErr) {
        await logEvent({ category: 'okcoins', action: 'badges.award', status: 'error', userId: safeId, context: { error: insErr.message, count: toInsert.length } });
      } else {
        await logEvent({ category: 'okcoins', action: 'badges.award', status: 'success', userId: safeId, context: { count: toInsert.length } });
      }
    }
  } catch (e) {
    try {
      await logEvent({ category: 'okcoins', action: 'badges.award', status: 'error', userId: userId || null, context: { exception: String(e?.message || e) } });
    } catch {}
  }
}

// Solde disponible utilisateur
app.get("/api/okcoins/balance", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const snap = await getUserOkcBalanceSnapshot(guard.userId);
    return res.json(snap);
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Ledger utilisateur (paginé)
app.get("/api/okcoins/ledger", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const limitRaw = req.query.limit;
    const offsetRaw = req.query.offset;
    const limit = Math.min(Math.max(parseInt(limitRaw, 10) || 20, 1), 100);
    const offset = Math.max(parseInt(offsetRaw, 10) || 0, 0);

    const { data: items, error, count } = await supabase
      .from("okcoins_ledger")
      .select("id, created_at, delta, kind, ref_type, ref_id, balance_after, metadata", { count: "exact" })
      .eq("user_id", guard.userId)
      .order("created_at", { ascending: false })
      .range(offset, offset + limit - 1);
    if (error) return res.status(500).json({ error: error.message || "Erreur lecture ledger" });

    const enriched = (items || []).map((it) => {
      const md = it && typeof it.metadata === "object" && it.metadata !== null ? it.metadata : {};
      const anonymous = Boolean(md.anonymous);
      let direction = it.delta >= 0 ? "in" : "out";
      let other_username = null;

      if (String(it.kind) === "donation_in") {
        other_username = anonymous ? "un membre" : (md.sender_username || md.from_username || null);
      } else if (String(it.kind) === "donation_out") {
        other_username = md.receiver_username || md.to_username || null;
      }

      return { ...it, direction, other_username, anonymous };
    });

    // Agrégation des achats IAP (coins) pour l'historique, au cas où une écriture ledger aurait échoué
    try {
      const ledgerTxIds = new Set(
        (enriched || [])
          .map((e) => (e && e.metadata && e.metadata.tx_id ? String(e.metadata.tx_id) : null))
          .filter(Boolean)
      );
      const { data: txs } = await supabase
        .from("iap_transactions")
        .select("id, transaction_id, product_id, product_type, purchased_at, provider, platform, status")
        .eq("user_id", guard.userId)
        .eq("product_type", "coins")
        .order("purchased_at", { ascending: false })
        .limit(limit);

      const prodIds = Array.from(new Set((txs || []).map((t) => t?.product_id).filter(Boolean)));
      let maps = [];
      if (prodIds.length) {
        const { data: m } = await supabase
          .from("iap_product_map")
          .select("store_product_id, pack_id, platform, provider")
          .in("store_product_id", prodIds);
        maps = Array.isArray(m) ? m : [];
      }
      const byProd = new Map(maps.map((r) => [String(r.store_product_id), r.pack_id]));
      const packIds = Array.from(new Set(maps.map((r) => r.pack_id).filter(Boolean)));
      let packs = [];
      if (packIds.length) {
        const { data: p } = await supabase
          .from("okcoins_packs")
          .select("id, coins")
          .in("id", packIds);
        packs = Array.isArray(p) ? p : [];
      }
      const coinsByPack = new Map(packs.map((p) => [p.id, Number(p.coins || 0)]));

      const txEntries = (txs || []).map((tx) => {
        if (ledgerTxIds.has(String(tx.transaction_id))) return null;
        const packId = byProd.get(String(tx.product_id));
        const coins = packId && coinsByPack.has(packId) ? coinsByPack.get(packId) : 0;
        return {
          id: `iap_${tx.id}`,
          created_at: tx.purchased_at || new Date().toISOString(),
          delta: Math.max(0, Number(coins || 0)),
          kind: "purchase_in",
          ref_type: "iap",
          ref_id: packId || null,
          balance_after: null,
          metadata: {
            provider: tx.provider,
            platform: tx.platform,
            productId: tx.product_id,
            tx_id: tx.transaction_id,
          },
          direction: "in",
          other_username: null,
          anonymous: false,
        };
      }).filter(Boolean);

      const merged = [...enriched, ...txEntries];
      merged.sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime());
      return res.json({ items: merged, total: typeof count === "number" ? count : null, limit, offset });
    } catch (_aggErr) {
      return res.json({ items: enriched, total: typeof count === "number" ? count : null, limit, offset });
    }
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Mes retraits (paginé)
app.get("/api/okcoins/withdrawals", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const limitRaw = req.query.limit;
    const offsetRaw = req.query.offset;
    const limit = Math.min(Math.max(parseInt(limitRaw, 10) || 20, 1), 100);
    const offset = Math.max(parseInt(offsetRaw, 10) || 0, 0);

    const { data: items, error, count } = await supabase
      .from("okcoins_withdrawals")
      .select("id, created_at, updated_at, amount, status, balance_at_request, processed_at, refused_at, refused_reason, admin_notes", { count: "exact" })
      .eq("user_id", guard.userId)
      .order("created_at", { ascending: false })
      .range(offset, offset + limit - 1);
    if (error) return res.status(500).json({ error: error.message || "Erreur lecture retraits" });

    return res.json({ items: items || [], total: typeof count === "number" ? count : null, limit, offset });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Créer une demande de retrait (utilisateur)
app.post("/api/okcoins/withdrawals/request", bodyParser.json(), async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const rawAmount = req.body?.amount;
    const amount = Math.floor(Number(rawAmount));
    if (!Number.isFinite(amount) || amount <= 0 || amount < 1000) {
      return res.status(400).json({ error: "amount_invalid" });
    }

    const snap = await getUserOkcBalanceSnapshot(guard.userId);
    if (amount > snap.available) {
      return res.status(400).json({ error: "insufficient_available_balance" });
    }

    const nowIso = new Date().toISOString();
    const payload = {
      user_id: guard.userId,
      amount,
      status: "requested",
      balance_at_request: snap.coins_balance,
      updated_at: nowIso,
    };

    const { data: inserted, error: insErr } = await supabase
      .from("okcoins_withdrawals")
      .insert(payload)
      .select("id")
      .maybeSingle();
    if (insErr) return res.status(500).json({ error: insErr.message || "Erreur création retrait" });

    // Email + Push admin
    try {
      const { data: prof } = await supabase
        .from("profiles")
        .select("username, email")
        .eq("id", guard.userId)
        .maybeSingle();

      const username = prof?.username || "membre";
      const email = prof?.email || "";
      const withdrawalEmail = process.env.WITHDRAWAL_ALERT_EMAIL || "contact@onekamer.co";
      const text = [
        "Nouvelle demande de retrait OK COINS",
        "",
        `Utilisateur : ${username}`,
        `Email : ${email}`,
        `ID utilisateur : ${guard.userId}`,
        `Montant demandé : ${amount.toLocaleString("fr-FR")} pièces`,
        `Date : ${new Date().toLocaleString("fr-FR")}`,
        "",
        "— Notification automatique OneKamer.co",
      ].join("\n");

      await sendEmailViaBrevo({ to: withdrawalEmail, subject: "Nouvelle demande de retrait OK COINS", text });
      await sendAdminWithdrawalPush(req, { username, amount });
    } catch (_n) {}

    await logEvent({ category: "withdrawal", action: "user.request", status: "success", userId: guard.userId, context: { amount, withdrawal_id: inserted?.id || null } });
    return res.json({ id: inserted?.id || null, status: "requested", amount });
  } catch (e) {
    await logEvent({ category: "withdrawal", action: "user.request", status: "error", userId: null, context: { error: e?.message || String(e) } });
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.patch("/api/admin/partenaires/:partnerId", bodyParser.json(), async (req, res) => {
  try {
    const { partnerId } = req.params;
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });

    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const patch = req.body || {};
    const update = {
      updated_at: new Date().toISOString(),
    };

    const allowed = [
      "name",
      "category_id",
      "address",
      "phone",
      "website",
      "email",
      "description",
      "recommandation",
      "latitude",
      "longitude",
      "media_url",
      "media_type",
    ];

    allowed.forEach((k) => {
      if (patch[k] !== undefined) update[k] = patch[k];
    });

    if (Object.keys(update).length === 1) return res.status(400).json({ error: "nothing_to_update" });

    const { error } = await supabase.from("partenaires").update(update).eq("id", partnerId);
    if (error) return res.status(500).json({ error: error.message || "Erreur mise à jour partenaire" });
    return res.json({ success: true });
  } catch (e) {
    console.error("❌ PATCH /api/admin/partenaires/:partnerId:", e);
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.delete("/api/admin/partenaires/:partnerId", async (req, res) => {
  try {
    const { partnerId } = req.params;
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });

    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    await supabase.from("favoris").delete().eq("type_contenu", "partenaire").eq("content_id", partnerId);

    const { error: delErr } = await supabase.from("partenaires").delete().eq("id", partnerId);
    if (delErr) return res.status(500).json({ error: delErr.message || "Erreur suppression partenaire" });
    return res.json({ success: true });
  } catch (e) {
    console.error("❌ DELETE /api/admin/partenaires/:partnerId:", e);
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.patch("/api/admin/faits-divers/:articleId", bodyParser.json(), async (req, res) => {
  try {
    const { articleId } = req.params;
    if (!articleId) return res.status(400).json({ error: "articleId requis" });

    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const patch = req.body || {};
    const update = {
      updated_at: new Date().toISOString(),
    };

    const allowed = ["title", "category_id", "excerpt", "full_content", "image_url"];
    allowed.forEach((k) => {
      if (patch[k] !== undefined) update[k] = patch[k];
    });

    if (Object.keys(update).length === 1) return res.status(400).json({ error: "nothing_to_update" });

    const { error } = await supabase.from("faits_divers").update(update).eq("id", articleId);
    if (error) return res.status(500).json({ error: error.message || "Erreur mise à jour fait divers" });
    return res.json({ success: true });
  } catch (e) {
    console.error("❌ PATCH /api/admin/faits-divers/:articleId:", e);
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.delete("/api/admin/faits-divers/:articleId", async (req, res) => {
  try {
    const { articleId } = req.params;
    if (!articleId) return res.status(400).json({ error: "articleId requis" });

    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    await supabase.from("faits_divers_comments").delete().eq("fait_divers_id", articleId);
    await supabase.from("faits_divers_likes").delete().eq("fait_divers_id", articleId);
    await supabase.from("favoris").delete().eq("type_contenu", "fait_divers").eq("content_id", articleId);

    const { error: delErr } = await supabase.from("faits_divers").delete().eq("id", articleId);
    if (delErr) return res.status(500).json({ error: delErr.message || "Erreur suppression fait divers" });
    return res.json({ success: true });
  } catch (e) {
    console.error("❌ DELETE /api/admin/faits-divers/:articleId:", e);
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.post("/api/partner/connect/onboarding-link", bodyParser.json(), async (req, res) => {
  try {
    const { partnerId } = req.body || {};
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });

    const auth = await requirePartnerOwner({ req, partnerId });
    if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

    let accountId = auth.partner?.stripe_connect_account_id || null;
    if (!accountId) {
      const account = await stripe.accounts.create({
        type: "express",
        capabilities: {
          card_payments: { requested: true },
          transfers: { requested: true },
        },
      });

      accountId = account.id;
      await supabase
        .from("partners_market")
        .update({
          stripe_connect_account_id: accountId,
          payout_status: "incomplete",
          updated_at: new Date().toISOString(),
        })
        .eq("id", partnerId);
    }

    const frontendBase = String(process.env.FRONTEND_URL || "https://onekamer.co").replace(/\/$/, "");
    const returnUrl = `${frontendBase}/compte`;
    const refreshUrl = `${frontendBase}/compte`;

    let link;
    try {
      link = await stripe.accountLinks.create({
        account: accountId,
        refresh_url: refreshUrl,
        return_url: returnUrl,
        type: "account_onboarding",
      });
    } catch (e) {
      if (!isStripeMissingAccountError(e)) throw e;

      await supabase
        .from("partners_market")
        .update({
          stripe_connect_account_id: null,
          payout_status: "incomplete",
          updated_at: new Date().toISOString(),
        })
        .eq("id", partnerId);

      const account = await stripe.accounts.create({
        type: "express",
        capabilities: {
          card_payments: { requested: true },
          transfers: { requested: true },
        },
      });

      accountId = account.id;
      await supabase
        .from("partners_market")
        .update({
          stripe_connect_account_id: accountId,
          payout_status: "incomplete",
          updated_at: new Date().toISOString(),
        })
        .eq("id", partnerId);

      link = await stripe.accountLinks.create({
        account: accountId,
        refresh_url: refreshUrl,
        return_url: returnUrl,
        type: "account_onboarding",
      });
    }

    return res.json({ url: link.url, accountId });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.post("/api/partner/connect/sync-status", bodyParser.json(), async (req, res) => {
  try {
    const { partnerId } = req.body || {};
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });

    const auth = await requirePartnerOwner({ req, partnerId });
    if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

    const accountId = auth.partner?.stripe_connect_account_id ? String(auth.partner.stripe_connect_account_id) : null;
    if (!accountId) return res.status(400).json({ error: "stripe_connect_account_id manquant" });

    let account;
    try {
      account = await stripe.accounts.retrieve(accountId);
    } catch (e) {
      if (!isStripeMissingAccountError(e)) throw e;
      await supabase
        .from("partners_market")
        .update({
          stripe_connect_account_id: null,
          payout_status: "incomplete",
          updated_at: new Date().toISOString(),
        })
        .eq("id", partnerId);
      return res.json({
        ok: true,
        payout_status: "incomplete",
        details_submitted: false,
        charges_enabled: false,
        payouts_enabled: false,
      });
    }

    const detailsSubmitted = account?.details_submitted === true;
    const chargesEnabled = account?.charges_enabled === true;
    const payoutsEnabled = account?.payouts_enabled === true;
    const payoutStatus = detailsSubmitted && chargesEnabled && payoutsEnabled ? "complete" : "incomplete";

    await supabase
      .from("partners_market")
      .update({
        payout_status: payoutStatus,
        updated_at: new Date().toISOString(),
      })
      .eq("id", partnerId);

    await logEvent({
      category: "marketplace",
      action: "connect.status.sync",
      status: "success",
      userId: auth.userId,
      context: {
        partner_id: partnerId,
        stripe_connect_account_id: accountId,
        payout_status: payoutStatus,
        details_submitted: detailsSubmitted,
        charges_enabled: chargesEnabled,
        payouts_enabled: payoutsEnabled,
      },
    });

    return res.json({
      ok: true,
      payout_status: payoutStatus,
      details_submitted: detailsSubmitted,
      charges_enabled: chargesEnabled,
      payouts_enabled: payoutsEnabled,
    });
  } catch (e) {
    await logEvent({
      category: "marketplace",
      action: "connect.status.sync",
      status: "error",
      context: { error: e?.message || String(e) },
    });
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

function countryToCurrency(countryCode) {
  const cc = String(countryCode || "").trim().toUpperCase();
  if (!cc) return "EUR";
  if (cc === "CA") return "CAD";
  if (cc === "GB") return "GBP";
  if (cc === "CH") return "CHF";
  if (cc === "MA") return "MAD";
  const euroCountries = new Set([
    "AT",
    "BE",
    "CY",
    "DE",
    "EE",
    "ES",
    "FI",
    "FR",
    "GR",
    "HR",
    "IE",
    "IT",
    "LT",
    "LU",
    "LV",
    "MT",
    "NL",
    "PT",
    "SI",
    "SK",
  ]);
  if (euroCountries.has(cc)) return "EUR";
  if (cc === "US") return "USD";
  return "USD";
}

async function requireUserJWT(req) {
  const authHeader = req.headers["authorization"] || "";
  const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : null;
  if (!token) return { ok: false, status: 401, error: "unauthorized" };

  const supabaseAuth = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);
  const { data: userData, error: userErr } = await supabaseAuth.auth.getUser(token);
  if (userErr || !userData?.user) return { ok: false, status: 401, error: "invalid_token" };

  return { ok: true, userId: userData.user.id, token };
}

function getRequestIp(req) {
  const xff = req.headers["x-forwarded-for"];
  if (typeof xff === "string" && xff.trim()) return xff.split(",")[0].trim();
  return req.ip || req.connection?.remoteAddress || "";
}

function hashIp(ip) {
  const v = String(ip || "").trim();
  if (!v) return null;
  return crypto.createHash("sha256").update(v).digest("hex");
}

function generateInviteCode() {
  return `OK-${crypto.randomBytes(4).toString("hex").toUpperCase()}`;
}

app.post("/api/invites/my-code", bodyParser.json(), async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { data: existing, error: readErr } = await supabase
      .from("invites")
      .select("code, created_at, revoked_at")
      .eq("inviter_user_id", guard.userId)
      .is("revoked_at", null)
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle();
    if (readErr) return res.status(500).json({ error: readErr.message || "invite_read_failed" });
    if (existing?.code) return res.json({ code: existing.code, created_at: existing.created_at });

    let code = generateInviteCode();
    for (let i = 0; i < 5; i += 1) {
      const { error: insErr } = await supabase
        .from("invites")
        .insert({ code, inviter_user_id: guard.userId });
      if (!insErr) return res.json({ code });
      code = generateInviteCode();
    }

    return res.status(500).json({ error: "invite_create_failed" });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.post("/api/invites/track", bodyParser.json(), async (req, res) => {
  try {
    const { code, event, meta, user_email, user_username } = req.body || {};
    const cleanCode = String(code || "").trim();
    const cleanEvent = String(event || "").trim();
    if (!cleanCode) return res.status(400).json({ error: "missing_code" });
    if (!cleanEvent) return res.status(400).json({ error: "missing_event" });

    const allowed = new Set(["click", "signup", "first_login", "install"]);
    if (!allowed.has(cleanEvent)) return res.status(400).json({ error: "invalid_event" });

    const { data: invite, error: invErr } = await supabase
      .from("invites")
      .select("code, inviter_user_id, revoked_at")
      .eq("code", cleanCode)
      .maybeSingle();
    if (invErr) return res.status(500).json({ error: invErr.message || "invite_read_failed" });
    if (!invite || invite.revoked_at) return res.status(404).json({ error: "invite_not_found" });

    let trackedUserId = null;
    let trackedEmail = null;
    let trackedUsername = null;

    const authHeader = req.headers["authorization"] || "";
    if (authHeader.startsWith("Bearer ")) {
      const guard = await requireUserJWT(req);
      if (guard.ok) {
        trackedUserId = guard.userId;
        const supabaseAuth = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);
        const { data: userData } = await supabaseAuth.auth.getUser(guard.token);
        trackedEmail = userData?.user?.email || null;

        const { data: prof } = await supabase
          .from("profiles")
          .select("username")
          .eq("id", guard.userId)
          .maybeSingle();
        trackedUsername = prof?.username || null;
      }
    }

    if (!trackedEmail && user_email) trackedEmail = String(user_email).trim() || null;
    if (!trackedUsername && user_username) trackedUsername = String(user_username).trim() || null;

    const ip = getRequestIp(req);
    const ipHash = hashIp(ip);
    const ua = String(req.headers["user-agent"] || "").slice(0, 500) || null;

    if (cleanEvent === "click" && ipHash) {
      const since = new Date(Date.now() - 10 * 60 * 1000).toISOString();
      const { data: lastClick } = await supabase
        .from("invite_events")
        .select("id")
        .eq("code", cleanCode)
        .eq("event", "click")
        .eq("ip_hash", ipHash)
        .gte("created_at", since)
        .order("created_at", { ascending: false })
        .limit(1);
      if (Array.isArray(lastClick) && lastClick.length > 0) {
        return res.json({ ok: true, deduped: true });
      }
    }

    const { error: insErr } = await supabase
      .from("invite_events")
      .insert({
        code: cleanCode,
        event: cleanEvent,
        user_id: trackedUserId,
        user_username: trackedUsername,
        user_email: trackedEmail,
        ip_hash: ipHash,
        user_agent: ua,
        meta: meta && typeof meta === "object" ? meta : null,
      });
    if (insErr) return res.status(500).json({ error: insErr.message || "invite_event_insert_failed" });

    return res.json({ ok: true });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.get("/api/invites/my-stats", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const period = String(req.query?.period || "30d").toLowerCase();
    let sinceIso = null;
    if (period === "7d") sinceIso = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
    else if (period === "30d") sinceIso = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();

    const { data: invite, error: invErr } = await supabase
      .from("invites")
      .select("code, created_at")
      .eq("inviter_user_id", guard.userId)
      .is("revoked_at", null)
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle();
    if (invErr) return res.status(500).json({ error: invErr.message || "invite_read_failed" });
    if (!invite?.code) return res.json({ code: null, stats: {}, recent: [] });

    const events = ["click", "signup", "first_login", "install"];
    const stats = {};
    for (const ev of events) {
      let q = supabase
        .from("invite_events")
        .select("id", { count: "exact", head: true })
        .eq("code", invite.code)
        .eq("event", ev);
      if (sinceIso) q = q.gte("created_at", sinceIso);
      const { count, error } = await q;
      if (error) return res.status(500).json({ error: error.message || "stats_read_failed" });
      stats[ev] = count || 0;
    }

    let recentQuery = supabase
      .from("invite_events")
      .select("id, event, created_at, user_id, user_username, user_email, meta")
      .eq("code", invite.code)
      .order("created_at", { ascending: false })
      .limit(10);
    if (sinceIso) recentQuery = recentQuery.gte("created_at", sinceIso);
    const { data: recent, error: recErr } = await recentQuery;
    if (recErr) return res.status(500).json({ error: recErr.message || "events_read_failed" });

    return res.json({ code: invite.code, stats, recent: recent || [] });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// ============================================================
// 🏆 Trophées (PROD) — Backend uniquement
// ============================================================
async function isEligibleForTrophy(userId, trophyKey) {
  const k = String(trophyKey || "").trim().toLowerCase();
  if (!userId || !k) return false;

  if (k === "profile_complete") {
    try {
      const { data: prof } = await supabase
        .from("profiles")
        .select("username, avatar_url, bio")
        .eq("id", userId)
        .maybeSingle();
      const username = String(prof?.username || "").trim();
      const avatar = String(prof?.avatar_url || "").trim();
      const bio = String(prof?.bio || "").trim();
      return username.length > 0 && avatar.length > 0 && bio.length > 0;
    } catch {
      return false;
    }
  }

  if (k === "first_post") {
    try {
      const { data } = await supabase
        .from("posts")
        .select("id")
        .eq("user_id", userId)
        .limit(1);
      return Array.isArray(data) && data.length > 0;
    } catch {
      return false;
    }
  }

  if (k === "first_referral") {
    try {
      const { data: invites } = await supabase
        .from("invites")
        .select("code")
        .eq("inviter_user_id", userId)
        .is("revoked_at", null)
        .limit(1000);
      const codes = Array.isArray(invites) ? invites.map((i) => i?.code).filter(Boolean) : [];
      if (codes.length === 0) return false;
      const { data: ev } = await supabase
        .from("invite_events")
        .select("id")
        .in("code", codes.slice(0, 1000))
        .eq("event", "signup")
        .limit(1);
      return Array.isArray(ev) && ev.length > 0;
    } catch {
      return false;
    }
  }

  if (k === "first_comment") {
    try {
      const { data } = await supabase
        .from("comments")
        .select("id")
        .eq("user_id", userId)
        .limit(1);
      return Array.isArray(data) && data.length > 0;
    } catch {
      return false;
    }
  }

  if (k === "first_mention") {
    try {
      const { data } = await supabase
        .from("comments")
        .select("id")
        .eq("user_id", userId)
        .ilike("content", "%@%")
        .limit(1);
      return Array.isArray(data) && data.length > 0;
    } catch {
      return false;
    }
  }

  if (k === "first_group") {
    try {
      const { data } = await supabase
        .from("groupes")
        .select("id")
        .eq("fondateur_id", userId)
        .limit(1);
      return Array.isArray(data) && data.length > 0;
    } catch {
      return false;
    }
  }

  if (k === "first_annonce") {
    try {
      const { data } = await supabase
        .from("annonces")
        .select("id")
        .eq("user_id", userId)
        .limit(1);
      return Array.isArray(data) && data.length > 0;
    } catch {
      return false;
    }
  }

  if (k === "first_event") {
    try {
      const { data } = await supabase
        .from("evenements")
        .select("id")
        .eq("user_id", userId)
        .limit(1);
      return Array.isArray(data) && data.length > 0;
    } catch {
      return false;
    }
  }

  return false;
}

const DEFAULT_TROPHIES = [
  { key: "profile_complete", name: "Profil complet", description: "Ajoutez un avatar, une bio et un pseudo.", category: "Profil", icon_url: null },
  { key: "first_post", name: "Première publication", description: "Publiez votre premier post.", category: "Publication", icon_url: null },
  { key: "first_referral", name: "Ambassadeur junior", description: "Faites inscrire un membre via votre lien.", category: "Communauté", icon_url: null },
  { key: "first_comment", name: "Premier commentaire", description: "Publiez votre premier commentaire.", category: "Communauté", icon_url: null },
  { key: "first_mention", name: "Première mention", description: "Mentionnez quelqu'un avec @pseudo.", category: "Communauté", icon_url: null },
  { key: "first_group", name: "Créateur de groupe", description: "Créez votre premier groupe.", category: "Communauté", icon_url: null },
  { key: "first_annonce", name: "Première annonce", description: "Publiez votre première annonce.", category: "Annonces", icon_url: null },
  { key: "first_event", name: "Premier événement", description: "Organisez votre premier événement.", category: "Événements", icon_url: null },
];

app.get("/api/trophies/my", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { data: existingRows, error: existErr } = await supabase
      .from("trophies")
      .select("id, key");
    if (existErr) return res.status(500).json({ error: existErr.message || "trophies_read_failed" });
    const existingKeys = new Set((Array.isArray(existingRows) ? existingRows : []).map((r) => String(r.key)));
    const toInsert = DEFAULT_TROPHIES.filter((t) => !existingKeys.has(String(t.key)));
    if (toInsert.length > 0) {
      try {
        await supabase.from("trophies").insert(toInsert);
      } catch (e) {
        // conflit silencieux
      }
    }

    const { data: allTrophies, error: tErr } = await supabase
      .from("trophies")
      .select("id, key, name, description, category, icon_url")
      .order("created_at", { ascending: true });
    if (tErr) return res.status(500).json({ error: tErr.message || "trophies_read_failed" });

    let { data: mine, error: uErr } = await supabase
      .from("user_trophies")
      .select("trophy_key, unlocked_at")
      .eq("user_id", guard.userId);
    if (uErr) return res.status(500).json({ error: uErr.message || "user_trophies_read_failed" });

    // Auto-award: attribuer les trophées manquants si l'utilisateur est déjà éligible
    try {
      const allKeys = (Array.isArray(allTrophies) ? allTrophies : []).map((t) => String(t.key));
      const idByKey = new Map((Array.isArray(allTrophies) ? allTrophies : []).map((t) => [String(t.key), t.id]));
      const unlocked = new Set((Array.isArray(mine) ? mine : []).map((r) => String(r.trophy_key)));
      const toCheck = allKeys.filter((k) => !unlocked.has(k));
      const eligibleKeys = [];
      for (const k of toCheck) {
        try {
          const ok = await isEligibleForTrophy(guard.userId, k);
          if (ok) eligibleKeys.push(k);
        } catch {}
      }
      if (eligibleKeys.length > 0) {
        const nowIso = new Date().toISOString();
        try {
          await supabase
            .from("user_trophies")
            .insert(
              eligibleKeys
                .filter((k) => idByKey.has(k))
                .map((k) => ({ user_id: guard.userId, trophy_id: idByKey.get(k), trophy_key: k, unlocked_at: nowIso }))
            );
        } catch {}
        const reread = await supabase
          .from("user_trophies")
          .select("trophy_key, unlocked_at")
          .eq("user_id", guard.userId);
        if (!reread.error) mine = Array.isArray(reread.data) ? reread.data : mine;
      }
    } catch {}

    const byKey = new Map((Array.isArray(mine) ? mine : []).map((r) => [String(r.trophy_key), r]));
    const items = (Array.isArray(allTrophies) ? allTrophies : []).map((t) => {
      const k = String(t.key);
      const got = byKey.get(k) || null;
      return {
        key: t.key,
        name: t.name,
        description: t.description,
        category: t.category,
        icon_url: t.icon_url || null,
        unlocked: !!got,
        unlocked_at: got?.unlocked_at || null,
      };
    });

    try {
      res.set("Cache-Control", "no-store");
      res.set("Vary", "Authorization");
    } catch {}
    return res.json({ items });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.post("/api/trophies/award", bodyParser.json(), async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const rawKey = req.body?.trophy_key;
    const trophyKey = String(rawKey || "").trim().toLowerCase();
    if (!trophyKey) return res.status(400).json({ error: "missing_trophy_key" });

    const { data: trophy, error: tErr } = await supabase
      .from("trophies")
      .select("id, key, name, description, category, icon_url")
      .eq("key", trophyKey)
      .maybeSingle();
    if (tErr) return res.status(500).json({ error: tErr.message || "trophy_read_failed" });
    if (!trophy) return res.status(404).json({ error: "trophy_not_found" });

    const { data: already, error: aErr } = await supabase
      .from("user_trophies")
      .select("trophy_key, unlocked_at")
      .eq("user_id", guard.userId)
      .eq("trophy_key", trophyKey)
      .maybeSingle();
    if (aErr) return res.status(500).json({ error: aErr.message || "user_trophies_read_failed" });
    if (already?.trophy_key) {
      return res.json({ already_awarded: true, item: { trophy_key: already.trophy_key, unlocked_at: already.unlocked_at } });
    }

    const eligible = await isEligibleForTrophy(guard.userId, trophyKey);
    if (!eligible) return res.status(400).json({ error: "not_eligible" });

    const nowIso = new Date().toISOString();
    const { error: insErr } = await supabase
      .from("user_trophies")
      .insert({ user_id: guard.userId, trophy_id: trophy.id, trophy_key: trophyKey, unlocked_at: nowIso });
    if (insErr) return res.status(500).json({ error: insErr.message || "user_trophy_insert_failed" });

    return res.json({ awarded: true, item: { trophy_key: trophyKey, unlocked_at: nowIso } });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.post("/api/presence/heartbeat", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { data: prof, error: pErr } = await supabase
      .from("profiles")
      .select("id, show_online_status")
      .eq("id", guard.userId)
      .maybeSingle();
    if (pErr) return res.status(500).json({ error: pErr.message || "profile_read_failed" });
    if (!prof) return res.status(404).json({ error: "profile_not_found" });

    if (prof.show_online_status !== true) {
      return res.json({ ok: true, updated: false });
    }

    const now = new Date().toISOString();
    const { error: upErr } = await supabase
      .from("profiles")
      .update({ last_seen_at: now, updated_at: now })
      .eq("id", guard.userId);
    if (upErr) return res.status(500).json({ error: upErr.message || "profile_update_failed" });
    return res.json({ ok: true, updated: true, last_seen_at: now });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

async function getActiveFeeSettings(currency) {
  const cur = String(currency || "").trim().toUpperCase();
  const { data, error } = await supabase
    .from("marketplace_fee_settings")
    .select("currency, percent_bps, fixed_fee_amount")
    .eq("currency", cur)
    .eq("is_active", true)
    .maybeSingle();
  if (error || !data) return null;
  return data;
}

function isStripeMissingAccountError(err) {
  const code = err?.code ? String(err.code) : "";
  const msg = err?.message ? String(err.message) : "";
  return (
    code === "resource_missing" ||
    /no such account/i.test(msg) ||
    /not connected to your platform/i.test(msg) ||
    /account\s+that\s+is\s+not\s+connected\s+to\s+your\s+platform/i.test(msg)
  );
}

async function requirePartnerOwner({ req, partnerId }) {
  const guard = await requireUserJWT(req);
  if (!guard.ok) return guard;

  const { data: partner, error: pErr } = await supabase
    .from("partners_market")
    .select("id, owner_user_id, stripe_connect_account_id, payout_status, has_accepted_marketplace_terms_vendors, marketplace_terms_vendors_version, accepted_marketplace_terms_vendors_at")
    .eq("id", partnerId)
    .maybeSingle();

  if (pErr) return { ok: false, status: 500, error: pErr.message || "partner_read_failed" };
  if (!partner) return { ok: false, status: 404, error: "partner_not_found" };
  if (partner.owner_user_id !== guard.userId) return { ok: false, status: 403, error: "forbidden" };
  return { ok: true, userId: guard.userId, partner };
}

function isVendorTermsCompliant(partner) {
  const accepted = partner?.has_accepted_marketplace_terms_vendors === true;
  const version = String(partner?.marketplace_terms_vendors_version || "");
  return accepted && version === CURRENT_VENDOR_TERMS_VERSION;
}

async function requireVipOrAdminUser({ req }) {
  const guard = await requireUserJWT(req);
  if (!guard.ok) return guard;

  const { data: profile, error } = await supabase
    .from("profiles")
    .select("id, plan, role, is_admin")
    .eq("id", guard.userId)
    .maybeSingle();

  if (error) return { ok: false, status: 500, error: error.message || "profile_read_failed" };
  if (!profile) return { ok: false, status: 404, error: "profile_not_found" };

  const plan = String(profile.plan || "free").toLowerCase();
  const isAdmin = Boolean(profile.is_admin) || String(profile.role || "").toLowerCase() === "admin";
  const isVip = plan === "vip";
  if (!isAdmin) {
    const { data: sub, error: subErr } = await supabase
      .from("abonnements")
      .select("is_permanent, end_date")
      .eq("profile_id", guard.userId)
      .order("end_date", { ascending: false })
      .limit(1)
      .maybeSingle();
    if (subErr) return { ok: false, status: 500, error: subErr.message || "subscription_read_failed" };
    const now = new Date();
    const active = sub?.end_date ? new Date(sub.end_date) > now : false;
    const allowed = (sub?.is_permanent === true) || (isVip && active);
    if (!allowed) return { ok: false, status: 403, error: "vip_required" };
  }

  return { ok: true, userId: guard.userId, token: guard.token, profile };
}

app.get("/api/market/partners", async (_req, res) => {
  try {
    const { data, error } = await supabase
      .from("partners_market")
      .select(
        "id, display_name, description, category, country_code, base_currency, status, payout_status, is_open, logo_url, phone, whatsapp, address, hours, created_at"
      )
      .order("created_at", { ascending: false });

    if (error) return res.status(500).json({ error: error.message || "Erreur lecture partenaires" });

    const partners = (data || []).map((p) => {
      const isApproved = String(p.status || "").toLowerCase() === "approved";
      const payoutComplete = String(p.payout_status || "").toLowerCase() === "complete";
      const isOpen = p.is_open === true;
      const commandable = isApproved && payoutComplete && isOpen;
      return { ...p, commandable };
    });

    return res.json({ partners });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.get("/api/market/partners/me", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { data: partner, error } = await supabase
      .from("partners_market")
      .select(
        "id, owner_user_id, display_name, description, category, status, payout_status, stripe_connect_account_id, is_open, logo_url, phone, whatsapp, address, hours, created_at, updated_at, has_accepted_marketplace_terms_vendors, marketplace_terms_vendors_version, accepted_marketplace_terms_vendors_at"
      )
      .eq("owner_user_id", guard.userId)
      .maybeSingle();

    if (error) return res.status(500).json({ error: error.message || "Erreur lecture boutique" });
    const partnerOut = partner ? { ...partner, vendor_terms_compliant: isVendorTermsCompliant(partner) } : null;
    return res.json({ partner: partnerOut });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Accepter la charte Marketplace vendeur
app.post("/api/market/partners/:partnerId/terms/accept", async (req, res) => {
  try {
    const { partnerId } = req.params;
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });

    const auth = await requirePartnerOwner({ req, partnerId });
    if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

    const now = new Date().toISOString();
    const { error } = await supabase
      .from("partners_market")
      .update({
        has_accepted_marketplace_terms_vendors: true,
        marketplace_terms_vendors_version: CURRENT_VENDOR_TERMS_VERSION,
        accepted_marketplace_terms_vendors_at: now,
        updated_at: now,
      })
      .eq("id", partnerId);
    if (error) return res.status(500).json({ error: error.message || "partner_update_failed" });
    return res.json({ ok: true, version: CURRENT_VENDOR_TERMS_VERSION });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.get("/api/market/partners/:partnerId/orders", async (req, res) => {
  try {
    const { partnerId } = req.params;
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });

    const auth = await requirePartnerOwner({ req, partnerId });
    if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

    const statusFilter = String(req.query.status || "all").trim().toLowerCase();
    const fulfillmentFilter = String(req.query.fulfillment || "all").trim().toLowerCase();
    const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 50, 1), 200);
    const offset = Math.max(parseInt(req.query.offset, 10) || 0, 0);

    let query = supabase
      .from("partner_orders")
      .select(
        "id, partner_id, customer_user_id, status, delivery_mode, customer_note, customer_country_code, base_currency, base_amount_total, charge_currency, charge_amount_total, platform_fee_amount, partner_amount, fulfillment_status, fulfillment_updated_at, created_at, updated_at, order_number"
      )
      .eq("partner_id", partnerId)
      .order("created_at", { ascending: false })
      .range(offset, offset + limit - 1);

    const paidStates = ["paid", "refunded", "disputed"];
    query = query.in("status", paidStates);

    if (!fulfillmentFilter || fulfillmentFilter === "all") {
      if (statusFilter && statusFilter !== "all") {
        if (statusFilter === "pending") {
          query = query.eq("status", "pending");
        } else if (statusFilter === "paid") {
          query = query.eq("status", "paid");
        } else if (statusFilter === "canceled" || statusFilter === "cancelled") {
          query = query.in("status", ["canceled", "cancelled"]);
        } else {
          query = query.eq("status", statusFilter);
        }
      }
    }

    const { data: orders, error: oErr } = await query;
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commandes" });

    let safeOrders = Array.isArray(orders) ? orders : [];
    const normalizeFulfillment = (o) => {
      const s = String(o?.status || '').toLowerCase();
      if (s === 'canceled' || s === 'cancelled') return 'completed';
      const f = String(o?.fulfillment_status || '').toLowerCase();
      if (f === 'canceled' || f === 'cancelled') return 'completed';
      if (paidStates.includes(s)) {
        return f || 'sent_to_seller';
      }
      return o?.fulfillment_status || null;
    };

    if (fulfillmentFilter && fulfillmentFilter !== 'all') {
      const list = fulfillmentFilter.split(',').map((x) => x.trim().toLowerCase()).filter(Boolean);
      safeOrders = safeOrders.filter((o) => list.includes(normalizeFulfillment(o)));
    }

    const orderIds = safeOrders.map((o) => o.id).filter(Boolean);

    let itemsByOrderId = {};
    if (orderIds.length > 0) {
      const { data: items, error: iErr } = await supabase
        .from("partner_order_items")
        .select("id, order_id, item_id, title_snapshot, unit_base_price_amount, quantity, total_base_amount")
        .in("order_id", orderIds)
        .order("created_at", { ascending: true });

      if (iErr) return res.status(500).json({ error: iErr.message || "Erreur lecture lignes commande" });

      itemsByOrderId = (items || []).reduce((acc, it) => {
        const oid = it?.order_id ? String(it.order_id) : null;
        if (!oid) return acc;
        if (!acc[oid]) acc[oid] = [];
        acc[oid].push(it);
        return acc;
      }, {});
    }

    const uniqueCustomerIds = Array.from(
      new Set(safeOrders.map((o) => (o?.customer_user_id ? String(o.customer_user_id) : null)).filter(Boolean))
    );

    const aliasByUserId = {};
    if (uniqueCustomerIds.length > 0) {
      uniqueCustomerIds.forEach((uid) => {
        aliasByUserId[uid] = `#${uid.slice(0, 6)}`;
      });
    }

    const enriched = safeOrders.map((o) => {
      const oid = o?.id ? String(o.id) : null;
      const uid = o?.customer_user_id ? String(o.customer_user_id) : null;
      const f = normalizeFulfillment(o);
      return {
        ...o,
        fulfillment_status: f,
        customer_alias: uid ? aliasByUserId[uid] || `#${uid.slice(0, 6)}` : null,
        items: oid ? itemsByOrderId[oid] || [] : [],
      };
    });

    return res.json({ orders: enriched, limit, offset });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.post("/api/market/partners/:partnerId/orders/:orderId/mark-received", async (req, res) => {
  try {
    const { partnerId, orderId } = req.params;
    if (!partnerId || !orderId) return res.status(400).json({ error: "partnerId et orderId requis" });

    const auth = await requirePartnerOwner({ req, partnerId });
    if (!auth.ok) return res.status(auth.status).json({ error: auth.error });
    if (!isVendorTermsCompliant(auth.partner)) return res.status(403).json({ error: "vendor_terms_required" });

    const { data: order, error: oErr } = await supabase
      .from("partner_orders")
      .select("id, partner_id, status, fulfillment_status")
      .eq("id", orderId)
      .eq("partner_id", partnerId)
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commande" });
    if (!order) return res.status(404).json({ error: "order_not_found" });

    const s = String(order.status || "").toLowerCase();
    const f = String(order.fulfillment_status || "").toLowerCase();
    if (!["paid", "refunded", "disputed"].includes(s)) return res.status(400).json({ error: "order_payment_invalid" });
    if (f !== "sent_to_seller") return res.status(400).json({ error: "fulfillment_transition_invalid" });

    const { error: upErr } = await supabase
      .from("partner_orders")
      .update({
        fulfillment_status: "preparing",
        fulfillment_updated_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      })
      .eq("id", orderId)
      .eq("partner_id", partnerId);
    if (upErr) return res.status(500).json({ error: upErr.message || "update_failed" });

    return res.json({ success: true, fulfillment_status: "preparing" });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Refuser une commande (vendeur) au moment d'accepter
app.post("/api/market/partners/:partnerId/orders/:orderId/refuse", bodyParser.json(), async (req, res) => {
  try {
    const { partnerId, orderId } = req.params;
    const { reason } = req.body || {};
    if (!partnerId || !orderId) return res.status(400).json({ error: "partnerId et orderId requis" });

    const auth = await requirePartnerOwner({ req, partnerId });
    if (!auth.ok) return res.status(auth.status).json({ error: auth.error });
    if (!isVendorTermsCompliant(auth.partner)) return res.status(403).json({ error: "vendor_terms_required" });

    const { data: order, error: oErr } = await supabase
      .from("partner_orders")
      .select("id, partner_id, customer_user_id, status, fulfillment_status")
      .eq("id", orderId)
      .eq("partner_id", partnerId)
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commande" });
    if (!order) return res.status(404).json({ error: "order_not_found" });

    const s = String(order.status || "").toLowerCase();
    const f = String(order.fulfillment_status || "").toLowerCase();
    if (s !== "paid") return res.status(400).json({ error: "order_payment_invalid" });
    if (f !== "sent_to_seller") return res.status(400).json({ error: "fulfillment_transition_invalid" });

    const now = new Date().toISOString();
    const { error: upErr } = await supabase
      .from("partner_orders")
      .update({ status: "refunded", fulfillment_status: "canceled", fulfillment_updated_at: now, updated_at: now })
      .eq("id", orderId)
      .eq("partner_id", partnerId);
    if (upErr) return res.status(500).json({ error: upErr.message || "Erreur mise à jour" });

    try {
      let convId = null;
      const { data: conv } = await supabase
        .from("marketplace_order_conversations")
        .select("id")
        .eq("order_id", orderId)
        .maybeSingle();
      if (conv?.id) {
        convId = conv.id;
      } else {
        const { data: inserted } = await supabase
          .from("marketplace_order_conversations")
          .insert({ order_id: orderId, buyer_id: order.customer_user_id, seller_id: auth.partner?.owner_user_id || null, created_at: now })
          .select("id")
          .maybeSingle();
        convId = inserted?.id || null;
      }
      if (convId) {
        const text = reason ? `Commande annulée par le vendeur. Motif: ${String(reason).slice(0, 500)}` : "Commande annulée par le vendeur.";
        await supabase
          .from("marketplace_order_messages")
          .insert({ conversation_id: convId, author_id: auth.userId || null, body: text, created_at: now });
      }
    } catch {}

    try {
      const code = await getOrderDisplayCode(orderId);
      await sendSupabaseLightPush(req, {
        title: "Marketplace",
        message: `Commande n°${code}\nA été annulée par le vendeur. Un remboursement sera effectué dans les 48h.`,
        targetUserIds: [String(order.customer_user_id)],
        data: { type: "market_order_canceled", orderId, orderNumber: code },
        url: `/market/orders/${orderId}`,
      });
    } catch {}

    try {
      const { data: admins } = await supabase.from("profiles").select("id").eq("is_admin", true);
      const targets = Array.isArray(admins) ? admins.map((a) => a.id).filter(Boolean) : [];
      if (targets.length > 0) {
        const code = await getOrderDisplayCode(orderId);
        await sendSupabaseLightPush(req, {
          title: "Marketplace",
          message: `Remboursement manuel requis. Commande n°${code} annulée par le vendeur.`,
          targetUserIds: targets,
          data: { type: "market_admin_manual_refund", orderId, partnerId, reason: reason || null, orderNumber: code },
          url: "/admin/payments",
        });
      }
    } catch {}

    return res.json({ ok: true });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Annuler une commande (vendeur) tant que non expédiée
app.post("/api/market/partners/:partnerId/orders/:orderId/cancel", bodyParser.json(), async (req, res) => {
  try {
    const { partnerId, orderId } = req.params;
    const { reason } = req.body || {};
    if (!partnerId || !orderId) return res.status(400).json({ error: "partnerId et orderId requis" });

    const auth = await requirePartnerOwner({ req, partnerId });
    if (!auth.ok) return res.status(auth.status).json({ error: auth.error });
    if (!isVendorTermsCompliant(auth.partner)) return res.status(403).json({ error: "vendor_terms_required" });

    const { data: order, error: oErr } = await supabase
      .from("partner_orders")
      .select("id, partner_id, customer_user_id, status, fulfillment_status")
      .eq("id", orderId)
      .eq("partner_id", partnerId)
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commande" });
    if (!order) return res.status(404).json({ error: "order_not_found" });

    const s = String(order.status || "").toLowerCase();
    const f = String(order.fulfillment_status || "").toLowerCase();
    if (s !== "paid") return res.status(400).json({ error: "order_payment_invalid" });
    if (["shipping", "delivered", "completed"].includes(f)) return res.status(400).json({ error: "fulfillment_transition_invalid" });

    const now = new Date().toISOString();
    const { error: upErr } = await supabase
      .from("partner_orders")
      .update({ status: "refunded", fulfillment_status: "canceled", fulfillment_updated_at: now, updated_at: now })
      .eq("id", orderId)
      .eq("partner_id", partnerId);
    if (upErr) return res.status(500).json({ error: upErr.message || "Erreur mise à jour" });

    try {
      let convId = null;
      const { data: conv } = await supabase
        .from("marketplace_order_conversations")
        .select("id")
        .eq("order_id", orderId)
        .maybeSingle();
      if (conv?.id) {
        convId = conv.id;
      } else {
        const { data: inserted } = await supabase
          .from("marketplace_order_conversations")
          .insert({ order_id: orderId, buyer_id: order.customer_user_id, seller_id: auth.partner?.owner_user_id || null, created_at: now })
          .select("id")
          .maybeSingle();
        convId = inserted?.id || null;
      }
      if (convId) {
        const text = reason ? `Commande annulée par le vendeur. Motif: ${String(reason).slice(0, 500)}` : "Commande annulée par le vendeur.";
        await supabase
          .from("marketplace_order_messages")
          .insert({ conversation_id: convId, author_id: auth.userId || null, body: text, created_at: now });
      }
    } catch {}

    try {
      const code = await getOrderDisplayCode(orderId);
      await sendSupabaseLightPush(req, {
        title: "Marketplace",
        message: `Commande n°${code}\nA été annulée par le vendeur. Un remboursement sera effectué dans les 48h.`,
        targetUserIds: [String(order.customer_user_id)],
        data: { type: "market_order_canceled", orderId, orderNumber: code },
        url: `/market/orders/${orderId}`,
      });
    } catch {}

    try {
      const { data: admins } = await supabase.from("profiles").select("id").eq("is_admin", true);
      const targets = Array.isArray(admins) ? admins.map((a) => a.id).filter(Boolean) : [];
      if (targets.length > 0) {
        const code = await getOrderDisplayCode(orderId);
        await sendSupabaseLightPush(req, {
          title: "Marketplace",
          message: `Remboursement manuel requis. Commande n°${code} annulée par le vendeur.`,
          targetUserIds: targets,
          data: { type: "market_admin_manual_refund", orderId, partnerId, reason: reason || null, orderNumber: code },
          url: "/admin/payments",
        });
      }
    } catch {}

    return res.json({ ok: true });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.put("/api/market/cart", bodyParser.json(), async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { partnerId, items } = req.body || {};
    const pid = partnerId ? String(partnerId).trim() : null;
    if (!pid) return res.status(400).json({ error: "partnerId requis" });
    if (!Array.isArray(items)) return res.status(400).json({ error: "items requis" });

    const normalizedItems = items
      .map((it) => ({
        itemId: it?.itemId ? String(it.itemId).trim() : null,
        quantity: Math.max(parseInt(it?.quantity, 10) || 1, 1),
      }))
      .filter((it) => it.itemId);

    const now = new Date().toISOString();

    const { data: existingCart, error: cReadErr } = await supabase
      .from("market_carts")
      .select("id")
      .eq("user_id", guard.userId)
      .eq("partner_id", pid)
      .eq("status", "active")
      .order("updated_at", { ascending: false })
      .limit(1)
      .maybeSingle();
    if (cReadErr) return res.status(500).json({ error: cReadErr.message || "Erreur lecture panier" });

    let cartId = existingCart?.id ? String(existingCart.id) : null;
    if (!cartId) {
      const { data: inserted, error: cInsErr } = await supabase
        .from("market_carts")
        .insert({ user_id: guard.userId, partner_id: pid, status: "active", created_at: now, updated_at: now })
        .select("id")
        .maybeSingle();
      if (cInsErr) return res.status(500).json({ error: cInsErr.message || "Erreur création panier" });
      cartId = inserted?.id ? String(inserted.id) : null;
    } else {
      const { error: cUpErr } = await supabase
        .from("market_carts")
        .update({ updated_at: now })
        .eq("id", cartId);
      if (cUpErr) return res.status(500).json({ error: cUpErr.message || "Erreur mise à jour panier" });
    }

    if (!cartId) return res.status(500).json({ error: "cart_create_failed" });

    const { error: delErr } = await supabase.from("market_cart_items").delete().eq("cart_id", cartId);
    if (delErr) return res.status(500).json({ error: delErr.message || "Erreur reset items panier" });

    if (normalizedItems.length === 0) {
      return res.json({ success: true, cartId, items: [] });
    }

    const itemIds = normalizedItems.map((x) => x.itemId);
    const { data: dbItems, error: iErr } = await supabase
      .from("partner_items")
      .select("id, partner_id, title, base_price_amount")
      .eq("partner_id", pid)
      .in("id", itemIds);
    if (iErr) return res.status(500).json({ error: iErr.message || "Erreur lecture produits" });

    const byId = new Map((dbItems || []).map((x) => [String(x.id), x]));
    const rows = [];

    for (const it of normalizedItems) {
      const row = byId.get(String(it.itemId));
      if (!row) continue;
      const priceMinor = Number(row.base_price_amount || 0);
      rows.push({
        cart_id: cartId,
        item_id: row.id,
        title_snapshot: row.title || null,
        unit_price_minor: Number.isFinite(priceMinor) ? Math.round(priceMinor) : 0,
        quantity: it.quantity,
      });
    }

    if (rows.length === 0) {
      return res.json({ success: true, cartId, items: [] });
    }

    const { error: insErr } = await supabase.from("market_cart_items").insert(rows);
    if (insErr) return res.status(500).json({ error: insErr.message || "Erreur ajout items panier" });

    return res.json({ success: true, cartId, itemsCount: rows.length });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.get("/api/market/partners/:partnerId/abandoned-carts", async (req, res) => {
  try {
    const { partnerId } = req.params;
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });

    const auth = await requirePartnerOwner({ req, partnerId });
    if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

    const minutes = Math.min(Math.max(parseInt(req.query.minutes, 10) || 60, 5), 4320);
    const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 50, 1), 200);
    const offset = Math.max(parseInt(req.query.offset, 10) || 0, 0);

    const cutoff = new Date(Date.now() - minutes * 60 * 1000).toISOString();

    const { data: carts, error: cErr } = await supabase
      .from("market_carts")
      .select("id, user_id, partner_id, status, created_at, updated_at")
      .eq("partner_id", partnerId)
      .eq("status", "active")
      .lt("updated_at", cutoff)
      .order("updated_at", { ascending: false })
      .range(offset, offset + limit - 1);
    if (cErr) return res.status(500).json({ error: cErr.message || "Erreur lecture paniers" });

    const safeCarts = Array.isArray(carts) ? carts : [];
    const cartIds = safeCarts.map((c) => c.id).filter(Boolean);

    let itemsByCartId = {};
    if (cartIds.length > 0) {
      const { data: items, error: iErr } = await supabase
        .from("market_cart_items")
        .select("id, cart_id, item_id, title_snapshot, unit_price_minor, quantity")
        .in("cart_id", cartIds)
        .order("created_at", { ascending: true });
      if (iErr) return res.status(500).json({ error: iErr.message || "Erreur lecture items panier" });

      itemsByCartId = (items || []).reduce((acc, it) => {
        const cid = it?.cart_id ? String(it.cart_id) : null;
        if (!cid) return acc;
        if (!acc[cid]) acc[cid] = [];
        acc[cid].push(it);
        return acc;
      }, {});
    }

    const uniqueUserIds = Array.from(
      new Set(safeCarts.map((c) => (c?.user_id ? String(c.user_id) : null)).filter(Boolean))
    );
    const emailByUserId = {};
    if (uniqueUserIds.length > 0 && supabase?.auth?.admin?.getUserById) {
      await Promise.all(
        uniqueUserIds.map(async (uid) => {
          try {
            const { data: uData, error: uErr } = await supabase.auth.admin.getUserById(uid);
            if (uErr) return;
            const email = String(uData?.user?.email || "").trim();
            if (email) emailByUserId[uid] = email;
          } catch {
            // ignore
          }
        })
      );
    }

    const enriched = safeCarts.map((c) => {
      const cid = c?.id ? String(c.id) : null;
      const uid = c?.user_id ? String(c.user_id) : null;
      const its = cid ? itemsByCartId[cid] || [] : [];
      const totalMinor = its.reduce(
        (sum, it) => sum + Number(it?.unit_price_minor || 0) * Math.max(parseInt(it?.quantity, 10) || 1, 1),
        0
      );
      return {
        ...c,
        customer_email: uid ? emailByUserId[uid] || null : null,
        items: its,
        total_minor: totalMinor,
        currency: "EUR",
      };
    });

    return res.json({ carts: enriched, cutoff, minutes, limit, offset });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.get("/api/admin/market/partners", async (req, res) => {
  try {
    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const statusFilter = req.query.status ? String(req.query.status).trim().toLowerCase() : "";
    const search = req.query.search ? String(req.query.search).trim() : "";
    const limitRaw = req.query.limit;
    const offsetRaw = req.query.offset;
    const limit = Math.min(Math.max(parseInt(limitRaw, 10) || 50, 1), 200);
    const offset = Math.max(parseInt(offsetRaw, 10) || 0, 0);

    let q = supabase
      .from("partners_market")
      .select(
        "id, owner_user_id, display_name, category, base_currency, status, payout_status, is_open, logo_url, created_at, updated_at",
        { count: "exact" }
      )
      .order("created_at", { ascending: false });

    if (statusFilter && ["pending", "approved", "rejected"].includes(statusFilter)) {
      q = q.eq("status", statusFilter);
    }

    if (search) {
      q = q.ilike("display_name", `%${search}%`);
    }

    const { data: rows, error, count } = await q.range(offset, offset + limit - 1);
    if (error) return res.status(500).json({ error: error.message || "Erreur lecture boutiques" });

    const ownerIds = Array.isArray(rows)
      ? Array.from(new Set(rows.map((r) => r?.owner_user_id).filter(Boolean)))
      : [];

    const ownersById = new Map();
    if (ownerIds.length > 0) {
      const { data: owners, error: oErr } = await supabase.from("profiles").select("id, username, email").in("id", ownerIds);
      if (!oErr && Array.isArray(owners)) {
        owners.forEach((o) => ownersById.set(o.id, o));
      }
    }

    const partners = (rows || []).map((p) => {
      const owner = ownersById.get(p.owner_user_id) || null;
      return {
        ...p,
        owner_username: owner?.username || null,
        owner_email: owner?.email || null,
      };
    });

    return res.json({ partners, count: typeof count === "number" ? count : null, limit, offset });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.patch("/api/admin/market/partners/:partnerId", bodyParser.json(), async (req, res) => {
  try {
    const { partnerId } = req.params;
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });

    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const patch = req.body || {};
    const update = {
      updated_at: new Date().toISOString(),
    };

    if (patch.status !== undefined) {
      const st = String(patch.status || "").trim().toLowerCase();
      if (!["pending", "approved", "rejected"].includes(st)) {
        return res.status(400).json({ error: "invalid_status" });
      }
      update.status = st;
    }

    if (patch.is_open !== undefined) {
      update.is_open = patch.is_open === true;
    }

    if (Object.keys(update).length === 1) {
      return res.status(400).json({ error: "nothing_to_update" });
    }

    const { error } = await supabase.from("partners_market").update(update).eq("id", partnerId);
    if (error) return res.status(500).json({ error: error.message || "Erreur mise à jour boutique" });
    return res.json({ success: true });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.delete("/api/admin/market/partners/:partnerId", async (req, res) => {
  try {
    const { partnerId } = req.params;
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });

    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const { data: orders, error: oErr } = await supabase.from("partner_orders").select("id").eq("partner_id", partnerId).limit(5000);
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commandes" });

    const orderIds = Array.isArray(orders) ? orders.map((o) => o.id).filter(Boolean) : [];
    if (orderIds.length > 0) {
      const { error: delOrderItemsErr } = await supabase.from("partner_order_items").delete().in("order_id", orderIds);
      if (delOrderItemsErr) return res.status(500).json({ error: delOrderItemsErr.message || "Erreur suppression lignes commande" });

      const { error: delOrderPaysErr } = await supabase.from("partner_order_payments").delete().in("order_id", orderIds);
      if (delOrderPaysErr) return res.status(500).json({ error: delOrderPaysErr.message || "Erreur suppression paiements commande" });

      const { error: delOrdersErr } = await supabase.from("partner_orders").delete().in("id", orderIds);
      if (delOrdersErr) return res.status(500).json({ error: delOrdersErr.message || "Erreur suppression commandes" });
    }

    const { error: delItemsErr } = await supabase.from("partner_items").delete().eq("partner_id", partnerId);
    if (delItemsErr) return res.status(500).json({ error: delItemsErr.message || "Erreur suppression produits" });

    const { error: delPartnerErr } = await supabase.from("partners_market").delete().eq("id", partnerId);
    if (delPartnerErr) return res.status(500).json({ error: delPartnerErr.message || "Erreur suppression boutique" });

    return res.json({ success: true });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.get("/api/admin/market/partners/performance", async (req, res) => {
  try {
    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const period = req.query.period ? String(req.query.period).trim().toLowerCase() : "30d";
    const currencyFilter = req.query.currency ? String(req.query.currency).trim().toUpperCase() : "ALL";
    const search = req.query.search ? String(req.query.search).trim() : "";
    const includeEmpty =
      req.query.includeEmpty === true ||
      req.query.includeEmpty === "true" ||
      req.query.includeEmpty === "1";

    const limitRaw = req.query.limit;
    const offsetRaw = req.query.offset;
    const limit = Math.min(Math.max(parseInt(limitRaw, 10) || 50, 1), 200);
    const offset = Math.max(parseInt(offsetRaw, 10) || 0, 0);

    const now = Date.now();
    const daysByPeriod = { "7d": 7, "30d": 30, "90d": 90, "365d": 365 };
    const days = daysByPeriod[period] || null;
    const sinceIso = days ? new Date(now - days * 24 * 60 * 60 * 1000).toISOString() : null;

    const maxRows = 10000;
    const pageSize = 1000;
    let fetched = 0;
    let pageOffset = 0;
    let paidOrders = [];

    while (fetched < maxRows) {
      let q = supabase
        .from("partner_orders")
        .select("id, partner_id, status, charge_currency, charge_amount_total, updated_at, created_at")
        .eq("status", "paid")
        .order("created_at", { ascending: false })
        .range(pageOffset, pageOffset + pageSize - 1);

      if (sinceIso) q = q.gte("created_at", sinceIso);
      if (currencyFilter && currencyFilter !== "ALL") q = q.eq("charge_currency", currencyFilter);

      const { data, error } = await q;
      if (error) return res.status(500).json({ error: error.message || "Erreur lecture commandes" });

      const rows = Array.isArray(data) ? data : [];
      if (rows.length === 0) break;
      paidOrders = paidOrders.concat(rows);
      fetched += rows.length;
      if (rows.length < pageSize) break;
      pageOffset += pageSize;
    }

    const statsByKey = new Map();
    const partnerIds = new Set();

    paidOrders.forEach((o) => {
      const pid = o?.partner_id ? String(o.partner_id) : null;
      const cur = o?.charge_currency ? String(o.charge_currency).toUpperCase() : null;
      const amt = Number(o?.charge_amount_total || 0);
      if (!pid || !cur || !Number.isFinite(amt)) return;

      partnerIds.add(pid);
      const key = `${pid}::${cur}`;
      const existing = statsByKey.get(key) || {
        partner_id: pid,
        currency: cur,
        orders_paid_count: 0,
        revenue_charge_total_minor: 0,
        last_paid_at: null,
      };

      existing.orders_paid_count += 1;
      existing.revenue_charge_total_minor += amt;

      const ts = o?.updated_at || o?.created_at || null;
      if (ts && (!existing.last_paid_at || String(ts) > String(existing.last_paid_at))) {
        existing.last_paid_at = ts;
      }

      statsByKey.set(key, existing);
    });

    const partnerIdList = Array.from(partnerIds);
    const partnerById = new Map();
    if (partnerIdList.length > 0) {
      const { data: partners, error: pErr } = await supabase
        .from("partners_market")
        .select("id, display_name, base_currency")
        .in("id", partnerIdList);
      if (pErr) return res.status(500).json({ error: pErr.message || "Erreur lecture boutiques" });
      (partners || []).forEach((p) => partnerById.set(String(p.id), p));
    }

    let allPartnersById = null;
    if (includeEmpty) {
      let pq = supabase.from("partners_market").select("id, display_name, base_currency, created_at");
      if (search) pq = pq.ilike("display_name", `%${search}%`);
      const { data: allPartners, error: apErr } = await pq.order("created_at", { ascending: false }).limit(2000);
      if (apErr) return res.status(500).json({ error: apErr.message || "Erreur lecture boutiques" });
      allPartnersById = new Map();
      (allPartners || []).forEach((p) => allPartnersById.set(String(p.id), p));
    }

    let rows = Array.from(statsByKey.values()).map((r) => {
      const p = partnerById.get(String(r.partner_id)) || null;
      const avg = r.orders_paid_count > 0 ? Math.round(r.revenue_charge_total_minor / r.orders_paid_count) : 0;
      return {
        ...r,
        avg_basket_minor: avg,
        partner_display_name: p?.display_name || null,
        partner_base_currency: p?.base_currency || null,
      };
    });

    if (search) {
      const searchLower = search.toLowerCase();
      rows = rows.filter((r) => String(r.partner_display_name || "").toLowerCase().includes(searchLower));
    }

    if (includeEmpty && allPartnersById) {
      allPartnersById.forEach((p) => {
        const pid = String(p.id);

        if (currencyFilter && currencyFilter !== "ALL") {
          const key = `${pid}::${currencyFilter}`;
          if (!statsByKey.has(key)) {
            rows.push({
              partner_id: pid,
              currency: currencyFilter,
              orders_paid_count: 0,
              revenue_charge_total_minor: 0,
              last_paid_at: null,
              avg_basket_minor: 0,
              partner_display_name: p.display_name || null,
              partner_base_currency: p.base_currency || null,
            });
          }
          return;
        }

        const baseCur = p.base_currency ? String(p.base_currency).toUpperCase() : "";
        const cur = baseCur || "EUR";
        const exists = rows.some((r) => String(r.partner_id) === pid);
        if (!exists) {
          rows.push({
            partner_id: pid,
            currency: cur,
            orders_paid_count: 0,
            revenue_charge_total_minor: 0,
            last_paid_at: null,
            avg_basket_minor: 0,
            partner_display_name: p.display_name || null,
            partner_base_currency: p.base_currency || null,
          });
        }
      });
    }

    rows.sort((a, b) => {
      const da = Number(a.revenue_charge_total_minor || 0);
      const db = Number(b.revenue_charge_total_minor || 0);
      return db - da;
    });

    const paged = rows.slice(offset, offset + limit);
    return res.json({ rows: paged, count: rows.length, limit, offset, period, currency: currencyFilter });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.post("/api/market/partners", bodyParser.json(), async (req, res) => {
  try {
    const guard = await requireVipOrAdminUser({ req });
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { display_name, description, category, logo_url, phone, whatsapp, address, hours } = req.body || {};

    const name = String(display_name || "").trim();
    const desc = String(description || "").trim();
    const cat = String(category || "").trim();
    const logo = String(logo_url || "").trim();

    if (!name) return res.status(400).json({ error: "display_name requis" });
    if (!desc) return res.status(400).json({ error: "description requise" });
    if (!cat) return res.status(400).json({ error: "category requise" });
    if (!logo) return res.status(400).json({ error: "logo_url requis" });

    const { data: existing, error: exErr } = await supabase
      .from("partners_market")
      .select("id")
      .eq("owner_user_id", guard.userId)
      .maybeSingle();
    if (exErr) return res.status(500).json({ error: exErr.message || "Erreur lecture boutique" });
    if (existing?.id) return res.status(409).json({ error: "partner_already_exists" });

    const now = new Date().toISOString();
    const { data: inserted, error } = await supabase
      .from("partners_market")
      .insert({
        owner_user_id: guard.userId,
        display_name: name,
        description: desc,
        category: cat,
        base_currency: "EUR",
        status: "pending",
        payout_status: "incomplete",
        is_open: false,
        logo_url: logo,
        phone: phone ? String(phone).trim() : null,
        whatsapp: whatsapp ? String(whatsapp).trim() : null,
        address: address ? String(address).trim() : null,
        hours: hours ? String(hours).trim() : null,
        created_at: now,
        updated_at: now,
      })
      .select("id")
      .maybeSingle();

    if (error) return res.status(500).json({ error: error.message || "Erreur création boutique" });
    if (!inserted?.id) return res.status(500).json({ error: "partner_create_failed" });
    return res.json({ success: true, partnerId: inserted.id });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.patch("/api/market/partners/:partnerId", bodyParser.json(), async (req, res) => {
  try {
    const { partnerId } = req.params;
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });

    const auth = await requirePartnerOwner({ req, partnerId });
    if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

    const patch = req.body || {};
    const update = {
      updated_at: new Date().toISOString(),
    };

    if (patch.display_name !== undefined) update.display_name = String(patch.display_name || "").trim();
    if (patch.description !== undefined) update.description = String(patch.description || "").trim();
    if (patch.category !== undefined) update.category = String(patch.category || "").trim();
    if (patch.logo_url !== undefined) update.logo_url = String(patch.logo_url || "").trim();
    if (patch.phone !== undefined) update.phone = patch.phone ? String(patch.phone).trim() : null;
    if (patch.whatsapp !== undefined) update.whatsapp = patch.whatsapp ? String(patch.whatsapp).trim() : null;
    if (patch.address !== undefined) update.address = patch.address ? String(patch.address).trim() : null;
    if (patch.hours !== undefined) update.hours = patch.hours ? String(patch.hours).trim() : null;
    if (patch.is_open !== undefined) update.is_open = !!patch.is_open;

    if ("display_name" in update && !update.display_name) {
      return res.status(400).json({ error: "display_name requis" });
    }
    if ("description" in update && !update.description) {
      return res.status(400).json({ error: "description requise" });
    }
    if ("category" in update && !update.category) {
      return res.status(400).json({ error: "category requise" });
    }
    if ("logo_url" in update && !update.logo_url) {
      return res.status(400).json({ error: "logo_url requis" });
    }

    const { error } = await supabase.from("partners_market").update(update).eq("id", partnerId);
    if (error) return res.status(500).json({ error: error.message || "Erreur mise à jour boutique" });
    return res.json({ success: true });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.get("/api/market/partners/:partnerId/items", async (req, res) => {
  try {
    const { partnerId } = req.params;
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });

    const { data: items, error } = await supabase
      .from("partner_items")
      .select("id, partner_id, type, title, description, base_price_amount, is_available, is_published, media")
      .eq("partner_id", partnerId)
      .eq("is_published", true)
      .order("created_at", { ascending: false });

    if (error) return res.status(500).json({ error: error.message || "Erreur lecture items" });
    return res.json({ items: items || [] });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.get("/api/market/partners/:partnerId/items/manage", async (req, res) => {
  try {
    const { partnerId } = req.params;
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });

    const auth = await requirePartnerOwner({ req, partnerId });
    if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

    const { data: items, error } = await supabase
      .from("partner_items")
      .select("id, partner_id, type, title, description, base_price_amount, is_available, is_published, media, created_at, updated_at")
      .eq("partner_id", partnerId)
      .order("created_at", { ascending: false });

    if (error) return res.status(500).json({ error: error.message || "Erreur lecture items" });
    return res.json({ items: items || [] });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.post("/api/market/partners/:partnerId/items", bodyParser.json(), async (req, res) => {
  try {
    const { partnerId } = req.params;
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });

    const auth = await requirePartnerOwner({ req, partnerId });
    if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

    const payload = req.body || {};
    const title = String(payload.title || "").trim();
    const description = payload.description ? String(payload.description).trim() : null;
    const type = payload.type ? String(payload.type).trim() : "product";

    const basePriceAmount = Number(payload.base_price_amount);
    if (!title) return res.status(400).json({ error: "title requis" });
    if (!Number.isFinite(basePriceAmount) || basePriceAmount < 0) {
      return res.status(400).json({ error: "base_price_amount invalide" });
    }

    const isAvailable = payload.is_available === false ? false : true;
    const isPublished = payload.is_published === true;
    const media = payload.media && typeof payload.media === "object" ? payload.media : null;

    const now = new Date().toISOString();
    const { data, error } = await supabase
      .from("partner_items")
      .insert({
        partner_id: partnerId,
        type,
        title,
        description,
        base_price_amount: Math.round(basePriceAmount),
        is_available: isAvailable,
        is_published: isPublished,
        media,
        created_at: now,
        updated_at: now,
      })
      .select("id")
      .maybeSingle();

    if (error) return res.status(500).json({ error: error.message || "Erreur création item" });
    if (!data?.id) return res.status(500).json({ error: "item_create_failed" });
    return res.json({ success: true, itemId: data.id });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.patch("/api/market/partners/:partnerId/items/:itemId", bodyParser.json(), async (req, res) => {
  try {
    const { partnerId, itemId } = req.params;
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });
    if (!itemId) return res.status(400).json({ error: "itemId requis" });

    const auth = await requirePartnerOwner({ req, partnerId });
    if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

    const patch = req.body || {};
    const update = { updated_at: new Date().toISOString() };

    if (patch.title !== undefined) update.title = String(patch.title || "").trim();
    if (patch.description !== undefined) update.description = patch.description ? String(patch.description).trim() : null;
    if (patch.type !== undefined) update.type = patch.type ? String(patch.type).trim() : "product";
    if (patch.base_price_amount !== undefined) {
      const v = Number(patch.base_price_amount);
      if (!Number.isFinite(v) || v < 0) return res.status(400).json({ error: "base_price_amount invalide" });
      update.base_price_amount = Math.round(v);
    }
    if (patch.is_available !== undefined) update.is_available = patch.is_available === true;
    if (patch.is_published !== undefined) update.is_published = patch.is_published === true;
    if (patch.media !== undefined) update.media = patch.media && typeof patch.media === "object" ? patch.media : null;

    if ("title" in update && !update.title) return res.status(400).json({ error: "title requis" });

    const { data: existing, error: readErr } = await supabase
      .from("partner_items")
      .select("id, partner_id")
      .eq("id", itemId)
      .maybeSingle();
    if (readErr) return res.status(500).json({ error: readErr.message || "Erreur lecture item" });
    if (!existing) return res.status(404).json({ error: "item_not_found" });
    if (String(existing.partner_id) !== String(partnerId)) return res.status(403).json({ error: "forbidden" });

    const { error } = await supabase.from("partner_items").update(update).eq("id", itemId);
    if (error) return res.status(500).json({ error: error.message || "Erreur mise à jour item" });
    return res.json({ success: true });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.delete("/api/market/partners/:partnerId/items/:itemId", async (req, res) => {
  try {
    const { partnerId, itemId } = req.params;
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });
    if (!itemId) return res.status(400).json({ error: "itemId requis" });

    const auth = await requirePartnerOwner({ req, partnerId });
    if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

    const { data: existing, error: readErr } = await supabase
      .from("partner_items")
      .select("id, partner_id")
      .eq("id", itemId)
      .maybeSingle();
    if (readErr) return res.status(500).json({ error: readErr.message || "Erreur lecture item" });
    if (!existing) return res.status(404).json({ error: "item_not_found" });
    if (String(existing.partner_id) !== String(partnerId)) return res.status(403).json({ error: "forbidden" });

    const { error } = await supabase.from("partner_items").delete().eq("id", itemId);
    if (error) return res.status(500).json({ error: error.message || "Erreur suppression item" });
    return res.json({ success: true });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.post("/api/market/orders", bodyParser.json(), async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { partnerId, items, delivery_mode, customer_note } = req.body || {};
    if (!partnerId || !Array.isArray(items) || items.length === 0) {
      return res.status(400).json({ error: "partnerId et items requis" });
    }

    const { data: partner, error: pErr } = await supabase
      .from("partners_market")
      .select("id, status, payout_status, is_open, base_currency")
      .eq("id", partnerId)
      .maybeSingle();
    if (pErr) return res.status(500).json({ error: pErr.message || "Erreur lecture partenaire" });
    if (!partner) return res.status(404).json({ error: "partner_not_found" });

    const baseCurrency = String(partner.base_currency || "").trim().toUpperCase();
    if (!baseCurrency) return res.status(400).json({ error: "partner_base_currency_missing" });

    const { data: prof, error: profErr } = await supabase
      .from("profiles")
      .select("country_code")
      .eq("id", guard.userId)
      .maybeSingle();
    if (profErr) return res.status(500).json({ error: profErr.message || "Erreur lecture profil" });

    const customerCountryCode = String(prof?.country_code || "").trim().toUpperCase() || null;
    const chargeCurrency = countryToCurrency(customerCountryCode);

    const itemIds = items
      .map((it) => String(it?.itemId || "").trim())
      .filter(Boolean);
    if (itemIds.length === 0) return res.status(400).json({ error: "items_invalid" });

    const { data: dbItems, error: iErr } = await supabase
      .from("partner_items")
      .select("id, partner_id, title, base_price_amount, is_available, is_published")
      .eq("partner_id", partnerId)
      .in("id", itemIds);
    if (iErr) return res.status(500).json({ error: iErr.message || "Erreur lecture items" });

    const byId = new Map((dbItems || []).map((x) => [x.id, x]));
    const orderLines = [];
    let baseTotal = 0;

    for (const it of items) {
      const id = String(it?.itemId || "").trim();
      const qty = Math.max(parseInt(it?.quantity, 10) || 1, 1);
      const row = byId.get(id);
      if (!row) return res.status(400).json({ error: `item_not_found:${id}` });
      if (!row.is_published) return res.status(400).json({ error: `item_not_published:${id}` });
      if (!row.is_available) return res.status(400).json({ error: `item_unavailable:${id}` });
      const unit = Number(row.base_price_amount);
      if (!Number.isFinite(unit) || unit < 0) return res.status(400).json({ error: `item_price_invalid:${id}` });
      const lineTotal = unit * qty;
      baseTotal += lineTotal;
      orderLines.push({
        item_id: row.id,
        title_snapshot: row.title,
        unit_base_price_amount: unit,
        quantity: qty,
        total_base_amount: lineTotal,
      });
    }

    // Calcul des frais de livraison (en devise de base boutique)
    const deliveryRaw = String(delivery_mode || "").toLowerCase();
    const shippingType = ["pickup", "standard", "express", "international"].includes(deliveryRaw)
      ? deliveryRaw
      : "pickup";

    let shippingFeeBase = 0;
    if (shippingType !== "pickup") {
      const { data: shipRow, error: shipErr } = await supabase
        .from("shipping_options")
        .select("shipping_type, price_cents, is_active")
        .eq("shop_id", partnerId)
        .eq("shipping_type", shippingType)
        .maybeSingle();
      if (shipErr) return res.status(500).json({ error: shipErr.message || "Erreur lecture options livraison" });
      if (!shipRow || shipRow.is_active !== true) {
        return res.status(400).json({ error: "shipping_option_unavailable" });
      }
      shippingFeeBase = Math.max(parseInt(shipRow.price_cents, 10) || 0, 0);
    }

    const baseTotalWithShip = baseTotal + shippingFeeBase;

    const { amount: chargeTotal, rate } = await fxService.convertMinorAmount({
      amount: baseTotalWithShip,
      fromCurrency: baseCurrency,
      toCurrency: chargeCurrency,
    });

    const fee = await getActiveFeeSettings(chargeCurrency);
    if (!fee) return res.status(400).json({ error: "fee_settings_missing" });

    const percentFee = Math.round((chargeTotal * Number(fee.percent_bps || 0)) / 10000);
    const fixedFee = Number(fee.fixed_fee_amount || 0);
    const platformFee = Math.max(percentFee + fixedFee, 0);
    const partnerAmount = Math.max(chargeTotal - platformFee, 0);

    const modeNorm = String(delivery_mode || "").toLowerCase();

    const { data: inserted, error: oErr } = await supabase
      .from("partner_orders")
      .insert({
        partner_id: partnerId,
        customer_user_id: guard.userId,
        status: "pending",
        delivery_mode: modeNorm && modeNorm !== "pickup" ? "partner_delivery" : "pickup",
        customer_note: customer_note ? String(customer_note) : null,
        customer_country_code: customerCountryCode,
        base_currency: baseCurrency,
        base_amount_total: baseTotal,
        charge_currency: chargeCurrency,
        charge_amount_total: chargeTotal,
        fx_rate_used: rate,
        fx_provider: "frankfurter",
        fx_timestamp: new Date().toISOString(),
        platform_fee_amount: platformFee,
        partner_amount: partnerAmount,
      })
      .select("id")
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur création commande" });

    const orderId = inserted?.id;
    if (!orderId) return res.status(500).json({ error: "order_create_failed" });

    const linesPayload = orderLines.map((l) => ({ ...l, order_id: orderId }));
    const { error: liErr } = await supabase.from("partner_order_items").insert(linesPayload);
    if (liErr) return res.status(500).json({ error: liErr.message || "Erreur création lignes" });

    return res.json({ success: true, orderId });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.post("/api/market/orders/:orderId/checkout", bodyParser.json(), async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { orderId } = req.params;
    if (!orderId) return res.status(400).json({ error: "orderId requis" });

    const { data: order, error: oErr } = await supabase
      .from("partner_orders")
      .select("id, partner_id, customer_user_id, status, charge_currency, charge_amount_total, platform_fee_amount, has_accepted_marketplace_terms_buyers")
      .eq("id", orderId)
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commande" });
    if (!order) return res.status(404).json({ error: "order_not_found" });
    if (order.customer_user_id !== guard.userId) return res.status(403).json({ error: "forbidden" });
    if (String(order.status || "").toLowerCase() !== "pending") {
      return res.status(400).json({ error: "order_status_invalid" });
    }
    if (order.has_accepted_marketplace_terms_buyers !== true) {
      return res.status(400).json({ error: "buyer_terms_required" });
    }
    if (order.has_accepted_marketplace_terms_buyers !== true) {
      return res.status(400).json({ error: "buyer_terms_required" });
    }

    const { data: partner, error: pErr } = await supabase
      .from("partners_market")
      .select("id, status, payout_status, is_open, stripe_connect_account_id")
      .eq("id", order.partner_id)
      .maybeSingle();
    if (pErr) return res.status(500).json({ error: pErr.message || "Erreur lecture partenaire" });
    if (!partner) return res.status(404).json({ error: "partner_not_found" });

    const isApproved = String(partner.status || "").toLowerCase() === "approved";
    const payoutComplete = String(partner.payout_status || "").toLowerCase() === "complete";
    const isOpen = partner.is_open === true;
    if (!(isApproved && payoutComplete && isOpen)) {
      return res.status(400).json({ error: "partner_not_commandable" });
    }

    const currency = String(order.charge_currency || "").toLowerCase();
    const unitAmount = Number(order.charge_amount_total);
    if (!currency || !Number.isFinite(unitAmount) || unitAmount <= 0) {
      return res.status(400).json({ error: "order_amount_invalid" });
    }

    const destinationAccount = partner?.stripe_connect_account_id
      ? String(partner.stripe_connect_account_id)
      : null;
    if (!destinationAccount) {
      return res.status(400).json({ error: "partner_connect_account_missing" });
    }

    const applicationFeeAmount = Number(order.platform_fee_amount);
    if (!Number.isFinite(applicationFeeAmount) || applicationFeeAmount < 0) {
      return res.status(400).json({ error: "order_fee_invalid" });
    }
    if (applicationFeeAmount > unitAmount) {
      return res.status(400).json({ error: "order_fee_too_high" });
    }

    const frontendBase = String(process.env.FRONTEND_URL || "").replace(/\/$/, "");
    if (!frontendBase) return res.status(500).json({ error: "FRONTEND_URL manquant" });

    const sessionStripe = await stripe.checkout.sessions.create({
      payment_method_types: ["card"],
      payment_intent_data: {
        application_fee_amount: applicationFeeAmount,
        transfer_data: { destination: destinationAccount },
      },
      line_items: [
        {
          price_data: {
            currency,
            product_data: { name: "Commande Partenaire — OneKamer" },
            unit_amount: unitAmount,
          },
          quantity: 1,
        },
      ],
      mode: "payment",
      success_url: `${frontendBase}/paiement-success?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${frontendBase}/paiement-annule`,
      metadata: { market_order_id: orderId, partner_id: order.partner_id, customer_user_id: guard.userId },
    });

    await supabase.from("partner_order_payments").insert({
      order_id: orderId,
      stripe_checkout_session_id: sessionStripe.id,
      status: "created",
    });

    await supabase
      .from("partner_orders")
      .update({ status: "pending" })
      .eq("id", orderId);

    return res.json({ success: true, url: sessionStripe.url });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.post("/api/market/orders/:orderId/intent", bodyParser.json(), async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { orderId } = req.params;
    if (!orderId) return res.status(400).json({ error: "orderId requis" });

    const { data: order, error: oErr } = await supabase
      .from("partner_orders")
      .select("id, partner_id, customer_user_id, status, charge_currency, charge_amount_total, platform_fee_amount, has_accepted_marketplace_terms_buyers")
      .eq("id", orderId)
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commande" });
    if (!order) return res.status(404).json({ error: "order_not_found" });
    if (order.customer_user_id !== guard.userId) return res.status(403).json({ error: "forbidden" });
    if (String(order.status || "").toLowerCase() !== "pending") {
      return res.status(400).json({ error: "order_status_invalid" });
    }
    if (order.has_accepted_marketplace_terms_buyers !== true) {
      return res.status(400).json({ error: "buyer_terms_required" });
    }

    const { data: partner, error: pErr } = await supabase
      .from("partners_market")
      .select("id, status, payout_status, is_open, stripe_connect_account_id")
      .eq("id", order.partner_id)
      .maybeSingle();
    if (pErr) return res.status(500).json({ error: pErr.message || "Erreur lecture partenaire" });
    if (!partner) return res.status(404).json({ error: "partner_not_found" });

    const isApproved = String(partner.status || "").toLowerCase() === "approved";
    const payoutComplete = String(partner.payout_status || "").toLowerCase() === "complete";
    const isOpen = partner.is_open === true;
    if (!(isApproved && payoutComplete && isOpen)) {
      return res.status(400).json({ error: "partner_not_commandable" });
    }

    const currency = String(order.charge_currency || "").toLowerCase();
    const unitAmount = Number(order.charge_amount_total);
    if (!currency || !Number.isFinite(unitAmount) || unitAmount <= 0) {
      return res.status(400).json({ error: "order_amount_invalid" });
    }

    const destinationAccount = partner?.stripe_connect_account_id ? String(partner.stripe_connect_account_id) : null;
    if (!destinationAccount) {
      return res.status(400).json({ error: "partner_connect_account_missing" });
    }

    const applicationFeeAmount = Number(order.platform_fee_amount);
    if (!Number.isFinite(applicationFeeAmount) || applicationFeeAmount < 0) {
      return res.status(400).json({ error: "order_fee_invalid" });
    }
    if (applicationFeeAmount > unitAmount) {
      return res.status(400).json({ error: "order_fee_too_high" });
    }

    const pi = await stripe.paymentIntents.create({
      amount: unitAmount,
      currency,
      application_fee_amount: applicationFeeAmount,
      transfer_data: { destination: destinationAccount },
      automatic_payment_methods: { enabled: true },
      metadata: {
        type: "market_order",
        market_order_id: orderId,
        partner_id: order.partner_id,
        customer_user_id: guard.userId,
      },
    });

    const { data: existingPay, error: readPayErr } = await supabase
      .from("partner_order_payments")
      .select("id, status")
      .eq("order_id", orderId)
      .maybeSingle();
    if (readPayErr) return res.status(500).json({ error: readPayErr.message || "Erreur lecture paiement" });
    if (existingPay?.id) {
      const { error: upErr } = await supabase
        .from("partner_order_payments")
        .update({ status: "created" })
        .eq("id", existingPay.id);
      if (upErr) return res.status(500).json({ error: upErr.message || "Erreur mise à jour paiement" });
    } else {
      const { error: insErr } = await supabase
        .from("partner_order_payments")
        .insert({ order_id: orderId, status: "created" });
      if (insErr) return res.status(500).json({ error: insErr.message || "Erreur création paiement" });
    }

    await supabase.from("partner_orders").update({ status: "pending" }).eq("id", orderId);

    const partnerAmount = Math.max(unitAmount - applicationFeeAmount, 0);
    return res.json({ clientSecret: pi.client_secret, order: { amount: unitAmount, currency, platform_fee: applicationFeeAmount, partner_amount: partnerAmount } });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Signaler une boutique (report)
app.post("/api/market/partners/:partnerId/report", bodyParser.json(), async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { partnerId } = req.params;
    const { reason, details } = req.body || {};
    const normalizedReason = typeof reason === "string" ? reason.trim() : "";
    if (!partnerId || !normalizedReason) return res.status(400).json({ error: "partnerId et reason requis" });

    const { data: existing, error: exErr } = await supabase
      .from("marketplace_shop_reports")
      .select("id, status")
      .eq("shop_id", partnerId)
      .eq("reporter_id", guard.userId)
      .eq("status", "open")
      .maybeSingle();
    if (exErr) return res.status(500).json({ error: exErr.message || "report_read_failed" });
    if (existing) return res.json({ id: existing.id, status: existing.status || "open", dedup: true });

    const payload = {
      shop_id: partnerId,
      reporter_id: guard.userId,
      reason: normalizedReason,
      details: typeof details === "string" ? details.slice(0, 2000) : null,
      status: "open",
      created_at: new Date().toISOString(),
    };

    const { data: inserted, error: insErr } = await supabase
      .from("marketplace_shop_reports")
      .insert(payload)
      .select("id")
      .maybeSingle();
    if (insErr) return res.status(500).json({ error: insErr.message || "report_insert_failed" });

    try {
      await logEvent({
        category: "marketplace",
        action: "shop.report.create",
        status: "success",
        userId: guard.userId,
        context: { partner_id: partnerId, reason: normalizedReason },
      });
    } catch {}

    try {
      const { data: admins, error } = await supabase
        .from("profiles")
        .select("id")
        .eq("is_admin", true);
      if (!error && Array.isArray(admins) && admins.length > 0) {
        const targetUserIds = admins.map((a) => a.id).filter(Boolean);
        let partnerName = null;
        try {
          const { data: p } = await supabase
            .from("partners_market")
            .select("display_name")
            .eq("id", partnerId)
            .maybeSingle();
          partnerName = p?.display_name || null;
        } catch {}
        const l2 = `Un utilisateur a signalé la boutique ${partnerName || partnerId}`;
        const l3 = normalizedReason ? normalizedReason : "";
        await sendSupabaseLightPush(req, {
          title: "Marketplace",
          message: `${l2}${l3 ? `\n${l3}` : ''}`,
          targetUserIds,
          data: { type: "market_shop_report", partnerId, partnerName: partnerName || null, reason: normalizedReason || null },
          url: "/admin/reports",
        });
      }
    } catch {}

    return res.json({ id: inserted?.id || null, status: "open" });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Liste des commandes (acheteur)
app.get("/api/market/orders", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const statusFilter = String(req.query.status || "all").trim().toLowerCase();
    const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 50, 1), 200);
    const offset = Math.max(parseInt(req.query.offset, 10) || 0, 0);

    let query = supabase
      .from("partner_orders")
      .select(
        "id, partner_id, customer_user_id, status, delivery_mode, fulfillment_status, base_currency, base_amount_total, charge_currency, charge_amount_total, created_at, updated_at, order_number"
      )
      .eq("customer_user_id", guard.userId)
      .order("created_at", { ascending: false })
      .range(offset, offset + limit - 1);

    if (statusFilter && statusFilter !== "all") {
      if (statusFilter === "pending") {
        query = query.eq("status", "pending");
      } else if (statusFilter === "paid") {
        query = query.eq("status", "paid");
      } else if (statusFilter === "canceled" || statusFilter === "cancelled") {
        query = query.in("status", ["canceled", "cancelled"]);
      } else {
        query = query.eq("status", statusFilter);
      }
    }

    const { data: orders, error: oErr } = await query;
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commandes" });

    const safeOrders = Array.isArray(orders) ? orders : [];
    const orderIds = safeOrders.map((o) => o.id).filter(Boolean);

    const partnerIds = [...new Set(safeOrders.map((o) => (o?.partner_id ? String(o.partner_id) : null)).filter(Boolean))];
    let partnerNameById = {};
    if (partnerIds.length > 0) {
      const { data: partners, error: pErr } = await supabase
        .from("partners_market")
        .select("id, display_name")
        .in("id", partnerIds);
      if (pErr) return res.status(500).json({ error: pErr.message || "Erreur lecture partenaires" });
      partnerNameById = (partners || []).reduce((acc, p) => {
        const pid = p?.id ? String(p.id) : null;
        if (pid) acc[pid] = p?.display_name || null;
        return acc;
      }, {});
    }

    let itemsByOrderId = {};
    if (orderIds.length > 0) {
      const { data: items, error: iErr } = await supabase
        .from("partner_order_items")
        .select("id, order_id, item_id, title_snapshot, unit_base_price_amount, quantity, total_base_amount")
        .in("order_id", orderIds)
        .order("created_at", { ascending: true });

      if (iErr) return res.status(500).json({ error: iErr.message || "Erreur lecture lignes commande" });

      itemsByOrderId = (items || []).reduce((acc, it) => {
        const oid = it?.order_id ? String(it.order_id) : null;
        if (!oid) return acc;
        if (!acc[oid]) acc[oid] = [];
        acc[oid].push(it);
        return acc;
      }, {});
    }

    const enriched = safeOrders.map((o) => {
      const oid = o?.id ? String(o.id) : null;
      const s = String(o?.status || '').toLowerCase();
      const normalizedFulfillment = (s === 'cancelled' || s === 'canceled') ? 'completed' : (o?.fulfillment_status || null);
      return {
        ...o,
        fulfillment_status: normalizedFulfillment,
        items: oid ? itemsByOrderId[oid] || [] : [],
        partner_display_name: o?.partner_id ? (partnerNameById[String(o.partner_id)] || null) : null,
      };
    });

    return res.json({ orders: enriched, limit, offset });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Détail commande (buyer/seller)
app.get("/api/market/orders/:orderId", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { orderId } = req.params;
    if (!orderId) return res.status(400).json({ error: "orderId requis" });

    const { data: order, error: oErr } = await supabase
      .from("partner_orders")
      .select(
        "id, partner_id, customer_user_id, status, delivery_mode, customer_note, fulfillment_status, fulfillment_updated_at, buyer_received_at, payout_release_at, base_currency, base_amount_total, charge_currency, charge_amount_total, platform_fee_amount, partner_amount, created_at, updated_at, order_number, customer_first_name, customer_last_name, customer_phone, customer_address_line1, customer_address_line2, customer_address_postal_code, customer_address_city, customer_address_country, customer_country_code, tracking_url, carrier_name, shipped_at, tracking_added_at"
      )
      .eq("id", orderId)
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commande" });
    if (!order) return res.status(404).json({ error: "order_not_found" });

    const { data: partner, error: pErr } = await supabase
      .from("partners_market")
      .select("id, owner_user_id, display_name")
      .eq("id", order.partner_id)
      .maybeSingle();
    if (pErr) return res.status(500).json({ error: pErr.message || "Erreur lecture partenaire" });

    const sellerId = partner?.owner_user_id ? String(partner.owner_user_id) : null;
    const isBuyer = String(order.customer_user_id) === String(guard.userId);
    const isSeller = sellerId && sellerId === String(guard.userId);
    if (!(isBuyer || isSeller)) return res.status(403).json({ error: "forbidden" });

    let customerAlias = null;
    if (order?.customer_user_id) {
      const uid = String(order.customer_user_id);
      customerAlias = `#${uid.slice(0, 6)}`;
    }

    const { data: items, error: iErr } = await supabase
      .from("partner_order_items")
      .select("id, order_id, item_id, title_snapshot, unit_base_price_amount, quantity, total_base_amount")
      .eq("order_id", orderId)
      .order("created_at", { ascending: true });
    if (iErr) return res.status(500).json({ error: iErr.message || "Erreur lecture lignes commande" });

    const { data: conv } = await supabase
      .from("marketplace_order_conversations")
      .select("id")
      .eq("order_id", orderId)
      .maybeSingle();

    const ordStatus = String(order?.status || '').toLowerCase();
    const normalizedFulfillment = (ordStatus === 'cancelled' || ordStatus === 'canceled') ? 'completed' : (order?.fulfillment_status || null);

    // Email client depuis profiles (exposé tant que la commande n'est pas anonymisée côté vendeur)
    let customerEmail = null;
    try {
      if (order?.customer_user_id) {
        const { data: prof } = await supabase
          .from("profiles")
          .select("email")
          .eq("id", order.customer_user_id)
          .maybeSingle();
        customerEmail = prof?.email || null;
      }
    } catch {}

    let orderOut = { ...order, fulfillment_status: normalizedFulfillment, partner_display_name: partner?.display_name || null, customer_alias: customerAlias, customer_email: customerEmail };
    if (isSeller && String(normalizedFulfillment || '').toLowerCase() === 'completed') {
      orderOut = {
        ...orderOut,
        customer_first_name: null,
        customer_last_name: null,
        customer_phone: null,
        customer_address_line1: null,
        customer_address_line2: null,
        customer_address_postal_code: null,
        customer_address_city: null,
        customer_address_country: null,
        customer_email: null,
      };
    }

    return res.json({
      order: orderOut,
      items: Array.isArray(items) ? items : [],
      conversationId: conv?.id || null,
      role: isBuyer ? "buyer" : "seller",
    });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Enregistrer les informations de livraison (acheteur) avant paiement
app.post("/api/market/orders/:orderId/shipping-info", bodyParser.json(), async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { orderId } = req.params;
    if (!orderId) return res.status(400).json({ error: "orderId_requis" });

    const {
      first_name,
      last_name,
      email,
      phone,
      address_line1,
      address_line2,
      postal_code,
      city,
      country,
    } = req.body || {};

    const { data: order, error: oErr } = await supabase
      .from("partner_orders")
      .select("id, customer_user_id, status, delivery_mode")
      .eq("id", orderId)
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commande" });
    if (!order) return res.status(404).json({ error: "order_not_found" });
    if (String(order.customer_user_id) !== String(guard.userId)) return res.status(403).json({ error: "forbidden" });

    const ordStatus = String(order.status || '').toLowerCase();
    if (ordStatus !== 'pending') return res.status(400).json({ error: "order_not_pending" });

    const isPickup = String(order.delivery_mode || '').toLowerCase() === 'pickup';
    if (!isPickup) {
      const required = [first_name, last_name, phone, address_line1, postal_code, city, country];
      if (required.some((v) => !String(v || '').trim())) {
        return res.status(400).json({ error: "missing_fields" });
      }
    }

    const nowIso = new Date().toISOString();
    const updatePayload = {
      customer_first_name: (first_name || null) ? String(first_name).trim() : null,
      customer_last_name: (last_name || null) ? String(last_name).trim() : null,
      customer_phone: (phone || null) ? String(phone).trim() : null,
      customer_address_line1: (address_line1 || null) ? String(address_line1).trim() : null,
      customer_address_line2: (address_line2 || null) ? String(address_line2).trim() : null,
      customer_address_postal_code: (postal_code || null) ? String(postal_code).trim() : null,
      customer_address_city: (city || null) ? String(city).trim() : null,
      customer_address_country: (country || null) ? String(country).trim() : null,
      customer_country_code: (country || null) ? String(country).trim() : null,
      updated_at: nowIso,
    };

    const { error: upErr } = await supabase
      .from("partner_orders")
      .update(updatePayload)
      .eq("id", orderId);
    if (upErr) return res.status(500).json({ error: upErr.message || "Erreur mise à jour" });

    // Email non persisté côté commande; affichage via profiles
    return res.json({ ok: true });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Mettre à jour le statut d'exécution (vendeur)
app.patch("/api/market/orders/:orderId/fulfillment", bodyParser.json(), async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { orderId } = req.params;
    const { status, tracking_url, carrier_name } = req.body || {};
    if (!orderId) return res.status(400).json({ error: "orderId requis" });

    const next = String(status || "").toLowerCase();
    const allowed = ["preparing", "shipping", "delivered"];
    if (!allowed.includes(next)) return res.status(400).json({ error: "invalid_status" });

    const { data: order, error: oErr } = await supabase
      .from("partner_orders")
      .select("id, partner_id, customer_user_id, fulfillment_status, fulfillment_updated_at, tracking_url, carrier_name, shipped_at, tracking_added_at")
      .eq("id", orderId)
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commande" });
    if (!order) return res.status(404).json({ error: "order_not_found" });

    const { data: partner, error: pErr } = await supabase
      .from("partners_market")
      .select("id, owner_user_id, has_accepted_marketplace_terms_vendors, marketplace_terms_vendors_version")
      .eq("id", order.partner_id)
      .maybeSingle();
    if (pErr) return res.status(500).json({ error: pErr.message || "Erreur lecture partenaire" });
    if (!partner || String(partner.owner_user_id) !== String(guard.userId)) return res.status(403).json({ error: "forbidden" });
    if (!isVendorTermsCompliant(partner)) return res.status(403).json({ error: "vendor_terms_required" });

    const nowIso = new Date().toISOString();
    const updatePayload = { fulfillment_status: next, fulfillment_updated_at: nowIso, updated_at: nowIso };
    if (next === "shipping") {
      const track = typeof tracking_url === "string" ? tracking_url.trim() : "";
      if (!track) return res.status(400).json({ error: "tracking_url_required" });
      const carrier = typeof carrier_name === "string" ? carrier_name.trim() : null;
      updatePayload.tracking_url = track;
      updatePayload.carrier_name = carrier || null;
      updatePayload.shipped_at = nowIso;
      updatePayload.tracking_added_at = nowIso;
    }
    if (next === "delivered") {
      updatePayload.payout_release_at = new Date(Date.now() + 14 * 24 * 60 * 60 * 1000).toISOString();
    }
    const { data: upd, error: uErr } = await supabase
      .from("partner_orders")
      .update(updatePayload)
      .eq("id", orderId)
      .select("id, fulfillment_status, fulfillment_updated_at, tracking_url, carrier_name, shipped_at, tracking_added_at")
      .maybeSingle();
    if (uErr) return res.status(500).json({ error: uErr.message || "Erreur mise à jour" });

    if (next === "shipping") {
      try {
        let convId = null;
        const { data: conv } = await supabase
          .from("marketplace_order_conversations")
          .select("id")
          .eq("order_id", orderId)
          .maybeSingle();
        if (conv?.id) {
          convId = conv.id;
        } else {
          const { data: insertedConv } = await supabase
            .from("marketplace_order_conversations")
            .insert({
              order_id: orderId,
              buyer_id: order.customer_user_id,
              seller_id: partner.owner_user_id,
              created_at: nowIso,
            })
            .select("id")
            .maybeSingle();
          convId = insertedConv?.id || null;
        }
        if (convId) {
          await supabase
            .from("marketplace_order_messages")
            .insert({
              conversation_id: convId,
              author_id: guard.userId,
              body: "Lien de suivi ajouté",
              created_at: nowIso,
            });
        }
      } catch {}
    }

    try {
      const code = await getOrderDisplayCode(orderId);
      const map = {
        preparing: `Commande n°${code} est en cours de préparation`,
        shipping: `Commande n°${code} a été envoyée`,
        delivered: `Commande n°${code} a été marquée comme livrée`,
      };
      const l2 = map[next] || `Commande n°${code}`;
      await sendSupabaseLightPush(req, {
        title: "Marketplace",
        message: `${l2}\nCliquez pour voir le statut de la commande.`,
        targetUserIds: [String(order.customer_user_id)],
        data: { type: "market_order_fulfillment_update", orderId, orderNumber: code, status: next },
        url: `/market/orders/${orderId}`,
      });
    } catch {}

    return res.json({ order: upd });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Confirmation de réception (acheteur)
app.post("/api/market/orders/:orderId/confirm-received", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { orderId } = req.params;
    if (!orderId) return res.status(400).json({ error: "orderId requis" });

    const { data: order, error: oErr } = await supabase
      .from("partner_orders")
      .select("id, partner_id, customer_user_id, fulfillment_status, buyer_received_at")
      .eq("id", orderId)
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commande" });
    if (!order) return res.status(404).json({ error: "order_not_found" });
    if (String(order.customer_user_id) !== String(guard.userId)) return res.status(403).json({ error: "forbidden" });

    const f = String(order.fulfillment_status || "").toLowerCase();
    if (f !== "delivered") return res.status(400).json({ error: "not_delivered" });

    const now = new Date().toISOString();
    const { error: uErr } = await supabase
      .from("partner_orders")
      .update({ buyer_received_at: order.buyer_received_at || now, fulfillment_status: "completed", fulfillment_updated_at: now, updated_at: now })
      .eq("id", orderId);
    if (uErr) return res.status(500).json({ error: uErr.message || "Erreur mise à jour" });

    let sellerId = null;
    try {
      const { data: partner } = await supabase
        .from("partners_market")
        .select("id, owner_user_id")
        .eq("id", order.partner_id)
        .maybeSingle();
      sellerId = partner?.owner_user_id ? String(partner.owner_user_id) : null;
    } catch {}

    if (sellerId) {
      try {
        const code = await getOrderDisplayCode(orderId);
        await sendSupabaseLightPush(req, {
          title: "Marketplace",
          message: `Commande n°${code} réceptionnée\nL’acheteur a confirmé la réception de la commande.`,
          targetUserIds: [sellerId],
          data: { type: "market_order_received_confirmed", orderId, orderNumber: code },
          url: `/market/orders/${orderId}`,
        });
      } catch {}
    }

    return res.json({ ok: true });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Messages d'une commande
app.get("/api/market/orders/:orderId/messages", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { orderId } = req.params;
    if (!orderId) return res.status(400).json({ error: "orderId requis" });

    const { data: order, error: oErr } = await supabase
      .from("partner_orders")
      .select("id, partner_id, customer_user_id, status, fulfillment_status")
      .eq("id", orderId)
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commande" });
    if (!order) return res.status(404).json({ error: "order_not_found" });

    const { data: partner, error: pErr } = await supabase
      .from("partners_market")
      .select("id, owner_user_id")
      .eq("id", order.partner_id)
      .maybeSingle();
    if (pErr) return res.status(500).json({ error: pErr.message || "Erreur lecture partenaire" });

    const sellerId = partner?.owner_user_id ? String(partner.owner_user_id) : null;
    const isBuyer = String(order.customer_user_id) === String(guard.userId);
    const isSeller = sellerId && sellerId === String(guard.userId);
    if (!(isBuyer || isSeller)) return res.status(403).json({ error: "forbidden" });

    const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 100, 1), 500);
    const offset = Math.max(parseInt(req.query.offset, 10) || 0, 0);

    const { data: conv } = await supabase
      .from("marketplace_order_conversations")
      .select("id")
      .eq("order_id", orderId)
      .maybeSingle();

    if (!conv?.id) {
      return res.json({ messages: [], limit, offset, conversationId: null });
    }

    const { data: messages, error: mErr } = await supabase
      .from("marketplace_order_messages")
      .select("id, conversation_id, sender_id:author_id, content:body, created_at")
      .eq("conversation_id", conv.id)
      .order("created_at", { ascending: true })
      .range(offset, offset + limit - 1);
    if (mErr) return res.status(500).json({ error: mErr.message || "Erreur lecture messages" });

    return res.json({ messages: Array.isArray(messages) ? messages : [], limit, offset, conversationId: conv.id });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.post("/api/market/orders/:orderId/messages", bodyParser.json(), async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { orderId } = req.params;
    const { content } = req.body || {};
    if (!orderId) return res.status(400).json({ error: "orderId requis" });
    const text = typeof content === "string" ? content.trim() : "";
    if (!text) return res.status(400).json({ error: "content requis" });

    const { data: order, error: oErr } = await supabase
      .from("partner_orders")
      .select("id, partner_id, customer_user_id, status, fulfillment_status")
      .eq("id", orderId)
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commande" });
    if (!order) return res.status(404).json({ error: "order_not_found" });

    const statusNorm = String(order.status || "").toLowerCase();
    if (["pending", "canceled", "cancelled"].includes(statusNorm)) {
      return res.status(400).json({ error: "order_not_paid" });
    }
    const fNorm = String(order.fulfillment_status || '').toLowerCase();
    if (fNorm === 'completed') {
      return res.status(403).json({ error: "chat_locked" });
    }

    const { data: partner, error: pErr } = await supabase
      .from("partners_market")
      .select("id, owner_user_id")
      .eq("id", order.partner_id)
      .maybeSingle();
    if (pErr) return res.status(500).json({ error: pErr.message || "Erreur lecture partenaire" });

    const sellerId = partner?.owner_user_id ? String(partner.owner_user_id) : null;
    const isBuyer = String(order.customer_user_id) === String(guard.userId);
    const isSeller = sellerId && sellerId === String(guard.userId);
    if (!(isBuyer || isSeller)) return res.status(403).json({ error: "forbidden" });

    let convId = null;
    try {
      const { data: conv } = await supabase
        .from("marketplace_order_conversations")
        .select("id")
        .eq("order_id", orderId)
        .maybeSingle();
      if (conv?.id) {
        convId = conv.id;
      } else {
        const { data: inserted, error: insConvErr } = await supabase
          .from("marketplace_order_conversations")
          .insert({
            order_id: orderId,
            buyer_id: order.customer_user_id,
            seller_id: partner.owner_user_id,
            created_at: new Date().toISOString(),
          })
          .select("id")
          .maybeSingle();
        if (insConvErr) return res.status(500).json({ error: insConvErr.message || "Erreur création conversation" });
        convId = inserted?.id || null;
      }
    } catch (e) {
      return res.status(500).json({ error: e?.message || "Erreur conversation" });
    }

    if (!convId) return res.status(500).json({ error: "conversation_unavailable" });

    const safeText = text.slice(0, 2000);
    const { data: msg, error: mErr } = await supabase
      .from("marketplace_order_messages")
      .insert({
        conversation_id: convId,
        author_id: guard.userId,
        body: safeText,
        created_at: new Date().toISOString(),
      })
      .select("id")
      .maybeSingle();
    if (mErr) return res.status(500).json({ error: mErr.message || "Erreur envoi message" });

    try {
      const recipientId = isBuyer ? sellerId : String(order.customer_user_id);
      if (recipientId) {
        const code = await getOrderDisplayCode(orderId);
        const actor = isBuyer ? "L’acheteur" : "Le vendeur";
        const prev = marketMessagePreview(safeText);
        await sendSupabaseLightPush(req, {
          title: "Marketplace",
          message: `Commande n°${code}\n${actor} : ${prev}`,
          targetUserIds: [recipientId],
          data: { type: "market_order_message", orderId, orderNumber: code, role: isBuyer ? 'buyer' : 'seller', preview: prev },
          url: `/market/orders/${orderId}`,
        });
      }
    } catch {}

    return res.json({ success: true, messageId: msg?.id || null, conversationId: convId });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Avis acheteur (créer)
app.post("/api/market/orders/:orderId/rating", bodyParser.json(), async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { orderId } = req.params;
    const { rating, comment } = req.body || {};
    if (!orderId) return res.status(400).json({ error: "orderId requis" });

    const { data: order, error: oErr } = await supabase
      .from("partner_orders")
      .select("id, partner_id, customer_user_id, status, fulfillment_status")
      .eq("id", orderId)
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commande" });
    if (!order) return res.status(404).json({ error: "order_not_found" });
    if (String(order.customer_user_id) !== String(guard.userId)) return res.status(403).json({ error: "forbidden" });

    const s = String(order.status || '').toLowerCase();
    const f = String(order.fulfillment_status || '').toLowerCase();
    if (!(s === 'paid' && f === 'completed')) return res.status(400).json({ error: "order_not_completed" });

    const r = Math.max(Math.min(parseInt(rating, 10) || 0, 5), 0);
    if (!(r >= 1 && r <= 5)) return res.status(400).json({ error: "rating_invalid" });
    const text = typeof comment === 'string' ? comment.slice(0, 1000).trim() : null;

    const { data: existing } = await supabase
      .from("marketplace_partner_ratings")
      .select("id")
      .eq("order_id", orderId)
      .maybeSingle();
    if (existing?.id) return res.status(409).json({ error: "rating_exists" });

    const payload = {
      order_id: orderId,
      partner_id: order.partner_id,
      buyer_id: guard.userId,
      rating: r,
      comment: text,
    };
    const { data: ins, error: insErr } = await supabase
      .from("marketplace_partner_ratings")
      .insert(payload)
      .select("id")
      .maybeSingle();
    if (insErr) return res.status(500).json({ error: insErr.message || "Erreur enregistrement avis" });

    try {
      const { data: partner } = await supabase
        .from("partners_market")
        .select("id, owner_user_id")
        .eq("id", order.partner_id)
        .maybeSingle();
      const sellerId = partner?.owner_user_id ? String(partner.owner_user_id) : null;
      if (sellerId) {
        const code = await getOrderDisplayCode(orderId);
        await sendSupabaseLightPush(req, {
          title: "Marketplace",
          message: `Commande n°${code}\nL’acheteur a laissé un avis. Cliquez pour le voir.`,
          targetUserIds: [sellerId],
          data: { type: "market_partner_new_rating", orderId, orderNumber: code },
          url: `/market/orders/${orderId}`,
        });
      }
    } catch {}

    return res.json({ success: true, ratingId: ins?.id || null });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Lire l'avis (buyer/seller)
app.get("/api/market/orders/:orderId/rating", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { orderId } = req.params;
    if (!orderId) return res.status(400).json({ error: "orderId requis" });

    const { data: order, error: oErr } = await supabase
      .from("partner_orders")
      .select("id, partner_id, customer_user_id")
      .eq("id", orderId)
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commande" });
    if (!order) return res.status(404).json({ error: "order_not_found" });

    let sellerId = null;
    try {
      const { data: partner } = await supabase
        .from("partners_market")
        .select("id, owner_user_id")
        .eq("id", order.partner_id)
        .maybeSingle();
      sellerId = partner?.owner_user_id ? String(partner.owner_user_id) : null;
    } catch {}

    const isBuyer = String(order.customer_user_id) === String(guard.userId);
    const isSeller = sellerId && String(sellerId) === String(guard.userId);
    if (!(isBuyer || isSeller)) return res.status(403).json({ error: "forbidden" });

    const { data: row, error: rErr } = await supabase
      .from("marketplace_partner_ratings")
      .select("id, rating, comment, created_at, is_edited")
      .eq("order_id", orderId)
      .maybeSingle();
    if (rErr) return res.status(500).json({ error: rErr.message || "Erreur lecture avis" });

    return res.json({ rating: row || null });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Seller: liste/résumé avis
app.get("/api/market/partners/:partnerId/ratings", async (req, res) => {
  try {
    const { partnerId } = req.params;
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });

    const auth = await requirePartnerOwner({ req, partnerId });
    if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

    const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 50, 1), 200);
    const offset = Math.max(parseInt(req.query.offset, 10) || 0, 0);

    const { data: rows, error: rErr } = await supabase
      .from("marketplace_partner_ratings")
      .select("id, order_id, buyer_id, rating, comment, created_at")
      .eq("partner_id", partnerId)
      .order("created_at", { ascending: false })
      .range(offset, offset + limit - 1);
    if (rErr) return res.status(500).json({ error: rErr.message || "Erreur lecture avis" });

    const orderIds = (rows || []).map((x) => x.order_id).filter(Boolean);
    let ordersById = {};
    if (orderIds.length > 0) {
      const { data: ords, error: oErr } = await supabase
        .from("partner_orders")
        .select("id, order_number, created_at")
        .in("id", orderIds);
      if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commandes" });
      ordersById = (ords || []).reduce((acc, o) => { acc[String(o.id)] = o; return acc; }, {});
    }

    const list = (rows || []).map((x) => {
      const o = ordersById[String(x.order_id)] || {};
      const buyerAlias = x?.buyer_id ? `#${String(x.buyer_id).slice(0, 6)}` : null;
      return { id: x.id, order_id: x.order_id, order_number: o?.order_number || null, buyer_alias: buyerAlias, rating: x.rating, comment: x.comment, created_at: x.created_at };
    });

    return res.json({ ratings: list, limit, offset });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.get("/api/market/partners/:partnerId/ratings/summary", async (req, res) => {
  try {
    const { partnerId } = req.params;
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });

    const auth = await requirePartnerOwner({ req, partnerId });
    if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

    let avg = null;
    let count = 0;
    try {
      const { data: row } = await supabase
        .from("marketplace_partner_ratings_summary")
        .select("partner_id, ratings_count, avg_rating")
        .eq("partner_id", partnerId)
        .maybeSingle();
      if (row) {
        count = Number(row.ratings_count || 0);
        avg = row.avg_rating != null ? Number(row.avg_rating) : null;
      }
    } catch {}

    if (avg == null) {
      const { data: rows } = await supabase
        .from("marketplace_partner_ratings")
        .select("rating")
        .eq("partner_id", partnerId);
      const arr = Array.isArray(rows) ? rows.map((x) => Number(x.rating) || 0).filter((n) => n > 0) : [];
      count = arr.length;
      avg = count > 0 ? arr.reduce((a, b) => a + b, 0) / count : null;
    }

    const avgRounded = avg != null ? Math.round(avg * 10) / 10 : null;
    return res.json({ avg: avgRounded, count });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Public: résumé / liste avis
app.get("/api/market/public/partners/:partnerId/ratings/summary", async (req, res) => {
  try {
    const { partnerId } = req.params;
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });

    let avg = null;
    let count = 0;
    try {
      const { data: row } = await supabase
        .from("marketplace_partner_ratings_summary")
        .select("partner_id, ratings_count, avg_rating")
        .eq("partner_id", partnerId)
        .maybeSingle();
      if (row) {
        count = Number(row.ratings_count || 0);
        avg = row.avg_rating != null ? Number(row.avg_rating) : null;
      }
    } catch {}

    if (avg == null) {
      const { data: rows } = await supabase
        .from("marketplace_partner_ratings")
        .select("rating")
        .eq("partner_id", partnerId);
      const arr = Array.isArray(rows) ? rows.map((x) => Number(x.rating) || 0).filter((n) => n > 0) : [];
      count = arr.length;
      avg = count > 0 ? arr.reduce((a, b) => a + b, 0) / count : null;
    }

    const avgRounded = avg != null ? Math.round(avg * 10) / 10 : null;
    return res.json({ avg: avgRounded, count });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.get("/api/market/public/partners/:partnerId/ratings", async (req, res) => {
  try {
    const { partnerId } = req.params;
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });

    const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 50, 1), 200);
    const offset = Math.max(parseInt(req.query.offset, 10) || 0, 0);

    const { data: rows, error: rErr } = await supabase
      .from("marketplace_partner_ratings")
      .select("id, buyer_id, rating, comment, created_at")
      .eq("partner_id", partnerId)
      .order("created_at", { ascending: false })
      .range(offset, offset + limit - 1);
    if (rErr) return res.status(500).json({ error: rErr.message || "Erreur lecture avis" });

    const list = (rows || []).map((x) => ({ id: x.id, buyer_alias: x?.buyer_id ? `#${String(x.buyer_id).slice(0, 6)}` : null, rating: x.rating, comment: x.comment, created_at: x.created_at }));
    return res.json({ ratings: list, limit, offset });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Options de livraison (GET/PUT)
app.get("/api/market/partners/:partnerId/shipping-options", async (req, res) => {
  try {
    const { partnerId } = req.params;
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });

    const { data: shop, error: pErr } = await supabase
      .from("partners_market")
      .select("id, base_currency")
      .eq("id", partnerId)
      .maybeSingle();
    if (pErr) return res.status(500).json({ error: pErr.message || "Erreur lecture partenaire" });

    const { data: rows, error } = await supabase
      .from("shipping_options")
      .select("id, shop_id, shipping_type, label, price_cents, is_active, created_at, updated_at")
      .eq("shop_id", partnerId);
    if (error) return res.status(500).json({ error: error.message || "Erreur lecture options livraison" });

    const allowed = ["pickup", "standard", "express", "international"];
    const defaults = {
      pickup: { shipping_type: "pickup", label: "Retrait sur place", price_cents: 0, is_active: true },
      standard: { shipping_type: "standard", label: "Livraison standard", price_cents: 0, is_active: false },
      express: { shipping_type: "express", label: "Livraison express", price_cents: 0, is_active: false },
      international: { shipping_type: "international", label: "Livraison internationale", price_cents: 0, is_active: false },
    };

    const byType = new Map();
    (Array.isArray(rows) ? rows : []).forEach((r) => {
      const t = String(r?.shipping_type || "").toLowerCase();
      if (allowed.includes(t)) byType.set(t, r);
    });

    const options = allowed.map((t) => {
      const r = byType.get(t);
      if (r) return r;
      const d = defaults[t];
      return { id: null, shop_id: partnerId, ...d };
    });

    return res.json({ options, base_currency: shop?.base_currency || null });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.put("/api/market/partners/:partnerId/shipping-options", bodyParser.json(), async (req, res) => {
  try {
    const { partnerId } = req.params;
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });

    const auth = await requirePartnerOwner({ req, partnerId });
    if (!auth.ok) return res.status(auth.status).json({ error: auth.error });

    const allowed = new Set(["pickup", "standard", "express", "international"]);
    const payload = Array.isArray(req.body) ? req.body : Array.isArray(req.body?.options) ? req.body.options : [];
    if (!Array.isArray(payload)) return res.status(400).json({ error: "payload_invalid" });

    const normalized = payload
      .map((x) => ({
        shipping_type: String(x?.shipping_type || "").toLowerCase().trim(),
        label: typeof x?.label === "string" ? x.label.trim().slice(0, 120) : null,
        price_cents: Math.max(parseInt(x?.price_cents, 10) || 0, 0),
        is_active: x?.is_active === true,
      }))
      .filter((x) => allowed.has(x.shipping_type));

    const { data: existing, error: exErr } = await supabase
      .from("shipping_options")
      .select("id, shipping_type")
      .eq("shop_id", partnerId);
    if (exErr) return res.status(500).json({ error: exErr.message || "Erreur lecture options" });
    const idByType = new Map((existing || []).map((r) => [String(r.shipping_type).toLowerCase(), r.id]));

    const toInsert = [];
    const toUpdate = [];
    normalized.forEach((n) => {
      const id = idByType.get(n.shipping_type) || null;
      const row = {
        shop_id: partnerId,
        shipping_type: n.shipping_type,
        label: n.label || (n.shipping_type === "pickup" ? "Retrait sur place" : n.shipping_type === "standard" ? "Livraison standard" : n.shipping_type === "express" ? "Livraison express" : "Livraison internationale"),
        price_cents: n.price_cents,
        is_active: n.is_active === true,
        updated_at: new Date().toISOString(),
      };
      if (id) {
        toUpdate.push({ id, ...row });
      } else {
        toInsert.push({ ...row, created_at: new Date().toISOString() });
      }
    });

    if (toInsert.length > 0) {
      const { error: insErr } = await supabase.from("shipping_options").insert(toInsert);
      if (insErr) return res.status(500).json({ error: insErr.message || "Erreur création options" });
    }
    if (toUpdate.length > 0) {
      for (const row of toUpdate) {
        const { id, ...rest } = row;
        const { error: upErr } = await supabase.from("shipping_options").update(rest).eq("id", id);
        if (upErr) return res.status(500).json({ error: upErr.message || "Erreur mise à jour option" });
      }
    }

    const { data: finalRows, error: finErr } = await supabase
      .from("shipping_options")
      .select("id, shop_id, shipping_type, label, price_cents, is_active, created_at, updated_at")
      .eq("shop_id", partnerId);
    if (finErr) return res.status(500).json({ error: finErr.message || "Erreur lecture options" });

    return res.json({ options: finalRows });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Sync paiement après Checkout (retour Stripe)
app.post("/api/market/orders/sync-payment", bodyParser.json(), async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { sessionId } = req.body || {};
    const sid = sessionId ? String(sessionId).trim() : "";
    if (!sid) return res.status(400).json({ error: "sessionId requis" });

    const { data: paymentRow, error: payErr } = await supabase
      .from("partner_order_payments")
      .select("order_id, stripe_checkout_session_id, status")
      .eq("stripe_checkout_session_id", sid)
      .maybeSingle();
    if (payErr) return res.status(500).json({ error: payErr.message || "Erreur lecture payment" });
    if (!paymentRow?.order_id) return res.status(404).json({ error: "payment_not_found" });

    const orderId = String(paymentRow.order_id);

    const { data: order, error: oErr } = await supabase
      .from("partner_orders")
      .select("id, customer_user_id, status")
      .eq("id", orderId)
      .maybeSingle();
    if (oErr) return res.status(500).json({ error: oErr.message || "Erreur lecture commande" });
    if (!order) return res.status(404).json({ error: "order_not_found" });
    if (order.customer_user_id !== guard.userId) return res.status(403).json({ error: "forbidden" });

    const session = await stripe.checkout.sessions.retrieve(sid, { expand: ["payment_intent"] });

    const marketOrderId = session?.metadata?.market_order_id ? String(session.metadata.market_order_id) : null;
    if (marketOrderId && marketOrderId !== orderId) {
      return res.status(400).json({ error: "order_session_mismatch" });
    }

    const paymentStatus = String(session?.payment_status || "").toLowerCase();
    const isPaid = paymentStatus === "paid";
    if (!isPaid) {
      return res.status(400).json({ error: "payment_not_paid", payment_status: session?.payment_status || null });
    }

    const cfArray = Array.isArray(session?.custom_fields) ? session.custom_fields : [];
    const cfMap = Object.fromEntries(
      cfArray.filter((f) => f && f.key).map((f) => [String(f.key), (f.text && f.text.value) ? String(f.text.value) : null])
    );
    const fullName = session?.shipping_details?.name || session?.customer_details?.name || "";
    const parts = String(fullName).trim().split(/\s+/);
    const fallbackFirst = parts.length > 1 ? parts.slice(0, -1).join(" ") : parts[0] || null;
    const fallbackLast = parts.length > 1 ? parts[parts.length - 1] : null;
    const firstName = (cfMap.first_name || null) || fallbackFirst;
    const lastName = (cfMap.last_name || null) || fallbackLast;
    const address = session?.shipping_details?.address || session?.customer_details?.address || null;
    const phone = session?.customer_details?.phone || session?.shipping_details?.phone || null;

    await supabase
      .from("partner_orders")
      .update({
        customer_first_name: firstName || null,
        customer_last_name: lastName || null,
        customer_phone: phone || null,
        customer_address_line1: address?.line1 || null,
        customer_address_line2: address?.line2 || null,
        customer_address_postal_code: address?.postal_code || null,
        customer_address_city: address?.city || null,
        customer_address_country: address?.country || null,
        customer_country_code: address?.country || null,
        updated_at: new Date().toISOString(),
      })
      .eq("id", orderId);

    const paymentIntentId = session?.payment_intent
      ? (typeof session.payment_intent === "string" ? session.payment_intent : session.payment_intent?.id || null)
      : null;

    await supabase
      .from("partner_orders")
      .update({ status: "paid", fulfillment_status: "sent_to_seller", fulfillment_updated_at: new Date().toISOString(), updated_at: new Date().toISOString() })
      .eq("id", orderId);

    await supabase
      .from("partner_order_payments")
      .update({
        status: "succeeded",
        stripe_payment_intent_id: paymentIntentId,
        updated_at: new Date().toISOString(),
      })
      .eq("stripe_checkout_session_id", sid);

    try {
      const { data: conv } = await supabase
        .from("marketplace_order_conversations")
        .select("id")
        .eq("order_id", orderId)
        .maybeSingle();
      if (!conv) {
        const { data: ordRow } = await supabase
          .from("partner_orders")
          .select("id, partner_id, customer_user_id")
          .eq("id", orderId)
          .maybeSingle();
        const { data: partnerRow } = await supabase
          .from("partners_market")
          .select("id, owner_user_id")
          .eq("id", ordRow?.partner_id)
          .maybeSingle();
        if (ordRow?.id && partnerRow?.owner_user_id) {
          await supabase.from("marketplace_order_conversations").insert({
            order_id: orderId,
            buyer_id: ordRow.customer_user_id,
            seller_id: partnerRow.owner_user_id,
            created_at: new Date().toISOString(),
          });
        }
      }
    } catch {}

    await logEvent({
      category: "marketplace",
      action: "checkout.sync",
      status: "success",
      userId: guard.userId,
      context: {
        market_order_id: orderId,
        stripe_checkout_session_id: sid,
        stripe_payment_intent_id: paymentIntentId,
      },
    });

    return res.json({
      ok: true,
      orderId,
      payment_status: session?.payment_status || null,
      stripe_payment_intent_id: paymentIntentId,
    });
  } catch (e) {
    await logEvent({
      category: "marketplace",
      action: "checkout.sync",
      status: "error",
      context: { error: e?.message || String(e) },
    });
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

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
// 👥 Contacts - Sync Supabase profiles -> Brevo List
//   - Route sécurisée par header x-sync-secret
//   - Upsert sur email (pas de doublons)
// ============================================================

const brevoSyncSecret = process.env.BREVO_SYNC_SECRET;
const BREVO_LIST_ONEKAMER_BASE = 2;

function isValidEmail(email) {
  const e = String(email || "").trim();
  if (!e) return false;
  if (!e.includes("@")) return false;
  if (e.length > 254) return false;
  return true;
}

async function brevoUpsertContact({ email, username, listId }) {
  if (!brevoApiKey) throw new Error("BREVO_API_KEY manquant");

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 20000);

  try {
    const payload = {
      email,
      updateEnabled: true,
      listIds: [listId],
      attributes: {},
    };

    const uname = String(username || "").trim();
    if (uname) payload.attributes.USERNAME = uname;

    const response = await fetch("https://api.brevo.com/v3/contacts", {
      method: "POST",
      headers: {
        "api-key": brevoApiKey,
        "Content-Type": "application/json",
        accept: "application/json",
      },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });

    if (!response.ok) {
      const errorText = await response.text();
      const err = new Error(`Brevo contacts API error ${response.status}: ${errorText}`);
      err.status = response.status;
      err.brevoBody = errorText;
      throw err;
    }
  } finally {
    clearTimeout(timeout);
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function brevoUpsertContactWithRetry({ email, username, listId }) {
  const maxAttempts = 5;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await brevoUpsertContact({ email, username, listId });
      return;
    } catch (e) {
      const status = Number(e?.status || 0);
      const shouldRetry = status === 429 || (status >= 500 && status <= 599);
      if (!shouldRetry || attempt === maxAttempts) throw e;

      const base = 1000;
      const maxDelay = 15000;
      const delay = Math.min(maxDelay, base * Math.pow(2, attempt - 1));
      const jitter = Math.floor(Math.random() * 250);
      await sleep(delay + jitter);
    }
  }
}

function maskEmail(email) {
  const e = String(email || "").trim();
  const at = e.indexOf("@");
  if (at <= 0) return "***";
  const local = e.slice(0, at);
  const domain = e.slice(at + 1);
  const head = local.slice(0, 2);
  return `${head}${local.length > 2 ? "***" : "***"}@${domain}`;
}

async function mapLimit(items, limit, fn) {
  const results = { ok: 0, fail: 0, failedSamples: [] };
  let index = 0;

  async function worker() {
    while (index < items.length) {
      const current = items[index++];
      try {
        await fn(current);
        results.ok += 1;
      } catch (e) {
        results.fail += 1;

        if (results.failedSamples.length < 20) {
          results.failedSamples.push({
            email: maskEmail(current?.email),
            error: String(e?.message || e || "unknown_error"),
          });
        }

        if (results.failedSamples.length <= 3) {
          console.warn(
            "⚠️ Brevo sync contact failed:",
            maskEmail(current?.email),
            String(e?.message || e || "unknown_error")
          );
        }
      }
    }
  }

  const workers = Array.from({ length: Math.max(1, Math.min(limit, items.length)) }, () => worker());
  await Promise.all(workers);
  return results;
}

app.post("/api/brevo/sync-contacts", async (req, res) => {
  try {
    const got = String(req.headers["x-sync-secret"] || "");
    if (!brevoSyncSecret || !got || got !== brevoSyncSecret) {
      return res.status(401).json({ error: "unauthorized" });
    }

    const listId = BREVO_LIST_ONEKAMER_BASE;

    const pageSize = 1000;
    let offset = 0;
    let scanned = 0;
    let candidates = [];

    while (true) {
      const { data, error } = await supabase
        .from("profiles")
        .select("id, email, username")
        .order("created_at", { ascending: true })
        .range(offset, offset + pageSize - 1);

      if (error) return res.status(500).json({ error: error.message || "profiles_read_failed" });

      const rows = Array.isArray(data) ? data : [];
      scanned += rows.length;
      if (rows.length === 0) break;

      for (const r of rows) {
        const email = String(r?.email || "").trim().toLowerCase();
        if (!isValidEmail(email)) continue;
        candidates.push({ email, username: r?.username || null });
      }

      if (rows.length < pageSize) break;
      offset += pageSize;
    }

    const byEmail = new Map();
    for (const c of candidates) {
      if (!byEmail.has(c.email)) byEmail.set(c.email, c);
    }
    const unique = Array.from(byEmail.values());

    const concurrency = 2;
    const r = await mapLimit(unique, concurrency, (c) => brevoUpsertContactWithRetry({ ...c, listId }));

    return res.json({
      listId,
      scanned,
      candidates: unique.length,
      synced: r.ok,
      failed: r.fail,
      failedSamples: r.failedSamples,
    });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

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
    console.warn("⚠️ Log insert exception:", e?.message || e);
  }
}

function getFirebaseServiceAccountFromEnv() {
  const raw = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
  if (!raw) return null;
  try {
    if (raw.trim().startsWith("{")) return JSON.parse(raw);
    const json = Buffer.from(raw, "base64").toString("utf8");
    return JSON.parse(json);
  } catch (_e) {
    return null;
  }
}

function initFirebaseAdminOnce() {
  try {
    if (admin.apps && admin.apps.length > 0) return true;
    const sa = getFirebaseServiceAccountFromEnv();
    if (!sa) return false;
    admin.initializeApp({ credential: admin.credential.cert(sa) });
    return true;
  } catch (_e) {
    return false;
  }
}

// ======================
// 🍎 APNs (iOS) support
// ======================
const APNS_TEAM_ID = process.env.APNS_TEAM_ID;
const APNS_KEY_ID = process.env.APNS_KEY_ID;
const APNS_BUNDLE_ID = process.env.APNS_BUNDLE_ID; // topic
const APNS_PRIVATE_KEY = process.env.APNS_PRIVATE_KEY;
const APNS_ENV = (process.env.APNS_ENV || "sandbox").toLowerCase();

let apnProvider = null;
function getApnProvider() {
  if (apnProvider) return apnProvider;
  if (!APNS_TEAM_ID || !APNS_KEY_ID || !APNS_BUNDLE_ID || !APNS_PRIVATE_KEY) {
    return null;
  }
  const key = APNS_PRIVATE_KEY.includes("\\n") ? APNS_PRIVATE_KEY.replace(/\\n/g, "\n") : APNS_PRIVATE_KEY;
  apnProvider = new apn.Provider({
    token: { key, keyId: APNS_KEY_ID, teamId: APNS_TEAM_ID },
    production: APNS_ENV === "production",
  });
  return apnProvider;
}

async function sendApnsToUsers({ targetUserIds = [], title = "OneKamer", message = "", url = "/", data = {} }) {
  if (!Array.isArray(targetUserIds) || targetUserIds.length === 0) return { sent: 0, tokens: 0 };
  const provider = getApnProvider();
  if (!provider) return { sent: 0, tokens: 0, skipped: "apns_not_configured" };

  const { data: rows, error } = await supabase
    .from("device_push_tokens")
    .select("user_id, token")
    .in("user_id", targetUserIds)
    .eq("platform", "ios")
    .eq("provider", "apns")
    .eq("enabled", true);

  if (error) return { sent: 0, tokens: 0, skipped: "tokens_read_error" };

  const tokens = (rows || []).map((r) => r.token).filter(Boolean);
  if (tokens.length === 0) return { sent: 0, tokens: 0 };

  let sent = 0;
  for (const t of tokens) {
    const note = new apn.Notification();
    note.topic = APNS_BUNDLE_ID;
    note.alert = { title: title || "OneKamer", body: message || "" };
    note.sound = "default";
    note.pushType = "alert";
    note.priority = 10;
    note.payload = { data, url };
    try {
      const result = await provider.send(note, t);
      sent += result?.sent?.length || 0;
    } catch (_e) {
      // best-effort
    }
  }
  return { sent, tokens: tokens.length };
}

async function sendNativeFcmToUsers({ targetUserIds = [], title = "OneKamer", message = "", url = "/", data = {} }) {
  if (!Array.isArray(targetUserIds) || targetUserIds.length === 0) return { sent: 0, tokens: 0 };
  if (!initFirebaseAdminOnce()) return { sent: 0, tokens: 0, skipped: "firebase_not_configured" };

  const { data: rows, error } = await supabase
    .from("device_push_tokens")
    .select("token")
    .in("user_id", targetUserIds)
    .eq("enabled", true)
    .eq("provider", "fcm");

  if (error) return { sent: 0, tokens: 0, skipped: "tokens_read_error" };

  const tokens = (rows || []).map((r) => r.token).filter(Boolean);
  if (tokens.length === 0) return { sent: 0, tokens: 0 };

  try {
    const resp = await admin.messaging().sendEachForMulticast({
      tokens,
      notification: { title: title || "OneKamer", body: message || "" },
      data: {
        url: String(url || "/"),
        payload: JSON.stringify(data || {}),
      },
    });

    const sent = resp?.successCount ?? 0;
    return { sent, tokens: tokens.length };
  } catch (_e) {
    return { sent: 0, tokens: tokens.length, skipped: "send_error" };
  }
}

const loggedAliases = new Set();
function logAliasOnce(message) {
  if (loggedAliases.has(message)) return;
  loggedAliases.add(message);
  console.log(message);
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

async function stripeWebhookHandler(req, res) {
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
    if (event.type === "payment_intent.succeeded") {
      const pi = event.data.object;
      const md = pi?.metadata || {};
      const type = String(md.type || "");

      // OK COINS via PaymentIntent
      if (type === "okcoins_pack") {
        const userId = md.userId || md.user_id || null;
        const packId = md.packId || md.pack_id || null;
        if (userId && packId) {
          try {
            const { error } = await supabase.rpc("okc_grant_pack_after_payment", {
              p_user: userId,
              p_pack_id: parseInt(packId, 10),
              p_status: "paid",
            });
            if (error) {
              await logEvent({ category: "okcoins", action: "pi.succeeded.credit", status: "error", userId, context: { packId, rpc_error: error.message, payment_intent_id: pi.id } });
            } else {
              await logEvent({ category: "okcoins", action: "pi.succeeded.credit", status: "success", userId, context: { packId, payment_intent_id: pi.id } });
              // Décerner les badges OK Coins éventuellement atteints
              try { await awardOkCoinsBadges(userId); } catch {}
            }
          } catch (e) {
            await logEvent({ category: "okcoins", action: "pi.succeeded.credit", status: "error", userId, context: { packId, error: e?.message || String(e), payment_intent_id: pi.id } });
          }
        }
        return res.json({ received: true });
      }

      // Paiement Évènement via PaymentIntent
      if (type === "event_payment") {
        const userId = md.userId || null;
        const eventId = md.eventId || null;
        const amountPaidNow = typeof pi?.amount === "number" ? pi.amount : 0;
        if (userId && eventId && amountPaidNow > 0) {
          try {
            const { data: ev, error: evErr } = await supabase
              .from("evenements")
              .select("id, price_amount, currency")
              .eq("id", eventId)
              .maybeSingle();
            if (evErr) throw new Error(evErr.message);

            const amountTotal = typeof ev?.price_amount === "number" ? ev.price_amount : 0;
            if (!amountTotal || amountTotal <= 0) {
              await logEvent({ category: "event_payment", action: "pi.succeeded.skipped", status: "info", userId, context: { eventId, reason: "event_not_payable", payment_intent_id: pi.id } });
              return res.json({ received: true });
            }

            const { data: existingPay, error: getPayErr } = await supabase
              .from("event_payments")
              .select("id, amount_total, amount_paid")
              .eq("event_id", eventId)
              .eq("user_id", userId)
              .maybeSingle();
            if (getPayErr) throw new Error(getPayErr.message);

            const prevPaid = typeof existingPay?.amount_paid === "number" ? existingPay.amount_paid : 0;
            const newPaid = Math.min(prevPaid + amountPaidNow, amountTotal);
            const newStatus = newPaid >= amountTotal ? "paid" : newPaid > 0 ? "deposit_paid" : "unpaid";

            const upsertPayload = {
              event_id: eventId,
              user_id: userId,
              amount_total: amountTotal,
              amount_paid: newPaid,
              currency: ev?.currency ? String(ev.currency).toLowerCase() : null,
              status: newStatus,
              stripe_payment_intent_id: pi.id,
              updated_at: new Date().toISOString(),
            };

            const { error: upsertErr } = await supabase
              .from("event_payments")
              .upsert(upsertPayload, { onConflict: "event_id,user_id" });
            if (upsertErr) throw new Error(upsertErr.message);

            const { data: existingQr, error: qrErr } = await supabase
              .from("event_qrcodes")
              .select("id")
              .eq("event_id", eventId)
              .eq("user_id", userId)
              .eq("status", "active")
              .maybeSingle();
            if (qrErr) throw new Error(qrErr.message);

            if (!existingQr) {
              const qrcode_value = crypto.randomUUID();
              const { error: insQrErr } = await supabase
                .from("event_qrcodes")
                .insert([{ user_id: userId, event_id: eventId, qrcode_value }]);
              if (insQrErr) throw new Error(insQrErr.message);
            }

            await logEvent({
              category: "event_payment",
              action: "pi.succeeded",
              status: "success",
              userId,
              context: { eventId, amountPaidNow, amountPaid: newPaid, amountTotal, paymentStatus: newStatus, payment_intent_id: pi.id },
            });
          } catch (e) {
            await logEvent({
              category: "event_payment",
              action: "pi.succeeded",
              status: "error",
              userId: md.userId || null,
              context: { eventId: md.eventId || null, error: e?.message || String(e), payment_intent_id: pi.id },
            });
          }
        }
        return res.json({ received: true });
      }

      if (type === "market_order") {
        const orderId = md.market_order_id || null;
        const customerUserId = md.customer_user_id || null;
        try {
          const { error: evtErr } = await supabase
            .from("stripe_events")
            .insert({ event_id: event.id });
          if (evtErr && evtErr.code === "23505") {
            await logEvent({ category: "marketplace", action: "pi.succeeded.duplicate", status: "info", userId: customerUserId, context: { orderId, payment_intent_id: pi.id, event_id: event.id } });
            return res.json({ received: true });
          }
        } catch {}

        if (orderId) {
          try {
            const nowIso = new Date().toISOString();
            let existingOrder = null;
            try {
              const { data: ord } = await supabase
                .from("partner_orders")
                .select("id, fulfillment_status")
                .eq("id", orderId)
                .maybeSingle();
              existingOrder = ord || null;
            } catch {}

            const updatePayload = { status: "paid", updated_at: nowIso };
            const currentFs = String(existingOrder?.fulfillment_status || "").toLowerCase();
            if (!["shipping", "delivered", "completed"].includes(currentFs)) {
              updatePayload.fulfillment_status = "sent_to_seller";
              updatePayload.fulfillment_updated_at = nowIso;
            }
            await supabase.from("partner_orders").update(updatePayload).eq("id", orderId);

            const { data: payRow, error: readErr } = await supabase
              .from("partner_order_payments")
              .select("id")
              .eq("order_id", orderId)
              .maybeSingle();
            if (readErr) throw new Error(readErr.message);

            if (payRow?.id) {
              const { error: upErr } = await supabase
                .from("partner_order_payments")
                .update({ status: "succeeded" })
                .eq("id", payRow.id);
              if (upErr) throw new Error(upErr.message);
            } else {
              const { error: insErr } = await supabase
                .from("partner_order_payments")
                .insert({ order_id: orderId, status: "succeeded" });
              if (insErr) throw new Error(insErr.message);
            }

            await logEvent({ category: "marketplace", action: "pi.succeeded", status: "success", userId: customerUserId, context: { orderId, payment_intent_id: pi.id } });

            try {
              let sellerId = null;
              try {
                const { data: orderRow } = await supabase
                  .from("partner_orders")
                  .select("id, partner_id, customer_user_id")
                  .eq("id", orderId)
                  .maybeSingle();
                if (orderRow?.partner_id) {
                  try {
                    const { data: carts } = await supabase
                      .from("market_carts")
                      .select("id")
                      .eq("user_id", customerUserId)
                      .eq("partner_id", orderRow.partner_id)
                      .eq("status", "active");
                    const cartIds = Array.isArray(carts) ? carts.map((c) => c.id).filter(Boolean) : [];
                    if (cartIds.length > 0) {
                      await supabase.from("market_cart_items").delete().in("cart_id", cartIds);
                      await supabase.from("market_carts").delete().in("id", cartIds);
                    }
                  } catch {}
                  const { data: partner } = await supabase
                    .from("partners_market")
                    .select("owner_user_id")
                    .eq("id", orderRow.partner_id)
                    .maybeSingle();
                  sellerId = partner?.owner_user_id ? String(partner.owner_user_id) : null;
                }
              } catch {}

              let buyerName = null;
              try {
                const { data: buyer } = await supabase
                  .from("profiles")
                  .select("username")
                  .eq("id", customerUserId)
                  .maybeSingle();
                buyerName = buyer?.username || null;
              } catch {}

              if (sellerId) {
                const code = await getOrderDisplayCode(orderId);
                await sendSupabaseLightPush(req, {
                  title: "Marketplace",
                  message: `Nouvelle commande\nCliquez pour l’accepter.`,
                  targetUserIds: [sellerId],
                  url: `/market/orders/${orderId}`,
                  data: { type: "market_order_paid", orderId, orderNumber: code },
                });
              }

              if (customerUserId) {
                const code = await getOrderDisplayCode(orderId);
                await sendSupabaseLightPush(req, {
                  title: "Marketplace",
                  message: `Commande n°${code} confirmée\nLa boutique a bien reçu votre commande.`,
                  targetUserIds: [String(customerUserId)],
                  url: `/market/orders/${orderId}`,
                  data: { type: "market_order_payment_confirmed", orderId, orderNumber: code },
                });
              }
            } catch {}
          } catch (e) {
            await logEvent({ category: "marketplace", action: "pi.succeeded", status: "error", userId: customerUserId, context: { orderId, error: e?.message || String(e), payment_intent_id: pi.id } });
          }
        }
        return res.json({ received: true });
      }

      // Autres types PI: on ignore
      return res.json({ received: true });
    }
    if (event.type === "account.updated" || event.type === "v2.core.account.updated") {
      const account = event.data.object;
      const accountId = account?.id ? String(account.id) : null;

      if (accountId) {
        const detailsSubmitted = account?.details_submitted === true;
        const chargesEnabled = account?.charges_enabled === true;
        const payoutsEnabled = account?.payouts_enabled === true;
        const payoutStatus = detailsSubmitted && chargesEnabled && payoutsEnabled ? "complete" : "incomplete";

        try {
          await supabase
            .from("partners_market")
            .update({
              payout_status: payoutStatus,
              updated_at: new Date().toISOString(),
            })
            .eq("stripe_connect_account_id", accountId);

          await logEvent({
            category: "marketplace",
            action: "connect.account.updated",
            status: "success",
            context: {
              stripe_connect_account_id: accountId,
              payout_status: payoutStatus,
              details_submitted: detailsSubmitted,
              charges_enabled: chargesEnabled,
              payouts_enabled: payoutsEnabled,
            },
          });
        } catch (e) {
          await logEvent({
            category: "marketplace",
            action: "connect.account.updated",
            status: "error",
            context: {
              stripe_connect_account_id: accountId,
              error: e?.message || String(e),
            },
          });
        }
      }

      return res.json({ received: true });
    }

    if (event.type === "checkout.session.completed") {
      const session = event.data.object;
      const { userId, packId, planKey, promoCode, eventId, paymentMode } = session.metadata || {};

      if (eventId && userId && session.mode === "payment") {
        try {
          const paidAmount = typeof session.amount_total === "number" ? session.amount_total : 0;

          const { data: ev, error: evErr } = await supabase
            .from("evenements")
            .select("id, price_amount, currency")
            .eq("id", eventId)
            .maybeSingle();
          if (evErr) throw new Error(evErr.message);

          const amountTotal = typeof ev?.price_amount === "number" ? ev.price_amount : null;
          const currency = ev?.currency ? String(ev.currency).toLowerCase() : null;

          if (!amountTotal || amountTotal <= 0 || !currency) {
            await logEvent({
              category: "event_payment",
              action: "checkout.completed.skipped",
              status: "info",
              userId,
              context: { reason: "event_not_payable_or_missing_currency", eventId, session_id: session.id },
            });
            return res.json({ received: true });
          }

          const { data: existingPay, error: getPayErr } = await supabase
            .from("event_payments")
            .select("id, amount_total, amount_paid")
            .eq("event_id", eventId)
            .eq("user_id", userId)
            .maybeSingle();
          if (getPayErr) throw new Error(getPayErr.message);

          const prevPaid = typeof existingPay?.amount_paid === "number" ? existingPay.amount_paid : 0;
          const newPaid = Math.min(prevPaid + paidAmount, amountTotal);
          const newStatus = newPaid >= amountTotal ? "paid" : newPaid > 0 ? "deposit_paid" : "unpaid";

          const upsertPayload = {
            event_id: eventId,
            user_id: userId,
            amount_total: amountTotal,
            amount_paid: newPaid,
            currency,
            status: newStatus,
            stripe_checkout_session_id: session.id,
            stripe_payment_intent_id: session.payment_intent || null,
            updated_at: new Date().toISOString(),
          };

          const { error: upsertErr } = await supabase
            .from("event_payments")
            .upsert(upsertPayload, { onConflict: "event_id,user_id" });
          if (upsertErr) throw new Error(upsertErr.message);

          const { data: existingQr, error: qrErr } = await supabase
            .from("event_qrcodes")
            .select("id")
            .eq("event_id", eventId)
            .eq("user_id", userId)
            .eq("status", "active")
            .maybeSingle();
          if (qrErr) throw new Error(qrErr.message);

          if (!existingQr) {
            const qrcode_value = crypto.randomUUID();
            const { error: insQrErr } = await supabase
              .from("event_qrcodes")
              .insert([{ user_id: userId, event_id: eventId, qrcode_value }]);
            if (insQrErr) throw new Error(insQrErr.message);
          }

          await logEvent({
            category: "event_payment",
            action: "checkout.completed",
            status: "success",
            userId,
            context: {
              eventId,
              paymentMode: paymentMode || null,
              paidAmount,
              amountTotal,
              amountPaid: newPaid,
              paymentStatus: newStatus,
              session_id: session.id,
            },
          });

          return res.json({ received: true });
        } catch (e) {
          console.error("❌ Event payment webhook error:", e?.message || e);
          await logEvent({
            category: "event_payment",
            action: "checkout.completed",
            status: "error",
            userId: userId || null,
            context: { eventId: eventId || null, error: e?.message || String(e), session_id: session.id },
          });
          return res.json({ received: true });
        }
      }

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

          const { data: pack, error: packErr } = await supabase
            .from("okcoins_packs")
            .select("pack_name, price_eur, is_active")
            .eq("id", packId)
            .single();

          if (packErr || !pack || !pack.is_active) {
            await logEvent({
              category: "okcoins",
              action: "checkout.completed.credit",
              status: "error",
              userId,
              context: { packId, error: packErr?.message || "Pack introuvable ou inactif" },
            });
            return res.status(404).json({ error: "Pack introuvable ou inactif" });
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
            // Décerner les badges OK Coins éventuellement atteints
            try { await awardOkCoinsBadges(userId); } catch {}
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

      // Cas 2 : Tracking code promo influenceur (abonnements)
      if (session.mode === "subscription" && promoCode) {
        try {
          const normalizedCode = String(promoCode).trim();
          if (normalizedCode) {
            const { data: promo, error: promoErr } = await supabase
              .from("promo_codes")
              .select("id, code, actif, date_debut, date_fin")
              .eq("code", normalizedCode)
              .maybeSingle();

            if (promoErr) {
              console.error("❌ Erreur lecture promo_codes:", promoErr);
              await logEvent({
                category: "promo",
                action: "usage.lookup",
                status: "error",
                userId,
                context: { promoCode: normalizedCode, error: promoErr.message },
              });
            } else if (promo && promo.actif !== false) {
              const now = new Date();
              const startOk = !promo.date_debut || new Date(promo.date_debut) <= now;
              const endOk = !promo.date_fin || new Date(promo.date_fin) >= now;

              if (startOk && endOk) {
                const amountPaid = typeof session.amount_total === "number" ? session.amount_total : null;

                const { error: usageErr } = await supabase.from("promo_code_usages").insert({
                  promo_code_id: promo.id,
                  user_id: userId || null,
                  plan: planKey || null,
                  stripe_checkout_session_id: session.id,
                  stripe_customer_id: session.customer || null,
                  amount_paid: amountPaid,
                  ok_coins_granted: 0,
                });

                if (usageErr) {
                  console.error("❌ Erreur insert promo_code_usages:", usageErr);
                  await logEvent({
                    category: "promo",
                    action: "usage.insert",
                    status: "error",
                    userId,
                    context: { promoCode: normalizedCode, error: usageErr.message },
                  });
                } else {
                  await logEvent({
                    category: "promo",
                    action: "usage.insert",
                    status: "success",
                    userId,
                    context: { promoCode: normalizedCode, planKey, session_id: session.id },
                  });
                }
              }
            }
          }
        } catch (e) {
          console.error("❌ Exception tracking promo_code:", e?.message || e);
          await logEvent({
            category: "promo",
            action: "usage.exception",
            status: "error",
            userId,
            context: { promoCode, error: e?.message || String(e) },
          });
        }
      }

      // Cas 3 : Abonnement Stripe (Standard / VIP)
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

      // Cas 4 : Achat unique “VIP à vie”
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
}

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

// ============================================================
// 🧹 Admin : suppression posts Échange communautaire (PROD)
//   - JWT obligatoire (Authorization: Bearer ...)
//   - Vérifie profiles.is_admin
//   - Suppression via service-role (bypass RLS)
// ============================================================

async function verifyIsAdminJWT(req) {
  const authHeader = req.headers["authorization"] || "";
  const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : null;
  if (!token) return { ok: false, reason: "unauthorized" };

  const supabaseAuth = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);
  const { data: userData, error: userErr } = await supabaseAuth.auth.getUser(token);
  if (userErr || !userData?.user) return { ok: false, reason: "invalid_token" };

  const userId = userData.user.id;
  const { data: prof, error: pErr } = await supabase
    .from("profiles")
    .select("id, is_admin, role")
    .eq("id", userId)
    .maybeSingle();

  if (pErr) return { ok: false, reason: "forbidden" };
  const isAdmin =
    prof?.is_admin === true ||
    prof?.is_admin === 1 ||
    prof?.is_admin === "true" ||
    String(prof?.role || "").toLowerCase() === "admin";
  if (!isAdmin) return { ok: false, reason: "forbidden" };

  return { ok: true, userId };
}

app.get("/api/admin/users", async (req, res) => {
  try {
    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const searchRaw = req.query.search ? String(req.query.search).trim() : "";
    const search = searchRaw.length ? searchRaw : "";
    const limitRaw = req.query.limit;
    const offsetRaw = req.query.offset;
    const limit = Math.min(Math.max(parseInt(limitRaw, 10) || 20, 1), 50);
    const offset = Math.max(parseInt(offsetRaw, 10) || 0, 0);

    const emailSearchMode = search.includes("@");
    let items = [];
    let total = null;

    if (emailSearchMode && supabase?.auth?.admin?.listUsers) {
      const page = Math.floor(offset / limit) + 1;
      const { data: uData, error: uErr } = await supabase.auth.admin.listUsers({ page, perPage: limit });
      if (uErr) return res.status(500).json({ error: uErr.message || "Erreur lecture utilisateurs" });

      const users = Array.isArray(uData?.users) ? uData.users : [];
      const q = search.toLowerCase();
      const filtered = users.filter((u) => String(u?.email || "").toLowerCase().includes(q));
      const ids = filtered.map((u) => u.id).filter(Boolean);
      if (ids.length === 0) return res.json({ items: [], total: 0, limit, offset });

      const { data: profs, error: pErr } = await supabase
        .from("profiles")
        .select("id, username, full_name, plan, role, is_admin, show_online_status, last_seen_at")
        .in("id", ids);
      if (pErr) return res.status(500).json({ error: pErr.message || "Erreur lecture profiles" });

      const profById = new Map((profs || []).map((p) => [String(p.id), p]));
      items = filtered
        .map((u) => {
          const p = profById.get(String(u.id));
          if (!p) return null;
          return {
            id: p.id,
            username: p.username || null,
            full_name: p.full_name || null,
            email: u.email || null,
            plan: p.plan || null,
            role: p.role || null,
            is_admin: p.is_admin,
            show_online_status: p.show_online_status,
            last_seen_at: p.last_seen_at,
          };
        })
        .filter(Boolean);
      total = items.length;
      return res.json({ items, total, limit, offset });
    }

    let q = supabase
      .from("profiles")
      .select("id, username, full_name, plan, role, is_admin, show_online_status, last_seen_at", { count: "exact" })
      .order("updated_at", { ascending: false });

    if (search) {
      q = q.or(`username.ilike.%${search}%,full_name.ilike.%${search}%`);
    }

    const { data: profs, error: pErr, count } = await q.range(offset, offset + limit - 1);
    if (pErr) return res.status(500).json({ error: pErr.message || "Erreur lecture profiles" });
    total = typeof count === "number" ? count : null;

    const emailByUserId = {};
    const ids = Array.isArray(profs) ? profs.map((p) => p?.id).filter(Boolean) : [];
    if (ids.length > 0 && supabase?.auth?.admin?.getUserById) {
      await Promise.all(
        ids.map(async (uid) => {
          try {
            const { data: uData, error: uErr } = await supabase.auth.admin.getUserById(uid);
            if (uErr) return;
            const email = String(uData?.user?.email || "").trim();
            if (email) emailByUserId[String(uid)] = email;
          } catch {
            // ignore
          }
        })
      );
    }

    items = (profs || []).map((p) => {
      const pid = p?.id ? String(p.id) : null;
      return {
        id: p.id,
        username: p.username || null,
        full_name: p.full_name || null,
        email: pid ? emailByUserId[pid] || null : null,
        plan: p.plan || null,
        role: p.role || null,
        is_admin: p.is_admin,
        show_online_status: p.show_online_status,
        last_seen_at: p.last_seen_at,
      };
    });

    if (search && !emailSearchMode) {
      const lowered = search.toLowerCase();
      const emailMatched = items.filter((it) => String(it.email || "").toLowerCase().includes(lowered));
      if (emailMatched.length > 0) {
        const mergedById = new Map(items.map((it) => [String(it.id), it]));
        emailMatched.forEach((it) => mergedById.set(String(it.id), it));
        items = Array.from(mergedById.values());
      }
    }

    return res.json({ items, total, limit, offset });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.get("/api/admin/invites/users-stats", async (req, res) => {
  try {
    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const period = String(req.query?.period || "30d").toLowerCase();
    const search = String(req.query?.search || "").trim();
    const suggestRaw = String(req.query?.suggest || "").toLowerCase();
    const suggest = suggestRaw === "1" || suggestRaw === "true" || suggestRaw === "yes";
    const suggestLimitRaw = req.query?.suggest_limit;
    const suggestLimit = Math.min(Math.max(parseInt(suggestLimitRaw, 10) || 10, 1), 20);
    const limitRaw = req.query?.limit;
    const offsetRaw = req.query?.offset;
    const limit = Math.min(Math.max(parseInt(limitRaw, 10) || 20, 1), 50);
    const offset = Math.max(parseInt(offsetRaw, 10) || 0, 0);

    if (suggest) {
      const q = search.toLowerCase();
      if (!q) return res.json({ items: [], limit: suggestLimit });

      const maxScan = 20000;
      const { data: invitesRaw, error: invErr } = await supabase
        .from("invites")
        .select("inviter_user_id, created_at")
        .is("revoked_at", null)
        .order("created_at", { ascending: false })
        .limit(maxScan);
      if (invErr) return res.status(500).json({ error: invErr.message || "invite_read_failed" });

      const inviterIds = Array.from(
        new Set((invitesRaw || []).map((i) => i?.inviter_user_id).filter(Boolean).map((id) => String(id)))
      );
      if (inviterIds.length === 0) return res.json({ items: [], limit: suggestLimit });

      const { data: profs, error: pErr } = await supabase
        .from("profiles")
        .select("id, username")
        .in("id", inviterIds);
      if (pErr) return res.status(500).json({ error: pErr.message || "profiles_read_failed" });

      const profList = Array.isArray(profs) ? profs : [];
      const base = profList
        .map((p) => ({
          id: p?.id || null,
          username: p?.username || null,
        }))
        .filter((p) => p.id);

      const itemsById = new Map();
      const usernameMatched = base.filter((p) => String(p.username || "").toLowerCase().includes(q));
      usernameMatched.forEach((p) => itemsById.set(String(p.id), { ...p, email: null }));

      if (itemsById.size < suggestLimit && supabase?.auth?.admin?.getUserById) {
        const remaining = suggestLimit - itemsById.size;
        const maxEmailChecks = Math.min(Math.max(remaining * 8, 50), 300);
        const idsToCheck = base
          .map((p) => String(p.id))
          .filter((id) => !itemsById.has(id))
          .slice(0, maxEmailChecks);

        for (const uid of idsToCheck) {
          if (itemsById.size >= suggestLimit) break;
          try {
            const { data: uData, error: uErr } = await supabase.auth.admin.getUserById(uid);
            if (uErr) continue;
            const email = String(uData?.user?.email || "").trim();
            if (!email) continue;
            if (!email.toLowerCase().includes(q)) continue;

            const found = base.find((p) => String(p.id) === String(uid)) || null;
            itemsById.set(String(uid), {
              id: uid,
              username: found?.username || null,
              email,
            });
          } catch {
            // ignore
          }
        }
      }

      const items = Array.from(itemsById.values())
        .sort((a, b) => String(a.username || a.email || "").localeCompare(String(b.username || b.email || "")))
        .slice(0, suggestLimit);

      return res.json({ items, limit: suggestLimit });
    }

    let sinceIso = null;
    if (period === "7d") sinceIso = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
    else if (period === "30d") sinceIso = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();

    const maxScan = 10000;
    const baseInvQuery = supabase
      .from("invites")
      .select("code, inviter_user_id, created_at")
      .is("revoked_at", null)
      .order("created_at", { ascending: false })
      .limit(search ? maxScan : limit)
      .range(search ? 0 : offset, search ? Math.min(maxScan - 1, maxScan - 1) : offset + limit - 1);

    const { data: invitesRaw, error: invErr } = await baseInvQuery;
    if (invErr) return res.status(500).json({ error: invErr.message || "invite_read_failed" });

    const invites = Array.isArray(invitesRaw) ? invitesRaw : [];
    const inviterIds = Array.from(new Set(invites.map((i) => i?.inviter_user_id).filter(Boolean)));

    const profById = new Map();
    if (inviterIds.length > 0) {
      const emailByUserId = {};
      if (supabase?.auth?.admin?.getUserById) {
        await Promise.all(
          inviterIds.map(async (uid) => {
            try {
              const { data: uData, error: uErr } = await supabase.auth.admin.getUserById(uid);
              if (uErr) return;
              const email = String(uData?.user?.email || "").trim();
              if (email) emailByUserId[String(uid)] = email;
            } catch {
              // ignore
            }
          })
        );
      }
      const { data: profs, error: pErr } = await supabase
        .from("profiles")
        .select("id, username")
        .in("id", inviterIds);
      if (pErr) return res.status(500).json({ error: pErr.message || "profiles_read_failed" });
      (profs || []).forEach((p) => {
        const uid = p?.id ? String(p.id) : null;
        if (!uid) return;
        profById.set(uid, {
          ...p,
          email: emailByUserId[uid] || null,
        });
      });
    }

    let filtered = invites
      .map((i) => {
        const uid = i?.inviter_user_id ? String(i.inviter_user_id) : null;
        const prof = uid ? profById.get(uid) || null : null;
        return {
          code: i?.code || null,
          inviter_user_id: uid,
          created_at: i?.created_at || null,
          username: prof?.username || null,
          email: prof?.email || null,
        };
      })
      .filter((r) => r.code && r.inviter_user_id);

    if (search) {
      const q = search.toLowerCase();
      filtered = filtered.filter((r) => {
        const u = String(r.username || "").toLowerCase();
        const e = String(r.email || "").toLowerCase();
        return u.includes(q) || e.includes(q);
      });
    }

    const count = filtered.length;
    const paged = filtered.slice(offset, offset + limit);
    const events = ["click", "signup", "first_login", "install"];

    const rows = [];
    for (const r of paged) {
      const stats = {};
      for (const ev of events) {
        let q = supabase
          .from("invite_events")
          .select("id", { count: "exact", head: true })
          .eq("code", r.code)
          .eq("event", ev);
        if (sinceIso) q = q.gte("created_at", sinceIso);
        const { count: c, error: cErr } = await q;
        if (cErr) return res.status(500).json({ error: cErr.message || "stats_read_failed" });
        stats[ev] = c || 0;
      }

      let recentQuery = supabase
        .from("invite_events")
        .select("id, event, created_at, user_id, user_username, user_email, meta")
        .eq("code", r.code)
        .order("created_at", { ascending: false })
        .limit(5);
      if (sinceIso) recentQuery = recentQuery.gte("created_at", sinceIso);
      const { data: recent, error: recErr } = await recentQuery;
      if (recErr) return res.status(500).json({ error: recErr.message || "events_read_failed" });

      rows.push({
        ...r,
        stats,
        recent: recent || [],
      });
    }

    return res.json({ rows, count, limit, offset, period });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.patch("/api/admin/users/:id", bodyParser.json(), async (req, res) => {
  try {
    const { id } = req.params;
    if (!id) return res.status(400).json({ error: "id requis" });

    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const planRaw = req.body?.plan;
    const roleRaw = req.body?.role;

    const plan = planRaw != null ? String(planRaw).toLowerCase().trim() : null;
    const role = roleRaw != null ? String(roleRaw).toLowerCase().trim() : null;

    const allowedPlans = ["free", "standard", "vip"];
    const allowedRoles = ["user", "admin", "qrcode_verif"];

    if (plan && !allowedPlans.includes(plan)) {
      return res.status(400).json({ error: "Plan invalide" });
    }
    if (role && !allowedRoles.includes(role)) {
      return res.status(400).json({ error: "Rôle invalide" });
    }

    const updatePayload = {};
    if (plan) updatePayload.plan = plan;
    if (role) {
      updatePayload.role = role;
      updatePayload.is_admin = role === "admin";
    }

    if (Object.keys(updatePayload).length === 0) {
      return res.status(400).json({ error: "Aucune mise à jour" });
    }

    updatePayload.updated_at = new Date().toISOString();

    const { data: updated, error } = await supabase
      .from("profiles")
      .update(updatePayload)
      .eq("id", id)
      .select("id, username, full_name, plan, role, is_admin")
      .maybeSingle();

    if (error) return res.status(500).json({ error: error.message || "Erreur update profil" });
    if (!updated) return res.status(404).json({ error: "Utilisateur introuvable" });

    let email = null;
    if (supabase?.auth?.admin?.getUserById) {
      try {
        const { data: uData, error: uErr } = await supabase.auth.admin.getUserById(updated.id);
        if (!uErr) email = String(uData?.user?.email || "").trim() || null;
      } catch {
        // ignore
      }
    }

    return res.json({
      item: {
        ...updated,
        email,
      },
    });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.delete("/api/admin/echange/posts/:postId", async (req, res) => {
  try {
    const { postId } = req.params;
    if (!postId) return res.status(400).json({ error: "postId requis" });

    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    await supabase.from("likes").delete().eq("content_type", "post").eq("content_id", postId);
    await supabase.from("comments").delete().eq("content_type", "post").eq("content_id", postId);

    const { error: delErr } = await supabase.from("posts").delete().eq("id", postId);
    if (delErr) return res.status(500).json({ error: delErr.message || "Erreur suppression post" });

    return res.json({ success: true });
  } catch (e) {
    console.error("❌ DELETE /api/admin/echange/posts/:postId:", e);
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.delete("/api/admin/echange/audio/:commentId", async (req, res) => {
  try {
    const { commentId } = req.params;
    if (!commentId) return res.status(400).json({ error: "commentId requis" });

    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const { data: c, error: cErr } = await supabase
      .from("comments")
      .select("id, content_type")
      .eq("id", commentId)
      .maybeSingle();

    if (cErr) return res.status(500).json({ error: cErr.message || "Erreur lecture commentaire" });
    if (!c) return res.status(404).json({ error: "not_found" });
    if (c.content_type !== "echange") return res.status(400).json({ error: "invalid_content_type" });

    const { error: delErr } = await supabase.from("comments").delete().eq("id", commentId);
    if (delErr) return res.status(500).json({ error: delErr.message || "Erreur suppression post vocal" });

    return res.json({ success: true });
  } catch (e) {
    console.error("❌ DELETE /api/admin/echange/audio/:commentId:", e);
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.delete("/api/admin/annonces/:annonceId", async (req, res) => {
  try {
    const { annonceId } = req.params;
    if (!annonceId) return res.status(400).json({ error: "annonceId requis" });

    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const { error: delErr } = await supabase.from("annonces").delete().eq("id", annonceId);
    if (delErr) return res.status(500).json({ error: delErr.message || "Erreur suppression annonce" });
    return res.json({ success: true });
  } catch (e) {
    console.error("❌ DELETE /api/admin/annonces/:annonceId:", e);
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.patch("/api/admin/annonces/:annonceId", bodyParser.json(), async (req, res) => {
  try {
    const { annonceId } = req.params;
    if (!annonceId) return res.status(400).json({ error: "annonceId requis" });

    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const patch = req.body || {};
    const update = {
      updated_at: new Date().toISOString(),
    };

    if (patch.titre !== undefined) update.titre = patch.titre;
    if (patch.categorie_id !== undefined) update.categorie_id = patch.categorie_id;
    if (patch.prix !== undefined) update.prix = patch.prix;
    if (patch.devise_id !== undefined) update.devise_id = patch.devise_id;
    if (patch.pays_id !== undefined) update.pays_id = patch.pays_id;
    if (patch.ville_id !== undefined) update.ville_id = patch.ville_id;
    if (patch.telephone !== undefined) update.telephone = patch.telephone;
    if (patch.email !== undefined) update.email = patch.email;
    if (patch.description !== undefined) update.description = patch.description;
    if (patch.media_url !== undefined) update.media_url = patch.media_url;
    if (patch.media_type !== undefined) update.media_type = patch.media_type;

    if (Object.keys(update).length === 1) return res.status(400).json({ error: "nothing_to_update" });

    const { error } = await supabase.from("annonces").update(update).eq("id", annonceId);
    if (error) return res.status(500).json({ error: error.message || "Erreur mise à jour annonce" });
    return res.json({ success: true });
  } catch (e) {
    console.error("❌ PATCH /api/admin/annonces/:annonceId:", e);
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.delete("/api/admin/evenements/:eventId", async (req, res) => {
  try {
    const { eventId } = req.params;
    if (!eventId) return res.status(400).json({ error: "eventId requis" });

    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const { error: delErr } = await supabase.from("evenements").delete().eq("id", eventId);
    if (delErr) return res.status(500).json({ error: delErr.message || "Erreur suppression événement" });
    return res.json({ success: true });
  } catch (e) {
    console.error("❌ DELETE /api/admin/evenements/:eventId:", e);
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.patch("/api/admin/evenements/:eventId", bodyParser.json(), async (req, res) => {
  try {
    const { eventId } = req.params;
    if (!eventId) return res.status(400).json({ error: "eventId requis" });

    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const patch = req.body || {};
    const update = {
      updated_at: new Date().toISOString(),
    };

    if (patch.title !== undefined) update.title = patch.title;
    if (patch.type_id !== undefined) update.type_id = patch.type_id;
    if (patch.organisateur !== undefined) update.organisateur = patch.organisateur;
    if (patch.date !== undefined) update.date = patch.date;
    if (patch.time !== undefined) update.time = patch.time;
    if (patch.location !== undefined) update.location = patch.location;
    if (patch.latitude !== undefined) update.latitude = patch.latitude;
    if (patch.longitude !== undefined) update.longitude = patch.longitude;
    if (patch.price !== undefined) update.price = patch.price;
    if (patch.devise_id !== undefined) update.devise_id = patch.devise_id;
    if (patch.telephone !== undefined) update.telephone = patch.telephone;
    if (patch.email !== undefined) update.email = patch.email;
    if (patch.site_web !== undefined) update.site_web = patch.site_web;
    if (patch.description !== undefined) update.description = patch.description;
    if (patch.media_url !== undefined) update.media_url = patch.media_url;
    if (patch.media_type !== undefined) update.media_type = patch.media_type;

    if (Object.keys(update).length === 1) return res.status(400).json({ error: "nothing_to_update" });

    const { error } = await supabase.from("evenements").update(update).eq("id", eventId);
    if (error) return res.status(500).json({ error: error.message || "Erreur mise à jour événement" });
    return res.json({ success: true });
  } catch (e) {
    console.error("❌ PATCH /api/admin/evenements/:eventId:", e);
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

async function sendSupabaseLightPush(req, { title, message, targetUserIds, url = "/", data = {} }) {
  if (NOTIF_PROVIDER !== "supabase_light") return { ok: false, skipped: true, reason: "provider_not_supabase_light" };
  if (!Array.isArray(targetUserIds) || targetUserIds.length === 0) {
    return { ok: false, skipped: true, reason: "no_targets" };
  }

  try {
    const baseUrl = `${req.protocol}://${req.get("host")}`;
    // Unifie l'envoi via le dispatcher multi-canaux (Web Push + FCM + APNs)
    const response = await fetch(`${baseUrl}/api/notifications/dispatch`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        title,
        message,
        targetUserIds,
        url,
        data,
      }),
    });

    const resp = await response.json().catch(() => null);
    if (!response.ok || resp?.success !== true) {
      return { ok: false, error: resp?.error || `dispatch_failed_${response.status}` };
    }
    return { ok: true, sent: (resp?.sentWeb || 0) + (resp?.native?.sent || 0) + (resp?.ios?.sent || 0) };
  } catch (e) {
    return { ok: false, error: e?.message || "push_send_exception" };
  }
}

app.post("/api/admin/moderation/warn", async (req, res) => {
  try {
    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const { targetUserId, contentType, contentId, reason, message } = req.body || {};
    if (!targetUserId || !contentType || !contentId || !reason || !message) {
      return res.status(400).json({ error: "targetUserId, contentType, contentId, reason, message requis" });
    }

    const adminUserId = verif.userId;

    const { data: adminProfile, error: adminErr } = await supabase
      .from("profiles")
      .select("id, username")
      .eq("id", adminUserId)
      .maybeSingle();
    if (adminErr) return res.status(500).json({ error: adminErr.message || "Erreur lecture admin" });

    const { data: targetProfile, error: targetErr } = await supabase
      .from("profiles")
      .select("id, username, email")
      .eq("id", targetUserId)
      .maybeSingle();
    if (targetErr) return res.status(500).json({ error: targetErr.message || "Erreur lecture cible" });
    if (!targetProfile) return res.status(404).json({ error: "target_not_found" });

    const adminUsername = adminProfile?.username || "admin";
    const targetUsername = targetProfile?.username || "Utilisateur";

    const insertPayload = {
      admin_user_id: adminUserId,
      admin_username: adminUsername,
      target_user_id: targetUserId,
      target_username: targetUsername,
      content_type: String(contentType),
      content_id: String(contentId),
      action_type: "warning",
      reason: String(reason),
      message: String(message),
      email_sent: false,
      notification_sent: false,
      delivery_error: null,
      meta: { env: "prod" },
    };

    const { data: inserted, error: insErr } = await supabase
      .from("admin_moderation_actions")
      .insert(insertPayload)
      .select("id")
      .maybeSingle();
    if (insErr) return res.status(500).json({ error: insErr.message || "Erreur insertion historique" });

    const actionId = inserted?.id;

    let notifOk = false;
    let emailOk = false;
    let deliveryError = null;

    const notifTitle = `Avertissement de modération`;
    const notifMessage = `Motif : ${reason}\n${message}`;

    const pushRes = await sendSupabaseLightPush(req, {
      title: notifTitle,
      message: notifMessage,
      targetUserIds: [targetUserId],
      url: "https://onekamer.co/echange",
      data: {
        type: "moderation_warning",
        contentType,
        contentId,
        actionId,
      },
    });

    if (pushRes?.ok) {
      notifOk = true;
    } else if (!pushRes?.skipped) {
      deliveryError = pushRes?.error || "push_failed";
    }

    let targetEmail = String(targetProfile?.email || "").trim();
    if (!targetEmail) {
      try {
        const sbAdmin = getSupabaseClient();
        const { data: uData, error: uErr } = await sbAdmin.auth.admin.getUserById(targetUserId);
        if (!uErr) {
          targetEmail = String(uData?.user?.email || "").trim();
        }
      } catch {}
    }

    if (targetEmail) {
      try {
        const subject = "Avertissement de modération — OneKamer";
        const text =
          `Bonjour ${targetUsername},\n\n` +
          `Nous vous contactons suite à un contenu publié sur OneKamer.\n\n` +
          `Motif : ${reason}\n\n` +
          `${message}\n\n` +
          `— L'équipe OneKamer`;

        await sendEmailViaBrevo({ to: targetEmail, subject, text });
        emailOk = true;
      } catch (e) {
        const errMsg = e?.message || "email_failed";
        deliveryError = deliveryError ? `${deliveryError} | ${errMsg}` : errMsg;
      }
    } else {
      deliveryError = deliveryError ? `${deliveryError} | missing_email` : "missing_email";
    }

    if (actionId) {
      try {
        await supabase
          .from("admin_moderation_actions")
          .update({
            email_sent: emailOk,
            notification_sent: notifOk,
            delivery_error: deliveryError,
          })
          .eq("id", actionId);
      } catch {}
    }

    return res.json({
      success: true,
      actionId,
      notification_sent: notifOk,
      email_sent: emailOk,
      delivery_error: deliveryError,
    });
  } catch (e) {
    console.error("❌ POST /api/admin/moderation/warn:", e);
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.get("/api/admin/moderation/actions", async (req, res) => {
  try {
    const verif = await verifyIsAdminJWT(req);
    if (!verif.ok) {
      const status = verif.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: verif.reason });
    }

    const limitRaw = req.query.limit;
    const limit = Math.min(Math.max(parseInt(limitRaw, 10) || 50, 1), 200);

    const { data, error } = await supabase
      .from("admin_moderation_actions")
      .select(
        "id, created_at, admin_user_id, admin_username, target_user_id, target_username, content_type, content_id, action_type, reason, message, email_sent, notification_sent, delivery_error"
      )
      .order("created_at", { ascending: false })
      .limit(limit);

    if (error) return res.status(500).json({ error: error.message || "Erreur lecture historique" });
    return res.json({ actions: data || [] });
  } catch (e) {
    console.error("❌ GET /api/admin/moderation/actions:", e);
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// ============================================================
// 2bis️⃣ Création de session Stripe - Paiement Évènement (full / deposit)
// ============================================================

app.get("/api/events/:eventId", async (req, res) => {
  const { eventId } = req.params;

  try {
    if (!eventId) return res.status(400).json({ error: "eventId requis" });

    const { data: ev, error: evErr } = await supabase
      .from("evenements")
      .select("id, title, date, location, price_amount, currency, deposit_percent")
      .eq("id", eventId)
      .maybeSingle();
    if (evErr) throw new Error(evErr.message);
    if (!ev) return res.status(404).json({ error: "event_not_found" });

    return res.json(ev);
  } catch (e) {
    console.error("❌ GET /api/events/:eventId:", e);
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.post("/api/events/:eventId/checkout", async (req, res) => {
  const { eventId } = req.params;

  try {
    const authHeader = req.headers["authorization"] || "";
    const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : null;
    if (!token) return res.status(401).json({ error: "unauthorized" });

    const supabaseAuth = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);
    const { data: userData, error: userErr } = await supabaseAuth.auth.getUser(token);
    if (userErr || !userData?.user) return res.status(401).json({ error: "invalid_token" });

    const userId = userData.user.id;
    const { payment_mode } = req.body || {};
    const paymentMode = payment_mode === "deposit" ? "deposit" : "full";

    if (!eventId) return res.status(400).json({ error: "eventId requis" });

    const { data: ev, error: evErr } = await supabase
      .from("evenements")
      .select("id, title, price_amount, currency, deposit_percent")
      .eq("id", eventId)
      .maybeSingle();
    if (evErr) throw new Error(evErr.message);
    if (!ev) return res.status(404).json({ error: "event_not_found" });

    const amountTotal = typeof ev.price_amount === "number" ? ev.price_amount : 0;
    const currency = ev.currency ? String(ev.currency).toLowerCase() : null;
    const depositPercent = typeof ev.deposit_percent === "number" ? ev.deposit_percent : null;

    if (!currency || !["eur", "usd", "cad", "xaf"].includes(currency)) {
      return res.status(400).json({ error: "currency_invalid" });
    }
    if (!amountTotal || amountTotal <= 0) {
      return res.status(400).json({ error: "event_not_payable" });
    }

    const { data: pay, error: payErr } = await supabase
      .from("event_payments")
      .select("amount_total, amount_paid, status")
      .eq("event_id", eventId)
      .eq("user_id", userId)
      .maybeSingle();
    if (payErr) throw new Error(payErr.message);

    const alreadyPaid = typeof pay?.amount_paid === "number" ? pay.amount_paid : 0;
    const remaining = Math.max(amountTotal - alreadyPaid, 0);
    if (remaining <= 0) {
      return res.status(200).json({ alreadyPaid: true, message: "Déjà payé" });
    }

    let amountToPay = remaining;
    if (paymentMode === "deposit") {
      if (!depositPercent || depositPercent <= 0) {
        return res.status(400).json({ error: "deposit_not_enabled" });
      }
      const depositAmount = Math.max(1, Math.round((amountTotal * depositPercent) / 100));
      amountToPay = Math.min(depositAmount, remaining);
    }

    const { error: upErr } = await supabase
      .from("event_payments")
      .upsert(
        {
          event_id: eventId,
          user_id: userId,
          status: pay?.status || "unpaid",
          amount_total: amountTotal,
          amount_paid: alreadyPaid,
          currency,
          updated_at: new Date().toISOString(),
        },
        { onConflict: "event_id,user_id" }
      );
    if (upErr) throw new Error(upErr.message);

    const session = await stripe.checkout.sessions.create({
      mode: "payment",
      payment_method_types: ["card"],
      line_items: [
        {
          price_data: {
            currency,
            product_data: { name: `Billet - ${ev.title || "Évènement"}` },
            unit_amount: amountToPay,
          },
          quantity: 1,
        },
      ],
      success_url: `${process.env.FRONTEND_URL}/paiement-success?eventId=${eventId}`,
      cancel_url: `${process.env.FRONTEND_URL}/paiement-annule?eventId=${eventId}`,
      metadata: { userId, eventId, paymentMode },
    });

    const { error: updSessionErr } = await supabase
      .from("event_payments")
      .update({ stripe_checkout_session_id: session.id, updated_at: new Date().toISOString() })
      .eq("event_id", eventId)
      .eq("user_id", userId);
    if (updSessionErr) throw new Error(updSessionErr.message);

    await logEvent({
      category: "event_payment",
      action: "checkout.create",
      status: "success",
      userId,
      context: { eventId, paymentMode, amountToPay, amountTotal, currency, session_id: session.id },
    });

    return res.json({ url: session.url });
  } catch (e) {
    console.error("❌ POST /api/events/:eventId/checkout:", e);
    await logEvent({
      category: "event_payment",
      action: "checkout.create",
      status: "error",
      userId: null,
      context: { eventId: req.params?.eventId || null, error: e?.message || String(e) },
    });
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

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
// 9️⃣ Influenceurs & Codes promo (PROD)
//    - Vue admin : stats globales via view_influenceurs_promo_stats
//    - Vue influenceur : stats perso via user_id
// ============================================================

app.get("/admin/influenceurs-promo", cors(), async (req, res) => {
  try {
    assertAdmin(req);

    const { data, error } = await supabase
      .from("view_influenceurs_promo_stats")
      .select("*")
      .order("nom_public", { ascending: true });

    if (error) {
      console.error("❌ Erreur lecture view_influenceurs_promo_stats:", error.message);
      return res.status(500).json({ error: "Erreur lecture des stats influenceurs" });
    }

    res.json({ items: data || [] });
  } catch (e) {
    const status = e.statusCode || 500;
    console.error("❌ /admin/influenceurs-promo (handler):", e);
    res.status(status).json({ error: e.message || "Erreur interne" });
  }
});

app.post("/admin/influenceurs-promo", cors(), async (req, res) => {
  try {
    assertAdmin(req);

    const {
      nom_public,
      identifiant_reseau,
      email,
      code,
      stripe_promotion_code_id,
      date_debut,
      date_fin,
      actif,
      ok_coins_bonus,
    } = req.body || {};

    if (!nom_public || !code || !stripe_promotion_code_id) {
      return res.status(400).json({
        error: "nom_public, code et stripe_promotion_code_id sont requis",
      });
    }

    let linkedUserId = null;
    if (email && typeof email === "string" && email.trim().length > 0) {
      const cleanEmail = email.trim().toLowerCase();
      const { data: profile, error: profileErr } = await supabase
        .from("profiles")
        .select("id, email")
        .ilike("email", cleanEmail)
        .maybeSingle();

      if (profileErr) {
        console.error("❌ Erreur recherche profil par email:", profileErr.message);
        return res.status(500).json({ error: "Erreur recherche profil par email" });
      }

      if (!profile) {
        return res.status(400).json({
          error: "Aucun profil trouvé avec cet email",
        });
      }

      linkedUserId = profile.id;
    }

    const { data: influenceur, error: inflErr } = await supabase
      .from("influenceurs")
      .insert({
        nom_public,
        handle: identifiant_reseau || null,
        canal_principal: null,
        user_id: linkedUserId,
      })
      .select("id")
      .maybeSingle();

    if (inflErr || !influenceur) {
      console.error("❌ Erreur création influenceur:", inflErr?.message || inflErr);
      return res.status(500).json({ error: "Erreur création influenceur" });
    }

    const { data: promo, error: promoErr } = await supabase
      .from("promo_codes")
      .insert({
        influenceur_id: influenceur.id,
        code,
        stripe_promotion_code_id,
        actif: typeof actif === "boolean" ? actif : true,
        date_debut: date_debut || null,
        date_fin: date_fin || null,
        ok_coins_bonus: typeof ok_coins_bonus === "number" ? ok_coins_bonus : 0,
      })
      .select("id")
      .maybeSingle();

    if (promoErr || !promo) {
      console.error("❌ Erreur création promo_codes:", promoErr?.message || promoErr);
      return res.status(500).json({ error: "Erreur création du code promo" });
    }

    return res.json({
      success: true,
      message: "Influenceur et code promo créés",
      promo_code_id: promo.id,
    });
  } catch (e) {
    const status = e.statusCode || 500;
    console.error("❌ /admin/influenceurs-promo (POST handler):", e);
    res.status(status).json({ error: e.message || "Erreur interne" });
  }
});

app.patch("/admin/influenceurs-promo/:promoCodeId", cors(), async (req, res) => {
  try {
    assertAdmin(req);

    const promoCodeId = req.params.promoCodeId;
    const { actif, date_debut, date_fin, ok_coins_bonus, stripe_promotion_code_id } = req.body || {};

    if (!promoCodeId) {
      return res.status(400).json({ error: "promoCodeId requis" });
    }

    const updatePayload = {};
    if (typeof actif === "boolean") updatePayload.actif = actif;
    if (date_debut !== undefined) updatePayload.date_debut = date_debut;
    if (date_fin !== undefined) updatePayload.date_fin = date_fin;
    if (ok_coins_bonus !== undefined) updatePayload.ok_coins_bonus = ok_coins_bonus;
    if (stripe_promotion_code_id !== undefined) updatePayload.stripe_promotion_code_id = stripe_promotion_code_id;

    if (Object.keys(updatePayload).length === 0) {
      return res.status(400).json({ error: "Aucun champ à mettre à jour" });
    }

    const { data, error } = await supabase
      .from("promo_codes")
      .update(updatePayload)
      .eq("id", promoCodeId)
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("❌ Erreur update promo_codes:", error.message);
      return res.status(500).json({ error: "Erreur mise à jour du code promo" });
    }

    res.json({ item: data });
  } catch (e) {
    const status = e.statusCode || 500;
    console.error("❌ /admin/influenceurs-promo/:promoCodeId (handler):", e);
    res.status(status).json({ error: e.message || "Erreur interne" });
  }
});

app.get("/influenceur/mes-stats", cors(), async (req, res) => {
  try {
    const userId = req.query.userId || req.body?.userId;
    if (!userId) {
      return res.status(400).json({ error: "userId requis" });
    }

    const { data, error } = await supabase
      .from("view_influenceurs_promo_stats")
      .select("*")
      .eq("user_id", userId)
      .maybeSingle();

    if (error && error.code !== "PGRST116") {
      console.error("❌ Erreur lecture mes-stats influenceur:", error.message);
      return res.status(500).json({ error: "Erreur lecture des stats" });
    }

    if (!data) {
      return res.json({ item: null });
    }

    res.json({ item: data });
  } catch (e) {
    const status = e.statusCode || 500;
    console.error("❌ /influenceur/mes-stats (handler):", e);
    res.status(status).json({ error: e.message || "Erreur interne" });
  }
});

// ============================================================
// 🔁 Expiration automatique des QR Codes (horaire)
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
  const { userId, planKey, priceId, promoCode } = req.body;

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
    let promotionCodeId = null;

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

    if (promoCode) {
      try {
        const normalizedCode = String(promoCode).trim();
        if (normalizedCode) {
          const { data: promo, error: promoErr } = await supabase
            .from("promo_codes")
            .select("id, code, stripe_promotion_code_id, actif, date_debut, date_fin")
            .eq("code", normalizedCode)
            .maybeSingle();

          if (promoErr) {
            await logEvent({
              category: "promo",
              action: "checkout.lookup",
              status: "error",
              userId,
              context: { promoCode: normalizedCode, error: promoErr.message },
            });
            return res.status(400).json({ error: "Code promo invalide" });
          }

          if (!promo || promo.actif === false) {
            return res.status(400).json({ error: "Code promo inactif ou introuvable" });
          }

          const now = new Date();
          const startOk = !promo.date_debut || new Date(promo.date_debut) <= now;
          const endOk = !promo.date_fin || new Date(promo.date_fin) >= now;

          if (!startOk || !endOk) {
            return res.status(400).json({ error: "Code promo expiré ou non encore valide" });
          }

          if (promo.stripe_promotion_code_id) {
            promotionCodeId = promo.stripe_promotion_code_id;
          }
        }
      } catch (e) {
        console.error("❌ Erreur validation promoCode:", e?.message || e);
        await logEvent({
          category: "promo",
          action: "checkout.exception",
          status: "error",
          userId,
          context: { promoCode, error: e?.message || String(e) },
        });
        return res.status(400).json({ error: "Code promo invalide" });
      }
    }

    const session = await stripe.checkout.sessions.create({
      mode: "subscription",
      payment_method_types: ["card"],
      line_items: [{ price: finalPriceId, quantity: 1 }],
      // Stripe n'autorise pas d'envoyer à la fois allow_promotion_codes et discounts.
      // On autorise la saisie libre de codes promo UNIQUEMENT lorsqu'aucun promotionCodeId
      // n'est déjà fourni depuis notre base (promo_codes).
      ...(promotionCodeId
        ? {}
        : {
            allow_promotion_codes: true,
          }),
      success_url: `${process.env.FRONTEND_URL}/success?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${process.env.FRONTEND_URL}/cancel`,
      metadata: {
        userId,
        planKey,
        ...(promoCode && { promoCode: String(promoCode).trim() }),
      },
      ...(promotionCodeId && {
        discounts: [
          {
            promotion_code: promotionCodeId,
          },
        ],
      }),
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

    // Ne supprime que l'ancienne entrée pour CE user_id et CE endpoint
    await supabase.from("push_subscriptions").delete().eq("endpoint", endpoint).eq("user_id", userId);
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

app.post("/push/register-device", bodyParser.json(), async (req, res) => {
  if (NOTIF_PROVIDER !== "supabase_light") return res.status(200).json({ ignored: true });

  try {
    const body = req.body || {};
    const userId = body.userId || body.user_id || body.uid || null;
    const token = body.token || body.device_token || body.deviceToken || null;
    const platform = body.platform || body.os || null;
    const deviceId = body.deviceId || body.device_id || null;
    const provider = body.provider || "fcm";

    if (!userId || !token || !platform) {
      return res.status(400).json({
        error: "userId/user_id, token/device_token et platform requis",
      });
    }

    const { data: prof, error: profErr } = await supabase
      .from("profiles")
      .select("username, email")
      .eq("id", userId)
      .maybeSingle();

    if (profErr) {
      await logEvent({
        category: "notifications",
        action: "device.register",
        status: "error",
        userId,
        context: { stage: "fetch_profile", error: profErr.message },
      });
      return res.status(500).json({ error: "Erreur lecture profil" });
    }

    const now = new Date().toISOString();
    const { error: upErr } = await supabase.from("device_push_tokens").upsert(
      {
        user_id: userId,
        username: prof?.username || null,
        email: prof?.email || null,
        platform: String(platform),
        provider: String(provider || "fcm"),
        token: String(token),
        device_id: deviceId ? String(deviceId) : null,
        enabled: true,
        last_seen_at: now,
        updated_at: now,
      },
      { onConflict: "token" }
    );

    if (upErr) {
      await logEvent({
        category: "notifications",
        action: "device.register",
        status: "error",
        userId,
        context: { stage: "upsert", error: upErr.message },
      });
      return res.status(500).json({ error: "Erreur enregistrement device" });
    }

    await logEvent({
      category: "notifications",
      action: "device.register",
      status: "success",
      userId,
      context: { platform, deviceId: deviceId || null },
    });

    return res.json({ success: true });
  } catch (e) {
    await logEvent({
      category: "notifications",
      action: "device.register",
      status: "error",
      context: { error: e?.message || e },
    });
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

app.post("/push/unregister-device", bodyParser.json(), async (req, res) => {
  if (NOTIF_PROVIDER !== "supabase_light") return res.status(200).json({ ignored: true });

  try {
    const { userId, token } = req.body || {};
    if (!token) return res.status(400).json({ error: "token requis" });

    const now = new Date().toISOString();
    const { error } = await supabase
      .from("device_push_tokens")
      .update({ enabled: false, updated_at: now })
      .eq("token", token);

    if (error) {
      await logEvent({
        category: "notifications",
        action: "device.unregister",
        status: "error",
        userId: userId || null,
        context: { error: error.message },
      });
      return res.status(500).json({ error: "Erreur désinscription device" });
    }

    await logEvent({
      category: "notifications",
      action: "device.unregister",
      status: "success",
      userId: userId || null,
    });

    return res.json({ success: true });
  } catch (e) {
    await logEvent({
      category: "notifications",
      action: "device.unregister",
      status: "error",
      userId: req.body?.userId || null,
      context: { error: e?.message || e },
    });
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Dispatch notification: insert + envoi Web Push
app.post("/notifications/dispatch", async (req, res) => {
  if (NOTIF_PROVIDER !== "supabase_light") return res.status(200).json({ ignored: true });

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

    let sentWeb = 0;
    if (VAPID_PUBLIC_KEY && VAPID_PRIVATE_KEY) {
      const { data: subs, error: subErr } = await supabase
        .from("push_subscriptions")
        .select("user_id, endpoint, p256dh, auth")
        .in("user_id", targetUserIds);
      if (subErr) console.warn("⚠️ Lecture subscriptions échouée:", subErr.message);

      const icon = "https://onekamer-media-cdn.b-cdn.net/logo/IMG_0885%202.PNG";
      const badge = "https://onekamer-media-cdn.b-cdn.net/android-chrome-72x72.png";
      const payload = (uid) => JSON.stringify({ title: title || "OneKamer", body: message, icon, badge, url, data });

      if (Array.isArray(subs)) {
        for (const s of subs) {
          try {
            await webpush.sendNotification(
              { endpoint: s.endpoint, expirationTime: null, keys: { p256dh: s.p256dh, auth: s.auth } },
              payload(s.user_id)
            );
            sentWeb++;
          } catch (e) {
            console.warn("⚠️ Échec envoi push à", s.user_id, e?.statusCode || e?.message || e);
            if (e?.statusCode === 404 || e?.statusCode === 410) {
              try {
                await supabase.from("push_subscriptions").delete().eq("endpoint", s.endpoint);
              } catch (_) {}
            }
          }
        }
      }
    }

    const nativeResult = await sendNativeFcmToUsers({ title, message, targetUserIds, data, url });
    const iosResult = await sendApnsToUsers({ title, message, targetUserIds, data, url });

    await logEvent({
      category: "notifications",
      action: "dispatch",
      status: "success",
      context: {
        target_count: targetUserIds.length,
        sent_web: sentWeb,
        sent_native: nativeResult?.sent ?? 0,
        sent_ios: iosResult?.sent ?? 0,
        native_tokens: nativeResult?.tokens ?? 0,
        ios_tokens: iosResult?.tokens ?? 0,
        native_skipped: nativeResult?.skipped ?? null,
      },
    });

    res.json({ success: true, sentWeb, native: nativeResult, ios: iosResult });
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
  logAliasOnce("🔁 Alias activé : /api/push/subscribe → /push/subscribe");
  req.url = "/push/subscribe";
  app._router.handle(req, res, next);
});

app.post("/api/notifications/dispatch", (req, res, next) => {
  logAliasOnce("🔁 Alias activé : /api/notifications/dispatch → /notifications/dispatch");
  req.url = "/notifications/dispatch";
  app._router.handle(req, res, next);
});

app.post("/api/notifications/broadcast", (req, res, next) => {
  logAliasOnce("🔁 Alias activé : /api/notifications/broadcast → /notifications/broadcast");
  req.url = "/notifications/broadcast";
  app._router.handle(req, res, next);
});

// Legacy Supabase webhook targets → route vers le nouveau relais Web Push
app.post("/api/supabase-notification", (req, res, next) => {
  logAliasOnce("🔁 Alias activé : /api/supabase-notification → /push/supabase-notification");
  req.url = "/push/supabase-notification";
  app._router.handle(req, res, next);
});

// Alias désinscription
app.post("/api/push/unsubscribe", (req, res, next) => {
  logAliasOnce("🔁 Alias activé : /api/push/unsubscribe → /push/unsubscribe");
  req.url = "/push/unsubscribe";
  app._router.handle(req, res, next);
});

app.post("/api/push/register-device", (req, res, next) => {
  logAliasOnce("🔁 Alias activé : /api/push/register-device → /push/register-device");
  req.url = "/push/register-device";
  app._router.handle(req, res, next);
});

app.post("/api/push/unregister-device", (req, res, next) => {
  logAliasOnce("🔁 Alias activé : /api/push/unregister-device → /push/unregister-device");
  req.url = "/push/unregister-device";
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
          if (e?.statusCode === 404 || e?.statusCode === 410) {
            try {
              await supabase.from("push_subscriptions").delete().eq("endpoint", s.endpoint);
            } catch (_) {}
          }
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
      .select("id, fondateur_id, est_prive, nom")
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

    // Notifier le fondateur (WhatsApp-style: title=username, body=groupName + "\n" + action)
    const founderId = grp.fondateur_id;
    const { data: requesterProf } = await supabase
      .from("profiles")
      .select("username")
      .eq("id", requesterId)
      .maybeSingle();

    const name = requesterProf?.username || "Un membre";
    const groupLabel = grp?.nom || "Espace groupes";
    const title = name;
    const message = `${groupLabel}\nSouhaite rejoindre le groupe.`;
    const url = `${process.env.FRONTEND_URL}/groupes/${groupId}?tab=demandes`;

    // Web Push
    await notifyUsersNative({
      targetUserIds: [founderId],
      title,
      message,
      url,
      data: { type: "group_join_request", groupId, requesterId }
    });
    // FCM
    await sendNativeFcmToUsers({
      title,
      message,
      targetUserIds: [founderId],
      data: { type: "group_join_request", groupId, requesterId },
      url,
    });
    // APNs
    await sendApnsToUsers({
      title,
      message,
      targetUserIds: [founderId],
      data: { type: "group_join_request", groupId, requesterId },
      url,
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
      .select("fondateur_id, nom")
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

    const groupLabel = grp?.nom || "Espace groupes";
    const title = groupLabel;
    const message = `${groupLabel}\nVotre demande a été approuvée.`;
    const url = `${process.env.FRONTEND_URL}/groupes/${reqRow.group_id}`;

    // Web Push
    await notifyUsersNative({
      targetUserIds: [reqRow.requester_id],
      title,
      message,
      url,
      data: { type: "group_join_approved", groupId: reqRow.group_id }
    });
    // FCM
    await sendNativeFcmToUsers({
      title,
      message,
      targetUserIds: [reqRow.requester_id],
      data: { type: "group_join_approved", groupId: reqRow.group_id },
      url,
    });
    // APNs
    await sendApnsToUsers({
      title,
      message,
      targetUserIds: [reqRow.requester_id],
      data: { type: "group_join_approved", groupId: reqRow.group_id },
      url,
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
      .select("id, created_at, title, message, type, link, is_read, content_id")
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
        contentId: n.content_id || null,
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
  logAliasOnce("🔁 Alias activé : /api/notifications → /notifications");
  req.url = "/notifications";
  app._router.handle(req, res, next);
});

app.post("/api/notifications/mark-read", (req, res, next) => {
  logAliasOnce("🔁 Alias activé : /api/notifications/mark-read → /notifications/mark-read");
  req.url = "/notifications/mark-read";
  app._router.handle(req, res, next);
});

app.post("/api/notifications/mark-all-read", (req, res, next) => {
  logAliasOnce("🔁 Alias activé : /api/notifications/mark-all-read → /notifications/mark-all-read");
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

    // Ne pas downgrader un utilisateur VIP à vie
    const { data: perm, error: permErr } = await supabase
      .from("abonnements")
      .select("id")
      .eq("profile_id", userId)
      .eq("is_permanent", true)
      .limit(1)
      .maybeSingle();
    if (!permErr && perm && perm.id) {
      return res.json({ ok: true });
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
  logAliasOnce("🔁 Alias activé : /notifications/onesignal → /send-notification");
  req.url = "/send-notification";
  app._router.handle(req, res, next);
});

// ============================================================
// 6️⃣ Route de santé (Render health check)
// ============================================================

app.get("/", (req, res) => {
  res.send("✅ OneKamer backend est opérationnel !");
});

let MARKET_REMINDERS_LOCK = false;
async function localDispatchNotification({ title, message, targetUserIds, url = "/", data = {} }) {
  const baseUrl = `http://127.0.0.1:${process.env.PORT || 3000}`;
  const r = await fetch(`${baseUrl}/api/notifications/dispatch`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ title, message, targetUserIds, url, data }),
  });
  const j = await r.json().catch(() => null);
  if (!r.ok || j?.success !== true) throw new Error(j?.error || "dispatch_failed");
  return j;
}

function formatOrderShort(o) {
  const n = Number(o && o.order_number);
  if (Number.isFinite(n)) return `n°${String(n).padStart(6, "0")}`;
  return `#${String(o?.id || "")}`;
}

// Canonical order code formatting aligned with Front formatOrderCode
function normalizeShopPrefix(name) {
  try {
    const raw = String(name || "").normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^A-Za-z]/g, '').toUpperCase();
    return (raw.slice(0, 3) || 'OK');
  } catch (_) {
    return 'OK';
  }
}

function formatOrderCodeCanonical({ shopName, createdAt, orderNumber }) {
  const prefix = normalizeShopPrefix(shopName);
  const d = createdAt ? new Date(createdAt) : new Date();
  const year = Number.isNaN(d.getTime()) ? new Date().getFullYear() : d.getFullYear();
  const num = String(Number(orderNumber || 0)).padStart(6, '0');
  return `${prefix}-${year}-${num}`;
}

async function getOrderDisplayCode(orderId) {
  try {
    const { data: o } = await supabase
      .from("partner_orders")
      .select("id, order_number, created_at, partner_id")
      .eq("id", orderId)
      .maybeSingle();
    if (!o) return String(orderId);
    let shopName = null;
    if (o.partner_id) {
      try {
        const { data: p } = await supabase
          .from("partners_market")
          .select("display_name")
          .eq("id", o.partner_id)
          .maybeSingle();
        shopName = p?.display_name || null;
      } catch {}
    }
    const code = formatOrderCodeCanonical({ shopName, createdAt: o.created_at, orderNumber: o.order_number });
    if (code) return code;
    const n = Number(o.order_number);
    if (Number.isFinite(n)) return String(n).padStart(6, '0');
    return String(orderId);
  } catch {
    return String(orderId);
  }
}

function marketMessagePreview(raw) {
  const text = String(raw || '').trim();
  if (!text) return '';
  const hasUrl = /https?:\/\/\S+/i.test(text);
  if (/(https?:\/\/\S+\.(png|jpg|jpeg|gif|webp|avif))(\?|$)/i.test(text)) return 'Message image';
  if (/(https?:\/\/\S+\.(mp4|webm|ogg|mov))(\?|$)/i.test(text)) return 'Message vidéo';
  if (/(https?:\/\/\S+\.(webm|ogg|m4a|mp3))(\?|$)/i.test(text)) return 'Message audio';
  if (hasUrl) return 'Lien';
  let t = text.length > 80 ? text.slice(0, 80) : text;
  if (text.length > 80 && !/…|\.\.\.$/.test(t)) t = `${t}…`;
  return t;
}

async function runMarketplaceRemindersOnce() {
  if (MARKET_REMINDERS_LOCK) return;
  MARKET_REMINDERS_LOCK = true;
  try {
    const now = Date.now();
    const oneDayAgo = new Date(now - 1 * 24 * 60 * 60 * 1000).toISOString();
    const twoDaysAgo = new Date(now - 2 * 24 * 60 * 60 * 1000).toISOString();
    const fiveDaysAgo = new Date(now - 5 * 24 * 60 * 60 * 1000).toISOString();

    const { data: orders1 } = await supabase
      .from("partner_orders")
      .select("id, customer_user_id, order_number, fulfillment_updated_at")
      .eq("status", "paid")
      .eq("fulfillment_status", "delivered")
      .lte("fulfillment_updated_at", oneDayAgo)
      .is("buyer_received_at", null)
      .limit(200);

    const list1 = Array.isArray(orders1) ? orders1 : [];
    for (const o of list1) {
      const uid = o && o.customer_user_id ? String(o.customer_user_id) : null;
      if (!uid) continue;
      const link = `/market/orders/${o.id}`;
      const { data: exist1 } = await supabase
        .from("notifications")
        .select("id")
        .eq("user_id", uid)
        .eq("type", "market_order_receive_reminder")
        .eq("link", link)
        .gt("created_at", new Date(now - 30 * 24 * 60 * 60 * 1000).toISOString())
        .maybeSingle();
      if (exist1 && exist1.id) continue;
      const code = await getOrderDisplayCode(o.id);
      const title = "Marketplace";
      const message = `Commande n°${code}\nPensez à confirmer la réception de votre commande.`;
      await localDispatchNotification({ title, message, targetUserIds: [uid], url: link, data: { type: "market_order_receive_reminder", orderId: o.id, orderNumber: code } });
      await logEvent({ category: "marketplace", action: "reminder.received", status: "success", userId: uid, context: { orderId: o.id } });
    }

    const { data: orders2 } = await supabase
      .from("partner_orders")
      .select("id, customer_user_id, order_number, buyer_received_at")
      .eq("status", "paid")
      .not("buyer_received_at", "is", null)
      .lte("buyer_received_at", twoDaysAgo)
      .limit(200);

    const list2 = Array.isArray(orders2) ? orders2 : [];
    for (const o of list2) {
      const uid = o && o.customer_user_id ? String(o.customer_user_id) : null;
      if (!uid) continue;
      const { data: hasRating } = await supabase
        .from("marketplace_partner_ratings")
        .select("id")
        .eq("order_id", o.id)
        .maybeSingle();
      if (hasRating && hasRating.id) continue;
      const link = `/market/orders/${o.id}`;
      const { data: exist2 } = await supabase
        .from("notifications")
        .select("id")
        .eq("user_id", uid)
        .eq("type", "market_order_review_reminder")
        .eq("link", link)
        .gt("created_at", new Date(now - 30 * 24 * 60 * 60 * 1000).toISOString())
        .maybeSingle();
      if (exist2 && exist2.id) continue;
      const code = await getOrderDisplayCode(o.id);
      const title = "Marketplace";
      const message = `Commande n°${code}\nVotre avis compte ! Laissez un avis sur votre commande.`;
      await localDispatchNotification({ title, message, targetUserIds: [uid], url: link, data: { type: "market_order_review_reminder", orderId: o.id, orderNumber: code } });
      await logEvent({ category: "marketplace", action: "reminder.review", status: "success", userId: uid, context: { orderId: o.id } });
    }

    // Fallback: avis J+5 après completion si pas de buyer_received_at
    const { data: orders3 } = await supabase
      .from("partner_orders")
      .select("id, customer_user_id, order_number, fulfillment_status, fulfillment_updated_at, buyer_received_at")
      .eq("status", "paid")
      .eq("fulfillment_status", "completed")
      .is("buyer_received_at", null)
      .lte("fulfillment_updated_at", fiveDaysAgo)
      .limit(200);

    const list3 = Array.isArray(orders3) ? orders3 : [];
    for (const o of list3) {
      const uid = o && o.customer_user_id ? String(o.customer_user_id) : null;
      if (!uid) continue;
      const { data: hasRating } = await supabase
        .from("marketplace_partner_ratings")
        .select("id")
        .eq("order_id", o.id)
        .maybeSingle();
      if (hasRating && hasRating.id) continue;
      const link = `/market/orders/${o.id}`;
      const { data: exist3 } = await supabase
        .from("notifications")
        .select("id")
        .eq("user_id", uid)
        .eq("type", "market_order_review_reminder")
        .eq("link", link)
        .gt("created_at", new Date(now - 30 * 24 * 60 * 60 * 1000).toISOString())
        .maybeSingle();
      if (exist3 && exist3.id) continue;
      const code = await getOrderDisplayCode(o.id);
      const title = "Marketplace";
      const message = `Commande n°${code}\nVotre avis compte ! Laissez un avis sur votre commande.`;
      await localDispatchNotification({ title, message, targetUserIds: [uid], url: link, data: { type: "market_order_review_reminder", orderId: o.id, orderNumber: code } });
      await logEvent({ category: "marketplace", action: "reminder.review.fallback", status: "success", userId: uid, context: { orderId: o.id } });
    }
  } catch (e) {
    await logEvent({ category: "marketplace", action: "reminders.error", status: "error", context: { error: e?.message || String(e) } });
  } finally {
    MARKET_REMINDERS_LOCK = false;
  }
}

setTimeout(() => { runMarketplaceRemindersOnce().catch(() => {}); }, 30000);
cron.schedule("*/10 * * * *", () => { runMarketplaceRemindersOnce().catch(() => {}); });

// ============================================================
// 🧾 API Facturation Marketplace — Génération/Liste/PDF
// ============================================================

// POST /api/market/partners/:partnerId/invoices/generate { period: "YYYY-MM" }
app.post("/api/market/partners/:partnerId/invoices/generate", async (req, res) => {
  try {
    const { partnerId } = req.params;
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });
    const access = await ensurePartnerAccess(req, partnerId);
    if (!access.ok) return res.status(access.status).json({ error: access.error });

    const periodRaw = String(req.body?.period || "").trim();
    const per = parseMonthPeriod(periodRaw);
    const startIso = per.start.toISOString().slice(0, 10);
    const endIso = per.end.toISOString().slice(0, 10);

    const { data: partner, error: pErr } = await supabase
      .from("partners_market")
      .select("id, display_name, legal_name, billing_address_line1, billing_address_line2, billing_city, billing_postcode, billing_region, billing_country_code, billing_email, country_code, vat_number, vat_validation_status")
      .eq("id", partnerId)
      .maybeSingle();
    if (pErr) return res.status(500).json({ error: pErr.message || "partner_read_failed" });
    if (!partner) return res.status(404).json({ error: "partner_not_found" });

    const { data: orders, error: oErr } = await supabase
      .from("partner_orders")
      .select("id, order_number, status, created_at, charge_currency, platform_fee_amount")
      .eq("partner_id", partnerId)
      .eq("status", "paid")
      .gte("created_at", startIso)
      .lte("created_at", endIso)
      .order("created_at", { ascending: true });
    if (oErr) return res.status(500).json({ error: oErr.message || "orders_read_failed" });
    if (!Array.isArray(orders) || orders.length === 0) return res.status(400).json({ error: "no_paid_orders_in_period" });

    const vat = computeVatSchemeForPartner(partner);
    const vatRate = vat.rate;

    let totalHtCents = 0;
    const lines = [];
    for (const od of orders) {
      let fee = Number(od.platform_fee_amount || 0);
      const curr = String(od.charge_currency || "EUR").toUpperCase();
      if (!Number.isFinite(fee)) fee = 0;
      if (curr !== "EUR") {
        try {
          const conv = await fxService.convertMinorAmount({ amount: fee, fromCurrency: curr, toCurrency: "EUR" });
          fee = Number(conv?.amount || fee);
        } catch {}
      }
      const vatAmount = Math.round(fee * (vatRate / 100));
      const ttc = fee + vatAmount;
      totalHtCents += fee;
      lines.push({ label: `Frais de service Commande #${od.order_number || od.id}` , service_fee_ht: fee, vat_rate: vatRate, vat_amount: vatAmount, total_ttc: ttc, currency: "EUR", order_id: od.id });
    }

    const totalTvaCents = Math.round(totalHtCents * (vatRate / 100));
    const totalTtcCents = totalHtCents + totalTvaCents;

    const year = per.start.getUTCFullYear();
    const nextNum = await generateNextInvoiceNumber({ supabase, year });

    let created = null;
    for (let i = 0; i < 5 && !created; i++) {
      const number = nextNum();
      const { data: ins, error: insErr } = await getSupabaseClient()
        .from("market_invoices")
        .insert({ partner_id: partnerId, number, period_start: startIso, period_end: endIso, currency: "EUR", vat_scheme: vat.scheme, vat_rate: vatRate, total_ht: (totalHtCents/100.0), total_tva: (totalTvaCents/100.0), total_ttc: (totalTtcCents/100.0), status: "draft", vat_note: vat.note, lines_count: lines.length })
        .select("id, number")
        .single();
      if (insErr) {
        if (String(insErr.message || "").includes("duplicate key value") || String(insErr.code || "") === "23505") continue;
        return res.status(500).json({ error: insErr.message || "invoice_insert_failed" });
      }
      created = ins;
    }
    if (!created) return res.status(500).json({ error: "invoice_number_conflict" });

    const invoiceId = created.id;
    const lineRows = lines.map((ln) => ({ invoice_id: invoiceId, order_id: ln.order_id, label: ln.label, service_fee_ht: (ln.service_fee_ht/100.0), vat_rate: ln.vat_rate, vat_amount: (ln.vat_amount/100.0), total_ttc: (ln.total_ttc/100.0), currency: "EUR" }));
    const { error: lineErr } = await getSupabaseClient().from("market_invoice_lines").insert(lineRows);
    if (lineErr) return res.status(500).json({ error: lineErr.message || "invoice_lines_insert_failed" });

    const pdfBuffer = await buildInvoicePdfBuffer({ invoice: { number: created.number, period_start: startIso, period_end: endIso, total_ht_cents: totalHtCents, total_tva_cents: totalTvaCents, total_ttc_cents: totalTtcCents, vat_note: vat.note, issued_at: new Date().toISOString() }, partner, lines });
    const path = `${partnerId}/${year}/${per.label}/${created.number}.pdf`;
    const sb = getSupabaseClient();
    try { await sb.storage.createBucket("invoices", { public: false }); } catch {}
    await uploadInvoicePdf({ bucket: "invoices", path, buffer: pdfBuffer });

    const nowIso = new Date().toISOString();
    const { data: upd, error: updErr } = await sb
      .from("market_invoices")
      .update({ status: "issued", issued_at: nowIso, pdf_bucket: "invoices", pdf_path: path })
      .eq("id", invoiceId)
      .select("id, number, issued_at, pdf_bucket, pdf_path, total_ht, total_tva, total_ttc, vat_scheme, vat_rate, vat_note, lines_count")
      .single();
    if (updErr) return res.status(500).json({ error: updErr.message || "invoice_update_failed" });

    const { data: signed } = await sb.storage.from("invoices").createSignedUrl(path, 60);
    return res.json({ ok: true, invoice: upd, download_url: signed?.signedUrl || null });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// GET /api/market/partners/:partnerId/invoices?limit=&offset=
app.get("/api/market/partners/:partnerId/invoices", async (req, res) => {
  try {
    const { partnerId } = req.params;
    if (!partnerId) return res.status(400).json({ error: "partnerId requis" });
    const access = await ensurePartnerAccess(req, partnerId);
    if (!access.ok) return res.status(access.status).json({ error: access.error });

    const limit = Math.min(Math.max(parseInt(String(req.query.limit || 20), 10), 1), 100);
    const offset = Math.max(parseInt(String(req.query.offset || 0), 10), 0);
    const { data, error, count } = await getSupabaseClient()
      .from("market_invoices")
      .select("id, number, period_start, period_end, currency, total_ht, total_tva, total_ttc, status, issued_at", { count: "exact" })
      .eq("partner_id", partnerId)
      .order("issued_at", { ascending: false })
      .range(offset, offset + limit - 1);
    if (error) return res.status(500).json({ error: error.message || "invoices_read_failed" });
    return res.json({ items: Array.isArray(data) ? data : [], count: typeof count === "number" ? count : null, limit, offset });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// GET /api/market/partners/:partnerId/invoices/:invoiceId/pdf
app.get("/api/market/partners/:partnerId/invoices/:invoiceId/pdf", async (req, res) => {
  try {
    const { partnerId, invoiceId } = req.params;
    const access = await ensurePartnerAccess(req, partnerId);
    if (!access.ok) return res.status(access.status).json({ error: access.error });

    const sb = getSupabaseClient();
    const { data: inv, error } = await sb
      .from("market_invoices")
      .select("id, partner_id, pdf_bucket, pdf_path")
      .eq("id", invoiceId)
      .maybeSingle();
    if (error) return res.status(500).json({ error: error.message || "invoice_read_failed" });
    if (!inv || String(inv.partner_id) !== String(partnerId)) return res.status(404).json({ error: "invoice_not_found" });
    if (!inv.pdf_bucket || !inv.pdf_path) return res.status(400).json({ error: "pdf_not_ready" });

    const { data: signed, error: sErr } = await sb.storage.from(inv.pdf_bucket).createSignedUrl(inv.pdf_path, 600);
    if (sErr) return res.status(500).json({ error: sErr.message || "signed_url_failed" });
    return res.json({ url: signed?.signedUrl || null });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Option 2 — Endpoints déduisant la boutique depuis le JWT (owner_user_id)
// GET /api/market/me/invoices
app.get("/api/market/me/invoices", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const sb = getSupabaseClient();
    const { data: partner, error: pErr } = await sb
      .from("partners_market")
      .select("id")
      .eq("owner_user_id", guard.userId)
      .maybeSingle();
    if (pErr) return res.status(500).json({ error: pErr.message || "partner_read_failed" });
    if (!partner) return res.status(404).json({ error: "no_partner" });

    const partnerId = String(partner.id);
    const limit = Math.min(Math.max(parseInt(String(req.query.limit || 20), 10), 1), 100);
    const offset = Math.max(parseInt(String(req.query.offset || 0), 10), 0);
    const { data, error, count } = await sb
      .from("market_invoices")
      .select("id, number, period_start, period_end, currency, total_ht, total_tva, total_ttc, status, issued_at", { count: "exact" })
      .eq("partner_id", partnerId)
      .order("issued_at", { ascending: false })
      .range(offset, offset + limit - 1);
    if (error) return res.status(500).json({ error: error.message || "invoices_read_failed" });
    return res.json({ items: Array.isArray(data) ? data : [], count: typeof count === "number" ? count : null, limit, offset });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// GET /api/market/me/invoices/:invoiceId/pdf — URL signée TTL 600s
app.get("/api/market/me/invoices/:invoiceId/pdf", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const { invoiceId } = req.params;
    const sb = getSupabaseClient();
    const { data: partner, error: pErr } = await sb
      .from("partners_market")
      .select("id")
      .eq("owner_user_id", guard.userId)
      .maybeSingle();
    if (pErr) return res.status(500).json({ error: pErr.message || "partner_read_failed" });
    if (!partner) return res.status(404).json({ error: "no_partner" });

    const { data: inv, error } = await sb
      .from("market_invoices")
      .select("id, partner_id, pdf_bucket, pdf_path")
      .eq("id", invoiceId)
      .maybeSingle();
    if (error) return res.status(500).json({ error: error.message || "invoice_read_failed" });
    if (!inv || String(inv.partner_id) !== String(partner.id)) return res.status(404).json({ error: "invoice_not_found" });
    if (!inv.pdf_bucket || !inv.pdf_path) return res.status(400).json({ error: "pdf_not_ready" });

    const { data: signed, error: sErr } = await sb.storage.from(inv.pdf_bucket).createSignedUrl(inv.pdf_path, 600);
    if (sErr) return res.status(500).json({ error: sErr.message || "signed_url_failed" });
    return res.json({ url: signed?.signedUrl || null });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// GET /api/market/me/billing — lecture TVA + adresse de facturation
app.get("/api/market/me/billing", async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const sb = getSupabaseClient();
    const { data: partner, error } = await sb
      .from("partners_market")
      .select("id, display_name, legal_name, address, country_code, vat_number, vat_validation_status, billing_address_line1, billing_address_line2, billing_city, billing_postcode, billing_region, billing_country_code, billing_email")
      .eq("owner_user_id", guard.userId)
      .maybeSingle();
    if (error) return res.status(500).json({ error: error.message || "partner_read_failed" });
    if (!partner) return res.status(404).json({ error: "no_partner" });

    return res.json({
      partner_id: partner.id,
      vat: {
        number: partner.vat_number || null,
        validation_status: partner.vat_validation_status || null,
        country_code: partner.country_code || null,
      },
      billing: {
        address_line1: partner.billing_address_line1 || null,
        address_line2: partner.billing_address_line2 || null,
        city: partner.billing_city || null,
        postcode: partner.billing_postcode || null,
        region: partner.billing_region || null,
        country_code: partner.billing_country_code || null,
        email: partner.billing_email || null,
      },
      shop_address: partner.address || null,
    });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// PUT /api/market/me/billing — mise à jour TVA + adresse facturation
app.put("/api/market/me/billing", bodyParser.json(), async (req, res) => {
  try {
    const guard = await requireUserJWT(req);
    if (!guard.ok) return res.status(guard.status).json({ error: guard.error });

    const {
      vat_number,
      vat_validation_status,
      billing_address_line1,
      billing_address_line2,
      billing_city,
      billing_postcode,
      billing_region,
      billing_country_code,
      billing_email,
    } = req.body || {};

    const sb = getSupabaseClient();
    const { data: partner, error: pErr } = await sb
      .from("partners_market")
      .select("id")
      .eq("owner_user_id", guard.userId)
      .maybeSingle();
    if (pErr) return res.status(500).json({ error: pErr.message || "partner_read_failed" });
    if (!partner) return res.status(404).json({ error: "no_partner" });

    const payload = {
      vat_number: typeof vat_number === "string" ? (vat_number.trim() || null) : undefined,
      vat_validation_status: typeof vat_validation_status === "string" ? (vat_validation_status.trim() || null) : undefined,
      billing_address_line1: typeof billing_address_line1 === "string" ? (billing_address_line1.trim() || null) : undefined,
      billing_address_line2: typeof billing_address_line2 === "string" ? (billing_address_line2.trim() || null) : undefined,
      billing_city: typeof billing_city === "string" ? (billing_city.trim() || null) : undefined,
      billing_postcode: typeof billing_postcode === "string" ? (billing_postcode.trim() || null) : undefined,
      billing_region: typeof billing_region === "string" ? (billing_region.trim() || null) : undefined,
      billing_country_code: typeof billing_country_code === "string" ? (billing_country_code.trim().toUpperCase() || null) : undefined,
      billing_email: typeof billing_email === "string" ? (billing_email.trim().toLowerCase() || null) : undefined,
      updated_at: new Date().toISOString(),
    };
    Object.keys(payload).forEach((k) => payload[k] === undefined && delete payload[k]);

    const { error: uErr } = await sb
      .from("partners_market")
      .update(payload)
      .eq("id", partner.id);
    if (uErr) return res.status(500).json({ error: uErr.message || "partner_update_failed" });

    return res.json({ ok: true });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// ============================================================
// 7️⃣ Lancement serveur
// ============================================================

// ============================================================
// 🗓️ Cron mensuel — Auto-génération des factures N-1 (08:00, jour 1)
// ============================================================
let INVOICES_CRON_LOCK = false;
async function runMonthlyInvoicesCronOnce() {
  if (INVOICES_CRON_LOCK) return;
  INVOICES_CRON_LOCK = true;
  const sb = getSupabaseClient();
  try {
    const per = parseMonthPeriod(""); // défaut: mois précédent
    const startIso = per.start.toISOString().slice(0, 10);
    const endIso = per.end.toISOString().slice(0, 10);

    const { data: paidOrders, error: oErr } = await sb
      .from("partner_orders")
      .select("id, partner_id, order_number, status, created_at, charge_currency, platform_fee_amount")
      .eq("status", "paid")
      .gte("created_at", startIso)
      .lte("created_at", endIso)
      .limit(20000);
    if (oErr) {
      await logEvent({ category: "invoices", action: "cron.scan_orders", status: "error", context: { error: oErr.message } });
      return;
    }
    const list = Array.isArray(paidOrders) ? paidOrders : [];
    if (!list.length) return;

    const partnerIds = Array.from(new Set(list.map((o) => String(o.partner_id)).filter(Boolean)));
    for (const partnerId of partnerIds) {
      try {
        const { data: already } = await sb
          .from("market_invoices")
          .select("id")
          .eq("partner_id", partnerId)
          .eq("period_start", startIso)
          .eq("period_end", endIso)
          .maybeSingle();
        if (already && already.id) continue;

        const { data: partner, error: pErr } = await sb
          .from("partners_market")
          .select("id, display_name, legal_name, billing_address_line1, billing_address_line2, billing_city, billing_postcode, billing_region, billing_country_code, billing_email, country_code, vat_number, vat_validation_status")
          .eq("id", partnerId)
          .maybeSingle();
        if (pErr || !partner) continue;

        const partnerOrders = list.filter((o) => String(o.partner_id) === String(partnerId));
        if (!partnerOrders.length) continue;

        const vat = computeVatSchemeForPartner(partner);
        const vatRate = vat.rate;

        let totalHtCents = 0;
        const lines = [];
        for (const od of partnerOrders) {
          let fee = Number(od.platform_fee_amount || 0);
          const curr = String(od.charge_currency || "EUR").toUpperCase();
          if (!Number.isFinite(fee)) fee = 0;
          if (curr !== "EUR") {
            try {
              const conv = await fxService.convertMinorAmount({ amount: fee, fromCurrency: curr, toCurrency: "EUR" });
              fee = Number(conv?.amount || fee);
            } catch {}
          }
          const vatAmount = Math.round(fee * (vatRate / 100));
          const ttc = fee + vatAmount;
          totalHtCents += fee;
          lines.push({ label: `Frais de service Commande #${od.order_number || od.id}` , service_fee_ht: fee, vat_rate: vatRate, vat_amount: vatAmount, total_ttc: ttc, currency: "EUR", order_id: od.id });
        }

        if (!lines.length) continue;

        const totalTvaCents = Math.round(totalHtCents * (vatRate / 100));
        const totalTtcCents = totalHtCents + totalTvaCents;

        const year = per.start.getUTCFullYear();
        const nextNum = await generateNextInvoiceNumber({ supabase: sb, year });

        let created = null;
        for (let i = 0; i < 5 && !created; i++) {
          const number = nextNum();
          const { data: ins, error: insErr } = await sb
            .from("market_invoices")
            .insert({ partner_id: partnerId, number, period_start: startIso, period_end: endIso, currency: "EUR", vat_scheme: vat.scheme, vat_rate: vatRate, total_ht: (totalHtCents/100.0), total_tva: (totalTvaCents/100.0), total_ttc: (totalTtcCents/100.0), status: "draft", vat_note: vat.note, lines_count: lines.length })
            .select("id, number")
            .single();
          if (insErr) {
            if (String(insErr.message || "").includes("duplicate key value") || String(insErr.code || "") === "23505") continue;
            await logEvent({ category: "invoices", action: "cron.insert", status: "error", context: { partnerId, error: insErr.message } });
            break;
          }
          created = ins;
        }
        if (!created) continue;

        const invoiceId = created.id;
        const lineRows = lines.map((ln) => ({ invoice_id: invoiceId, order_id: ln.order_id, label: ln.label, service_fee_ht: (ln.service_fee_ht/100.0), vat_rate: ln.vat_rate, vat_amount: (ln.vat_amount/100.0), total_ttc: (ln.total_ttc/100.0), currency: "EUR" }));
        const { error: lineErr } = await sb.from("market_invoice_lines").insert(lineRows);
        if (lineErr) {
          await logEvent({ category: "invoices", action: "cron.lines", status: "error", context: { partnerId, error: lineErr.message } });
          continue;
        }

        const pdfBuffer = await buildInvoicePdfBuffer({ invoice: { number: created.number, period_start: startIso, period_end: endIso, total_ht_cents: totalHtCents, total_tva_cents: totalTvaCents, total_ttc_cents: totalTtcCents, vat_note: vat.note, issued_at: new Date().toISOString() }, partner, lines });
        const path = `${partnerId}/${year}/${per.label}/${created.number}.pdf`;
        try { await sb.storage.createBucket("invoices", { public: false }); } catch {}
        try {
          await uploadInvoicePdf({ bucket: "invoices", path, buffer: pdfBuffer });
        } catch (e) {
          await logEvent({ category: "invoices", action: "cron.upload", status: "error", context: { partnerId, error: e?.message || String(e) } });
          continue;
        }

        const nowIso = new Date().toISOString();
        const { error: updErr } = await sb
          .from("market_invoices")
          .update({ status: "issued", issued_at: nowIso, pdf_bucket: "invoices", pdf_path: path })
          .eq("id", invoiceId);
        if (updErr) {
          await logEvent({ category: "invoices", action: "cron.update", status: "error", context: { partnerId, error: updErr.message } });
          continue;
        }

        await logEvent({ category: "invoices", action: "cron.issued", status: "success", context: { partnerId, invoiceId } });
      } catch (e) {
        await logEvent({ category: "invoices", action: "cron.partner.error", status: "error", context: { partnerId, error: e?.message || String(e) } });
      }
    }
  } catch (e) {
    await logEvent({ category: "invoices", action: "cron.run.error", status: "error", context: { error: e?.message || String(e) } });
  } finally {
    INVOICES_CRON_LOCK = false;
  }
}

// Planification: 08:00 (heure serveur) le 1er jour de chaque mois
cron.schedule("0 8 1 * *", () => { runMonthlyInvoicesCronOnce().catch(() => {}); });

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Serveur OneKamer actif sur port ${PORT}`);
});
