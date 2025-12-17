import express from "express";
import crypto from "crypto";
import QRCode from "qrcode";
import { createClient } from "@supabase/supabase-js";

const router = express.Router();
router.use(express.json());

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

async function getPaymentSnapshot({ eventId, userId }) {
  try {
    if (!eventId || !userId) return null;

    const { data: ev, error: evErr } = await supabase
      .from("evenements")
      .select("id, price_amount, currency")
      .eq("id", eventId)
      .maybeSingle();
    if (evErr) return null;

    const amountTotal = typeof ev?.price_amount === "number" ? ev.price_amount : null;
    const currency = ev?.currency ? String(ev.currency).toLowerCase() : null;

    if (!amountTotal || amountTotal <= 0 || !currency) {
      return {
        status: "free",
        amount_total: amountTotal,
        amount_paid: 0,
        remaining: 0,
        currency,
      };
    }

    const { data: pay, error: payErr } = await supabase
      .from("event_payments")
      .select("status, amount_total, amount_paid, currency")
      .eq("event_id", eventId)
      .eq("user_id", userId)
      .maybeSingle();
    if (payErr) return null;

    const paid = typeof pay?.amount_paid === "number" ? pay.amount_paid : 0;
    const total = typeof pay?.amount_total === "number" ? pay.amount_total : amountTotal;
    const remaining = Math.max(total - paid, 0);

    return {
      status: pay?.status || (paid >= total ? "paid" : paid > 0 ? "deposit_paid" : "unpaid"),
      amount_total: total,
      amount_paid: paid,
      remaining,
      currency: pay?.currency ? String(pay.currency).toLowerCase() : currency,
    };
  } catch {
    return null;
  }
}

function parseAdminAllowlist() {
  const raw = process.env.QR_ADMIN_USER_IDS || "";
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

function normalizeRole(role) {
  return String(role || "")
    .trim()
    .toLowerCase();
}

async function getUserFromBearer(req) {
  const authHeader = req.headers["authorization"] || "";
  const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : null;
  if (!token) return { ok: false, reason: "unauthorized" };

  const supabaseAuth = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);
  const { data: userData, error: userErr } = await supabaseAuth.auth.getUser(token);
  if (userErr || !userData?.user) return { ok: false, reason: "invalid_token" };
  return { ok: true, user: userData.user };
}

async function canAccessDashboard({ userId, email }) {
  try {
    if (!userId) return { ok: false };

    const { data: prof, error: pErr } = await supabase
      .from("profiles")
      .select("role, is_admin")
      .eq("id", userId)
      .maybeSingle();
    if (pErr) return { ok: false };

    const roleNorm = normalizeRole(prof?.role);
    const isAdmin = prof?.is_admin === true || roleNorm === "admin";
    const isQrVerif = roleNorm === "qrcode_verif";

    if (isAdmin || isQrVerif) return { ok: true, isAdmin, isQrVerif, byEmail: false };

    const em = String(email || "").trim().toLowerCase();
    if (!em) return { ok: false, isAdmin: false, isQrVerif: false, byEmail: false };

    const { data: anyAccess, error: aErr } = await supabase
      .from("event_dashboard_access")
      .select("id")
      .ilike("email", em)
      .limit(1);
    if (aErr) return { ok: false, isAdmin: false, isQrVerif: false, byEmail: false };
    const byEmail = Array.isArray(anyAccess) && anyAccess.length > 0;
    return { ok: byEmail, isAdmin: false, isQrVerif: false, byEmail };
  } catch {
    return { ok: false };
  }
}

async function canAccessEventDashboard({ userId, email, eventId }) {
  try {
    const base = await canAccessDashboard({ userId, email });
    if (base?.ok && (base.isAdmin || base.isQrVerif)) return true;

    const em = String(email || "").trim().toLowerCase();
    if (!em || !eventId) return false;

    const { data: rows, error } = await supabase
      .from("event_dashboard_access")
      .select("id")
      .eq("event_id", eventId)
      .ilike("email", em)
      .limit(1);
    if (error) return false;
    return Array.isArray(rows) && rows.length > 0;
  } catch {
    return false;
  }
}

async function verifyAdminJWT(req) {
  const authHeader = req.headers["authorization"] || "";
  const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : null;
  if (!token) return { ok: false, reason: "unauthorized" };
  const supabaseAuth = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);
  const { data: userData, error: userErr } = await supabaseAuth.auth.getUser(token);
  if (userErr || !userData?.user) return { ok: false, reason: "invalid_token" };
  const uid = userData.user.id;
  try {
    const { data: prof, error: pErr } = await supabase
      .from("profiles")
      .select("role")
      .eq("id", uid)
      .maybeSingle();
    if (pErr) return { ok: false, reason: "forbidden" };
    const roleNorm = normalizeRole(prof?.role);
    const isAdmin = roleNorm === "admin" || roleNorm === "qrcode_verif";
    if (!isAdmin) return { ok: false, reason: "forbidden" };
    return { ok: true, uid };
  } catch {
    return { ok: false, reason: "forbidden" };
  }
}

// Génération d'un QR Code pour un utilisateur/événement
router.post("/qrcode/generate", async (req, res) => {
  try {
    const authHeader = req.headers["authorization"] || "";
    const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : null;
    if (!token) return res.status(401).json({ error: "unauthorized" });
    const supabaseAuth = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);
    const { data: userData, error: userErr } = await supabaseAuth.auth.getUser(token);
    if (userErr || !userData?.user) return res.status(401).json({ error: "invalid_token" });

    const user_id = userData.user.id;
    const { event_id } = req.body || {};
    if (!event_id) return res.status(400).json({ error: "event_id requis" });

    const { data: existing } = await supabase
      .from("event_qrcodes")
      .select("id, qrcode_value, status")
      .eq("user_id", user_id)
      .eq("event_id", event_id)
      .eq("status", "active")
      .maybeSingle();

    if (existing) {
      const qrImage = await QRCode.toDataURL(existing.qrcode_value);
      return res.json({ qrcode_value: existing.qrcode_value, qrImage, status: existing.status });
    }

    const qrcode_value = crypto.randomUUID();
    const { data, error } = await supabase
      .from("event_qrcodes")
      .insert([{ user_id, event_id, qrcode_value }])
      .select("qrcode_value, status")
      .single();

    if (error) return res.status(500).json({ error: error.message });

    const qrImage = await QRCode.toDataURL(qrcode_value);
    return res.json({ qrcode_value, qrImage, status: data?.status || "active" });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Vérifier si l'utilisateur courant est autorisé à scanner (admin ou rôle QRcode_Verif)
router.get("/qrcode/admin/me", async (req, res) => {
  try {
    const authHeader = req.headers["authorization"] || "";
    const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : null;
    if (!token) return res.status(200).json({ isAdmin: false });
    const supabaseAuth = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);
    const { data: userData, error: userErr } = await supabaseAuth.auth.getUser(token);
    if (userErr || !userData?.user) return res.status(200).json({ isAdmin: false });
    const uid = userData.user.id;
    const { data: prof } = await supabase
      .from("profiles")
      .select("role")
      .eq("id", uid)
      .maybeSingle();
    const roleNorm = normalizeRole(prof?.role);
    const isAdmin = roleNorm === "admin" || roleNorm === "qrcode_verif";
    return res.json({ isAdmin: !!isAdmin });
  } catch {
    return res.json({ isAdmin: false });
  }
});

// Dashboard QR (admin / QRcode_verif / organisateur autorisé)
router.get("/admin/dashboard/me", async (req, res) => {
  try {
    const u = await getUserFromBearer(req);
    if (!u.ok) return res.status(200).json({ canAccess: false });

    const check = await canAccessDashboard({ userId: u.user.id, email: u.user.email });
    return res.json({
      canAccess: !!check?.ok,
      isAdmin: !!check?.isAdmin,
      isQrVerif: !!check?.isQrVerif,
      byEmail: !!check?.byEmail,
    });
  } catch {
    return res.status(200).json({ canAccess: false });
  }
});

router.get("/admin/events/:eventId/qrcode-stats", async (req, res) => {
  try {
    const { eventId } = req.params;
    if (!eventId) return res.status(400).json({ error: "eventId requis" });

    const u = await getUserFromBearer(req);
    if (!u.ok) return res.status(401).json({ error: u.reason });

    const allowed = await canAccessEventDashboard({ userId: u.user.id, email: u.user.email, eventId });
    if (!allowed) return res.status(403).json({ error: "forbidden" });

    const { data: ev, error: evErr } = await supabase
      .from("evenements")
      .select("id, title, date, location, price_amount, currency")
      .eq("id", eventId)
      .maybeSingle();
    if (evErr) return res.status(500).json({ error: evErr.message });
    if (!ev) return res.status(404).json({ error: "event_not_found" });

    const { data: qrs, error: qrErr } = await supabase
      .from("event_qrcodes")
      .select("user_id")
      .eq("event_id", eventId)
      .eq("status", "active")
      .limit(5000);
    if (qrErr) return res.status(500).json({ error: qrErr.message });

    const userIds = Array.isArray(qrs) ? qrs.map((r) => r.user_id).filter(Boolean) : [];

    const counts = {
      total_active_qr: userIds.length,
      paid: 0,
      deposit_paid: 0,
      unpaid: 0,
      free: 0,
    };

    const amountTotal = typeof ev?.price_amount === "number" ? ev.price_amount : 0;
    const currency = ev?.currency ? String(ev.currency).toLowerCase() : null;
    const isFree = !amountTotal || amountTotal <= 0 || !currency;

    if (isFree) {
      counts.free = userIds.length;
      return res.json({ event: ev, counts });
    }

    if (userIds.length === 0) {
      return res.json({ event: ev, counts });
    }

    const { data: pays, error: payErr } = await supabase
      .from("event_payments")
      .select("user_id, status")
      .eq("event_id", eventId)
      .in("user_id", userIds);
    if (payErr) return res.status(500).json({ error: payErr.message });

    const statusByUser = new Map();
    (pays || []).forEach((p) => {
      if (p?.user_id) statusByUser.set(p.user_id, String(p.status || "").toLowerCase());
    });

    for (const uid of userIds) {
      const st = statusByUser.get(uid) || "unpaid";
      if (st === "paid") counts.paid++;
      else if (st === "deposit_paid") counts.deposit_paid++;
      else counts.unpaid++;
    }

    return res.json({ event: ev, counts });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Supprimer un QR Code (propriétaire uniquement)
router.delete("/qrcode/:id", async (req, res) => {
  try {
    const authHeader = req.headers["authorization"] || "";
    const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : null;
    if (!token) return res.status(401).json({ error: "unauthorized" });

    const supabaseAuth = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);
    const { data: userData, error: userErr } = await supabaseAuth.auth.getUser(token);
    if (userErr || !userData?.user) return res.status(401).json({ error: "invalid_token" });

    const user_id = userData.user.id;
    const id = req.params.id;
    if (!id) return res.status(400).json({ error: "id requis" });

    const { data: item, error: getErr } = await supabase
      .from("event_qrcodes")
      .select("id, user_id")
      .eq("id", id)
      .maybeSingle();
    if (getErr) return res.status(500).json({ error: getErr.message });
    if (!item) return res.status(404).json({ error: "not_found" });
    if (item.user_id !== user_id) return res.status(403).json({ error: "forbidden" });

    const { error: delErr } = await supabase
      .from("event_qrcodes")
      .delete()
      .eq("id", id)
      .eq("user_id", user_id);
    if (delErr) return res.status(500).json({ error: delErr.message });

    return res.json({ deleted: true });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Vérification via secret admin
router.get("/qrcode/verify", async (req, res) => {
  try {
    const adminSecret = req.headers["x-admin-secret"];
    if (!adminSecret || adminSecret !== process.env.QR_ADMIN_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }

    const qrcode_value = req.query?.qrcode_value;
    if (!qrcode_value) return res.status(400).json({ error: "qrcode_value requis" });

    const { data, error } = await supabase
      .from("event_qrcodes")
      .select("id, status, event_id, user_id, evenements:event_id(title, date, location)")
      .eq("qrcode_value", qrcode_value)
      .maybeSingle();

    if (error) return res.status(500).json({ error: error.message });
    if (!data) return res.status(404).json({ valid: false, message: "QR Code inconnu" });

    if (data.status !== "active") {
      return res.json({ valid: false, message: `QR Code deja ${data.status}` });
    }

    const payment = await getPaymentSnapshot({ eventId: data.event_id, userId: data.user_id });

    await supabase
      .from("event_qrcodes")
      .update({ status: "used", validated_at: new Date().toISOString() })
      .eq("id", data.id);

    return res.json({ valid: true, message: "Entree validee", event: data.evenements || null, payment });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Vérification via JWT admin (allowlist)
router.get("/qrcode/verify-jwt", async (req, res) => {
  try {
    const check = await verifyAdminJWT(req);
    if (!check.ok) {
      const status = check.reason === "forbidden" ? 403 : 401;
      return res.status(status).json({ error: check.reason });
    }

    const qrcode_value = req.query?.qrcode_value;
    if (!qrcode_value) return res.status(400).json({ error: "qrcode_value requis" });

    const { data, error } = await supabase
      .from("event_qrcodes")
      .select("id, status, event_id, user_id, evenements:event_id(title, date, location)")
      .eq("qrcode_value", qrcode_value)
      .maybeSingle();

    if (error) return res.status(500).json({ error: error.message });
    if (!data) return res.status(404).json({ valid: false, message: "QR Code inconnu" });

    if (data.status !== "active") {
      return res.json({ valid: false, message: `QR Code deja ${data.status}` });
    }

    const payment = await getPaymentSnapshot({ eventId: data.event_id, userId: data.user_id });

    await supabase
      .from("event_qrcodes")
      .update({ status: "used", validated_at: new Date().toISOString() })
      .eq("id", data.id);

    return res.json({ valid: true, message: "Entree validee", event: data.evenements || null, payment });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Recherche d'événements (autocomplete)
router.get("/events/search", async (req, res) => {
  try {
    const q = (req.query?.q || "").trim();
    if (!q) return res.json([]);

    const { data, error } = await supabase
      .from("evenements")
      .select("id, title, date, location")
      .ilike("title", `%${q}%`)
      .order("date", { ascending: true })
      .limit(10);

    if (error) return res.status(500).json({ error: error.message });
    return res.json(data || []);
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Mes QR Codes (liste)
router.get("/qrcode/my", async (req, res) => {
  try {
    const authHeader = req.headers["authorization"] || "";
    const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : null;
    if (!token) return res.status(401).json({ error: "unauthorized" });

    const supabaseAuth = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);
    const { data: userData, error: userErr } = await supabaseAuth.auth.getUser(token);
    if (userErr || !userData?.user) return res.status(401).json({ error: "invalid_token" });

    const user_id = userData.user.id;
    const withImage = String(req.query?.withImage || "0") === "1";

    const { data, error } = await supabase
      .from("event_qrcodes")
      .select(
        "id, qrcode_value, status, created_at, validated_at, event_id, evenements:event_id(title, date, location, price_amount, currency)"
      )
      .eq("user_id", user_id)
      .order("created_at", { ascending: false })
      .limit(50);

    if (error) return res.status(500).json({ error: error.message });

    const baseItems = await Promise.all(
      (data || []).map(async (row) => {
        try {
          const payment = await getPaymentSnapshot({ eventId: row.event_id, userId: user_id });
          return { ...row, payment };
        } catch {
          return { ...row, payment: null };
        }
      })
    );

    if (!withImage) return res.json({ items: baseItems });

    const items = await Promise.all(
      baseItems.map(async (row) => {
        try {
          const qrImage = await QRCode.toDataURL(row.qrcode_value);
          return { ...row, qrImage };
        } catch {
          return { ...row, qrImage: null };
        }
      })
    );
    return res.json({ items });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

export default router;
