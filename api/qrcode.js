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

function parseAdminAllowlist() {
  const raw = process.env.QR_ADMIN_USER_IDS || "";
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

async function verifyAdminJWT(req) {
  const authHeader = req.headers["authorization"] || "";
  const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : null;
  if (!token) return { ok: false, reason: "unauthorized" };
  const supabaseAuth = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);
  const { data: userData, error: userErr } = await supabaseAuth.auth.getUser(token);
  if (userErr || !userData?.user) return { ok: false, reason: "invalid_token" };
  const uid = userData.user.id;
  const allow = parseAdminAllowlist();
  if (!allow.includes(uid)) return { ok: false, reason: "forbidden" };
  return { ok: true, uid };
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

    await supabase
      .from("event_qrcodes")
      .update({ status: "used", validated_at: new Date().toISOString() })
      .eq("id", data.id);

    return res.json({ valid: true, message: "Entree validee", event: data.evenements || null });
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

    await supabase
      .from("event_qrcodes")
      .update({ status: "used", validated_at: new Date().toISOString() })
      .eq("id", data.id);

    return res.json({ valid: true, message: "Entree validee", event: data.evenements || null });
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
      .select("id, qrcode_value, status, created_at, validated_at, event_id, evenements:event_id(title, date, location)")
      .eq("user_id", user_id)
      .order("created_at", { ascending: false })
      .limit(50);

    if (error) return res.status(500).json({ error: error.message });

    if (!withImage) return res.json({ items: data || [] });

    const items = await Promise.all(
      (data || []).map(async (row) => {
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
