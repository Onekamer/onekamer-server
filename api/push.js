import express from "express";
import { createClient } from "@supabase/supabase-js";
import webpush from "web-push";

const router = express.Router();
router.use(express.json());

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const NOTIF_PROVIDER = process.env.NOTIFICATIONS_PROVIDER || "onesignal";
const VAPID_PUBLIC_KEY = process.env.VAPID_PUBLIC_KEY;
const VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY;
const VAPID_SUBJECT = process.env.VAPID_SUBJECT || "mailto:contact@onekamer.co";

try {
  if (VAPID_PUBLIC_KEY && VAPID_PRIVATE_KEY) {
    webpush.setVapidDetails(VAPID_SUBJECT, VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);
  }
} catch {}

// Envoi push direct (sans insert DB)
router.post("/push/send", async (req, res) => {
  if (NOTIF_PROVIDER !== "supabase_light") return res.status(200).json({ ignored: true });
  if (!VAPID_PUBLIC_KEY || !VAPID_PRIVATE_KEY) return res.status(200).json({ success: false, reason: "vapid_not_configured" });
  try {
    const { title, message, targetUserIds = [], data = {}, url = "/" } = req.body || {};
    if (!title || !message || !Array.isArray(targetUserIds) || targetUserIds.length === 0) {
      return res.status(400).json({ error: "title, message et targetUserIds requis" });
    }

    const { data: subs } = await supabase
      .from("push_subscriptions")
      .select("user_id, endpoint, p256dh, auth")
      .in("user_id", targetUserIds);

    const icon = "https://onekamer-media-cdn.b-cdn.net/logo/IMG_0885%202.PNG";
    const payload = (u) => JSON.stringify({ title, body: message, icon, url, data });

    let sent = 0;
    if (Array.isArray(subs)) {
      for (const s of subs) {
        try {
          await webpush.sendNotification(
            { endpoint: s.endpoint, expirationTime: null, keys: { p256dh: s.p256dh, auth: s.auth } },
            payload(s.user_id)
          );
          sent++;
        } catch (err) {
          console.error("webpush_send_error", {
            status: err?.statusCode,
            code: err?.code,
            message: err?.message,
          });
        }
      }
    }

    res.json({ success: true, sent });
  } catch (e) {
    res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Relais générique (payload déjà formé)
router.post("/push/relay", async (req, res) => {
  if (NOTIF_PROVIDER !== "supabase_light") return res.status(200).json({ ignored: true });
  if (!VAPID_PUBLIC_KEY || !VAPID_PRIVATE_KEY) return res.status(200).json({ success: false, reason: "vapid_not_configured" });
  try {
    const body = req.body || {};
    const targetUserId = body.target || body.user_id || body.targetUserId;
    const title = body.title || body.headings?.en || body.heading || "OneKamer";
    const message = body.message || body.contents?.en || body.content || "";
    const url = body.url || body.link || "/";
    const data = body.data || { type: body.type };

    if (!targetUserId) return res.status(400).json({ error: "target requis" });

    const { data: subs } = await supabase
      .from("push_subscriptions")
      .select("endpoint, p256dh, auth")
      .eq("user_id", targetUserId);

    const icon = "https://onekamer-media-cdn.b-cdn.net/logo/IMG_0885%202.PNG";
    const payload = JSON.stringify({ title, body: message, icon, url, data });

    let sent = 0;
    if (Array.isArray(subs)) {
      for (const s of subs) {
        try {
          await webpush.sendNotification(
            { endpoint: s.endpoint, expirationTime: null, keys: { p256dh: s.p256dh, auth: s.auth } },
            payload
          );
          sent++;
        } catch (err) {
          console.error("webpush_relay_error", {
            status: err?.statusCode,
            code: err?.code,
            message: err?.message,
          });
        }
      }
    }

    res.json({ success: true, sent });
  } catch (e) {
    res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// Webhook compatible triggers Supabase
router.post("/supabase-notification", async (req, res) => {
  if (NOTIF_PROVIDER !== "supabase_light") return res.status(200).json({ ignored: true });
  if (!VAPID_PUBLIC_KEY || !VAPID_PRIVATE_KEY) return res.status(200).json({ success: false, reason: "vapid_not_configured" });
  try {
    const { record, type } = req.body || {};
    let targetUserId, title, message, url, data;

    if (record) {
      targetUserId = record.user_id || record.target || record.targetUserId;
      title = record.title || "OneKamer";
      message = record.message || "";
      url = (record.link && typeof record.link === 'string') ? record.link : "/";
      data = { type: record.type || type };
    } else {
      const body = req.body || {};
      targetUserId = body.user_id || body.target || body.targetUserId;
      title = body.title || body.headings?.en || body.heading || "OneKamer";
      message = body.message || body.contents?.en || body.content || "";
      url = body.url || body.link || "/";
      data = body.data || { type: body.type };
    }

    if (!targetUserId) return res.status(400).json({ error: "target requis" });

    const { data: subs } = await supabase
      .from("push_subscriptions")
      .select("endpoint, p256dh, auth")
      .eq("user_id", targetUserId);

    const icon = "https://onekamer-media-cdn.b-cdn.net/logo/IMG_0885%202.PNG";
    const payload = JSON.stringify({ title, body: message, icon, url, data });

    let sent = 0;
    if (Array.isArray(subs)) {
      for (const s of subs) {
        try {
          await webpush.sendNotification(
            { endpoint: s.endpoint, expirationTime: null, keys: { p256dh: s.p256dh, auth: s.auth } },
            payload
          );
          sent++;
        } catch (_) {}
      }
    }

    res.json({ success: true, sent });
  } catch (e) {
    res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

export default router;

router.get("/push/health", (req, res) => {
  res.json({
    provider: NOTIF_PROVIDER,
    vapidConfigured: Boolean(VAPID_PUBLIC_KEY && VAPID_PRIVATE_KEY),
    publicKeyPrefix: VAPID_PUBLIC_KEY ? VAPID_PUBLIC_KEY.slice(0, 12) : null,
  });
});

router.get("/push/count/:userId", async (req, res) => {
  try {
    const userId = req.params.userId;
    const { data: subs, error } = await supabase
      .from("push_subscriptions")
      .select("endpoint", { count: "exact", head: true })
      .eq("user_id", userId);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ userId, count: subs ? subs.length : 0 });
  } catch (e) {
    res.status(500).json({ error: e?.message || "Erreur" });
  }
});
