import express from "express";
import { createClient } from "@supabase/supabase-js";
import webpush from "web-push";

const router = express.Router();
router.use(express.json());

// ✅ Initialisation paresseuse de Supabase pour éviter de faire planter le serveur
// si les variables d'environnement sont absentes ou mal configurées.
let supabase = null;

function getSupabaseClient() {
  if (!supabase) {
    const url = process.env.SUPABASE_URL;
    const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

    if (!url || !serviceKey) {
      throw new Error("SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY manquant(e) pour /push");
    }

    supabase = createClient(url, serviceKey);
  }

  return supabase;
}

const NOTIF_PROVIDER = process.env.NOTIFICATIONS_PROVIDER || "onesignal";
const VAPID_PUBLIC_KEY = process.env.VAPID_PUBLIC_KEY;
const VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY;
const VAPID_SUBJECT = process.env.VAPID_SUBJECT || "mailto:contact@onekamer.co";

try {
  if (VAPID_PUBLIC_KEY && VAPID_PRIVATE_KEY) {
    webpush.setVapidDetails(VAPID_SUBJECT, VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);
  }
} catch {}

// Envoi push direct (et enregistrement dans public.notifications)
router.post("/push/send", async (req, res) => {
  if (NOTIF_PROVIDER !== "supabase_light") return res.status(200).json({ ignored: true });
  if (!VAPID_PUBLIC_KEY || !VAPID_PRIVATE_KEY)
    return res.status(200).json({ success: false, reason: "vapid_not_configured" });
  try {
    const supabaseClient = getSupabaseClient();
    const { title, message, targetUserIds = [], data = {}, url = "/" } = req.body || {};
    if (!title || !message || !Array.isArray(targetUserIds) || targetUserIds.length === 0) {
      return res.status(400).json({ error: "title, message et targetUserIds requis" });
    }

    const { data: subs } = await supabaseClient
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

    // Enregistre une entrée dans public.notifications pour chaque utilisateur ciblé
    try {
      const uniqueUserIds = Array.isArray(targetUserIds)
        ? Array.from(new Set(targetUserIds.filter(Boolean)))
        : [];

      if (uniqueUserIds.length > 0) {
        const notifType = (data && (data.type || data.notificationType)) || "systeme";
        const contentId = data && (data.contentId || data.content_id) ? data.contentId || data.content_id : null;
        const senderId = data && (data.senderId || data.sender_id) ? data.senderId || data.sender_id : null;

        for (const userId of uniqueUserIds) {
          try {
            await supabaseClient.rpc("create_notification", {
              p_user_id: userId,
              p_sender_id: senderId,
              p_type: notifType,
              p_content_id: contentId,
              p_title: title,
              p_message: message,
              p_link: url || "/",
            });
          } catch (err) {
            console.error("notification_persist_error", {
              user_id: userId,
              message: err?.message,
            });
          }
        }
      }
    } catch (err) {
      console.error("notification_persist_wrapper_error", {
        message: err?.message,
      });
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
    const supabaseClient = getSupabaseClient();
    const body = req.body || {};
    const targetUserId = body.target || body.user_id || body.targetUserId;
    const title = body.title || body.headings?.en || body.heading || "OneKamer";
    const message = body.message || body.contents?.en || body.content || "";
    const url = body.url || body.link || "/";
    const data = body.data || { type: body.type };

    if (!targetUserId) return res.status(400).json({ error: "target requis" });

    const { data: subs } = await supabaseClient
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
    const supabaseClient = getSupabaseClient();
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

    const { data: subs } = await supabaseClient
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
          console.error("webhook_push_error", {
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

export default router;

