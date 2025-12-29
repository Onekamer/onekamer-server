import express from "express";
import { createClient } from "@supabase/supabase-js";
import webpush from "web-push";
import apn from "apn";

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

// ======================
// 🍎 APNs (iOS) config
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

  // Render peut stocker la clé avec des \n, on corrige au cas où
  const key = APNS_PRIVATE_KEY.includes("\\n")
    ? APNS_PRIVATE_KEY.replace(/\\n/g, "\n")
    : APNS_PRIVATE_KEY;

  apnProvider = new apn.Provider({
    token: {
      key,
      keyId: APNS_KEY_ID,
      teamId: APNS_TEAM_ID,
    },
    production: APNS_ENV === "production",
  });

  return apnProvider;
}

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

// ======================
// 🍎 Send iOS push (APNs)
// ======================
router.post("/push/send-ios", async (req, res) => {
  if (NOTIF_PROVIDER !== "supabase_light") return res.status(200).json({ ignored: true });

  try {
      if (!APNS_BUNDLE_ID) {
      return res.status(500).json({ error: "missing_apns_bundle_id" });
    }
    const provider = getApnProvider();
    if (!provider) return res.status(200).json({ success: false, reason: "apns_not_configured" });

    const supabaseClient = getSupabaseClient();
    const { title, message, targetUserIds = [], data = {}, url = "/" } = req.body || {};

    if (!title || !message || !Array.isArray(targetUserIds) || targetUserIds.length === 0) {
      return res.status(400).json({ error: "title, message et targetUserIds requis" });
    }

    const { data: rows, error } = await supabaseClient
     .from("device_push_tokens")
     .select("user_id, token, platform, enabled, provider")
     .in("user_id", targetUserIds)
     .eq("platform", "ios")
     .eq("provider", "apns")
     .eq("enabled", true);

    if (error) return res.status(500).json({ error: error.message });

  if (!rows || rows.length === 0) {
  return res.json({
    success: true,
    sent: 0,
    failed: 0,
    total: 0,
    reason: "no_ios_tokens",
  });
}

let sent = 0;
let failed = 0;

for (const t of rows) {
  
  const note = new apn.Notification();
  note.topic = APNS_BUNDLE_ID;
  note.alert = { title, body: message };
  note.sound = "default";
  note.pushType = "alert";
  note.priority = 10;
  note.payload = { data, url };

  try {
    const result = await provider.send(note, t.token);
    sent += result.sent?.length || 0;
    failed += result.failed?.length || 0;
  } catch (e) {
    console.error("apns_send_error", {
      user_id: t.user_id,
      token: (t.token || "").slice(0, 10) + "...",
      message: e?.message,
    });
    failed++;
  }
}

return res.json({ success: true, sent, failed, total: rows.length });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

// ======================
// Register token
// ======================

router.post("/push/register-device", async (req, res) => {
  if (NOTIF_PROVIDER !== "supabase_light") return res.status(200).json({ ignored: true });

  try {
    const supabaseClient = getSupabaseClient();

    const body = req.body || {};
    console.log("[register-device] body=", body);
    console.log("[register-device] platform=", body.platform, "os=", body.os);
    console.log("[register-device] user_id=", body.user_id, "device_token?", !!body.device_token, "token?", !!body.token);
    
    const rawPlatform = body.platform || body.os || "";
    const normalizedPlatform = String(rawPlatform).toLowerCase().trim();
    
    const androidUserId = body.userId || body.user_id || body.uid || null;
    const androidToken = body.token || body.device_token || body.deviceToken || null;
    const androidPlatform = body.platform || body.os || null;
    const androidDeviceId = body.deviceId || body.device_id || null;
    const androidProvider = body.provider || "fcm";

    const {
      user_id,
      device_token,
      device_id = null,
      device_model = null,
      os_version = null,
      app_version = null,
    } = req.body || {};

    console.log("[register-device] VERSION=2025-12-29-01");

     // ======================
    // 🍎 iOS (début)
    // ======================
  if (normalizedPlatform === "ios"){
    console.log("[register-device][IOS] ENTER");
    console.log("[register-device][IOS] normalizedPlatform =", normalizedPlatform);
    console.log("[register-device][IOS] user_id =", user_id);
    if (!user_id || !device_token) {
      return res.status(400).json({ error: "user_id et device_token requis" });
    }
    console.log(
  "[register-device][IOS] device_token prefix =",
  String(device_token || "").slice(0, 12)
);

    const now = new Date().toISOString();

    const { data: prof, error: profErr } = await supabaseClient
     .from("profiles")
     .select("username, email")
     .eq("id", user_id)
     .maybeSingle();

  if (profErr) return res.status(500).json({ error: "Erreur lecture profil" });

    const { error } = await supabaseClient
      .from("device_push_tokens")
      .upsert(
        [{
          user_id,
          username: prof?.username || null,
          email: prof?.email || null,
          platform: "ios",
          provider: "apns",
          token: String(device_token),
          device_id: device_id ? String(device_id) : null,
          enabled: true,
          last_seen_at: now,
          updated_at: now,
        }],
        { onConflict: "token" }
      );

    if (error) return res.status(500).json({ error: error.message });

    return res.json({ success: true, version: "2025-12-29-01", branch: "ios", provider: "apns", normalizedPlatform });
    }
    // ======================
    // 🍎 iOS (fin)
    // ======================

    // ======================
    // 🤖 ANDROID (début)
    // ======================

    if (androidUserId && androidToken && normalizedPlatform !== "ios") {
      const { data: prof, error: profErr } = await supabaseClient
        .from("profiles")
        .select("username, email")
        .eq("id", androidUserId)
        .maybeSingle();

      if (profErr) return res.status(500).json({ error: "Erreur lecture profil" });

      const now = new Date().toISOString();
      const { error } = await supabaseClient
        .from("device_push_tokens")
        .upsert(
          [{
            user_id: androidUserId,
            username: prof?.username || null,
            email: prof?.email || null,
            platform: String(androidPlatform),
            provider: String(androidProvider || "fcm"),
            token: String(androidToken),
            device_id: androidDeviceId ? String(androidDeviceId) : null,
            enabled: true,
            last_seen_at: now,
            updated_at: now,
          }],
          { onConflict: "token" }
        );

      if (error) return res.status(500).json({ error: error.message });

      return res.json({ success: true });
    }
    // ======================
    // 🤖 ANDROID (fin)
    // ======================

    return res.status(400).json({ error: "platform invalide (ios / android)" });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "Erreur interne" });
  }
});

export default router;
