import express from "express";
import { createClient } from "@supabase/supabase-js";

const router = express.Router();
router.use(express.json());

const fetch = globalThis.fetch;

// ⚙️ Connexion Supabase
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// ⚙️ Variables OneSignal
const ONESIGNAL_APP_ID = process.env.ONESIGNAL_APP_ID;
const ONESIGNAL_API_KEY = process.env.ONESIGNAL_API_KEY;

// --------------------------------------------------
// 🔔 1️⃣ ROUTE DIRECTE OneSignal (ancienne, stable)
// --------------------------------------------------
router.post("/notifications/onesignal", async (req, res) => {
  try {
    const { title, message, target, url } = req.body;

    if (!target) {
      return res.status(400).json({ error: "Target (player_id) requis" });
    }

    const payload = {
      app_id: ONESIGNAL_APP_ID,
      include_player_ids: [target],
      headings: { en: title || "Notification OneKamer" },
      contents: { en: message || "Nouveau message reçu sur OneKamer" },
      url: url || "https://onekamer.co",
    };

    const response = await fetch("https://onesignal.com/api/v1/notifications", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Basic ${ONESIGNAL_API_KEY}`,
      },
      body: JSON.stringify(payload),
    });

    const data = await response.json();

    if (response.ok) {
      console.log(`✅ OneSignal envoyé avec succès à ${target}`);
      return res.status(200).json({ success: true, notification_id: data.id });
    } else {
      console.error("❌ Erreur OneSignal :", data);
      return res.status(500).json({ error: data });
    }
  } catch (error) {
    console.error("🔥 Erreur /notifications/onesignal :", error);
    res.status(500).json({ error: error.message });
  }
});

// --------------------------------------------------
// 🔔 2️⃣ ROUTE AUTOMATIQUE SUPABASE → ONESIGNAL
// --------------------------------------------------
router.post("/api/supabase-notification", async (req, res) => {
  try {
    const { record, type } = req.body;

    if (type !== "INSERT" || !record) {
      console.log("🟡 Ignoré (pas un INSERT ou record vide)");
      return res.status(200).json({ ignored: true });
    }

    const { id, user_id, sender_id, title, message, link, type: notifType } = record;
    console.log(`📨 Nouvelle notif Supabase → OneSignal [${notifType}] pour user ${user_id}`);

    const payload = {
      app_id: ONESIGNAL_APP_ID,
      include_external_user_ids: [user_id],
      headings: { en: title || "Notification" },
      contents: { en: message || "Nouvelle activité sur OneKamer" },
      url: `https://onekamer.co${link || "/"}`,
      data: { notif_id: id, sender_id, type: notifType },
    };

    const response = await fetch("https://onesignal.com/api/v1/notifications", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Basic ${ONESIGNAL_API_KEY}`,
      },
      body: JSON.stringify(payload),
    });

    const data = await response.json();
    if (response.ok) {
      console.log(`✅ Notification OneSignal envoyée à ${user_id} (${notifType})`);
      res.status(200).json({ success: true, details: data });
    } else {
      console.error("❌ Erreur OneSignal :", data);
      res.status(500).json({ error: data });
    }
  } catch (error) {
    console.error("🔥 Erreur webhook Supabase → OneSignal :", error);
    res.status(500).json({ error: error.message });
  }
});

// --------------------------------------------------
// 🔔 3️⃣ ROUTE DE TEST MANUELLE (Postman / terminal)
// --------------------------------------------------
router.post("/api/test-push", async (req, res) => {
  const { user_id, title, message, link } = req.body;

  if (!user_id) return res.status(400).json({ error: "user_id requis" });

  try {
    const response = await fetch("https://onesignal.com/api/v1/notifications", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Basic ${ONESIGNAL_API_KEY}`,
      },
      body: JSON.stringify({
        app_id: ONESIGNAL_APP_ID,
        include_external_user_ids: [user_id],
        headings: { en: title || "🔔 Notification OneKamer" },
        contents: { en: message || "Nouvelle activité détectée" },
        url: `https://onekamer.co${link || "/"}`,
      }),
    });

    const data = await response.json();
    res.json(data);
  } catch (err) {
    console.error("❌ Erreur /api/test-push:", err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
