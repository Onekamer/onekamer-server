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
// 🔔 Webhook Supabase → Render → OneSignal
// --------------------------------------------------
router.post("/supabase-notification", async (req, res) => {
  try {
    const { record, type } = req.body;
    if (type !== "INSERT" || !record) {
      console.log("🟡 Ignoré (non INSERT ou record vide)");
      return res.status(200).json({ ignored: true });
    }

    const { id, user_id, sender_id, title, message, link, type: notifType } = record;
    console.log(`📨 Nouvelle notif [${notifType}] pour user ${user_id}`);

    const payload = {
      app_id: ONESIGNAL_APP_ID,
      include_external_user_ids: [user_id],
      headings: { en: title || "Notification" },
      contents: { en: message || "Nouvelle activité sur OneKamer" },
      url: `${process.env.FRONTEND_URL}${link || "/"}`,
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
      console.log(`✅ Push OneSignal envoyé à ${user_id} (${notifType})`);
    } else {
      console.error("❌ Erreur OneSignal :", data);
    }

    res.status(200).json({ success: true, details: data });
  } catch (error) {
    console.error("🔥 Erreur notif OneSignal :", error);
    res.status(500).json({ error: error.message });
  }
});

// --------------------------------------------------
// 🔒 Route de test manuelle (Postman / dev)
// --------------------------------------------------
router.post("/test-push", async (req, res) => {
  const { user_id, title, message } = req.body;

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
        headings: { en: title },
        contents: { en: message },
      }),
    });

    const data = await response.json();
    res.json(data);
  } catch (err) {
    console.error("❌ Erreur test push:", err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
