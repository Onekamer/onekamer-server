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

// ============================================================
// 🔧 Fonction utilitaire : récupère une image (Supabase + BunnyCDN)
// ============================================================
async function getImageForNotification(type, contentId) {
  try {
    if (!type || !contentId) return null;

    let table = null;
    switch (type) {
      case "annonce":
        table = "annonces";
        break;
      case "evenement":
        table = "evenements";
        break;
      case "partenaire":
        table = "partenaires";
        break;
      case "faitdivers":
        table = "faits_divers";
        break;
      default:
        return null;
    }

    const { data, error } = await supabase
      .from(table)
      .select("media_url, image_url")
      .eq("id", contentId)
      .maybeSingle();

    if (error) {
      console.warn(`⚠️ Erreur récupération image pour ${type}:`, error.message);
      return null;
    }

    let url = data?.media_url || data?.image_url || null;

    // ✅ Si lien relatif → complète automatiquement avec BunnyCDN
    if (url && !url.startsWith("http")) {
      url = `https://onekamer.b-cdn.net/${url.replace(/^\/+/, "")}`;
    }

    // ✅ Fallback si aucune image trouvée
    if (!url) {
      const defaults = {
        annonce: "https://onekamer.b-cdn.net/defaults/default_annonce.jpg",
        evenement: "https://onekamer.b-cdn.net/defaults/default_evenement.jpg",
        partenaire: "https://onekamer.b-cdn.net/defaults/default_partenaire.jpg",
        faitdivers: "https://onekamer.b-cdn.net/defaults/default_faitdivers.jpg",
      };
      url = defaults[type] || "https://onekamer.b-cdn.net/defaults/default_generic.jpg";
    }

    return url;
  } catch (e) {
    console.warn("⚠️ Erreur getImageForNotification:", e.message);
    return null;
  }
}

// --------------------------------------------------
// 🔔 1️⃣ ROUTE DIRECTE OneSignal (stable & manuelle)
// --------------------------------------------------
router.post("/notifications/onesignal", async (req, res) => {
  try {
    const { title, message, target, url, image } = req.body;

    if (!target) {
      return res.status(400).json({ error: "Target (player_id) requis" });
    }

    const payload = {
      app_id: ONESIGNAL_APP_ID,
      include_player_ids: [target],
      headings: { en: title || "Notification OneKamer" },
      contents: { en: message || "Nouveau message reçu sur OneKamer" },
      url: url || "https://onekamer.co",
      big_picture: image || undefined,
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
router.post("/supabase-notification", async (req, res) => {
  try {
    const { record, type } = req.body;

    if (type !== "INSERT" || !record) {
      console.log("🟡 Ignoré (pas un INSERT ou record vide)");
      return res.status(200).json({ ignored: true });
    }

    const {
      id,
      user_id,
      sender_id,
      title,
      message,
      link,
      type: notifType,
      content_id,
    } = record;

    console.log(`📨 Nouvelle notif Supabase → OneSignal [${notifType}] pour user ${user_id}`);

    // 🖼️ Récupère l’image dynamique depuis Supabase/BunnyCDN
    const imageUrl = await getImageForNotification(notifType, content_id);
    console.log("🧩 Notification image URL:", imageUrl);

    const payload = {
      app_id: ONESIGNAL_APP_ID,
      include_external_user_ids: [user_id],
      headings: { en: title || "Notification" },
      contents: { en: message || "Nouvelle activité sur OneKamer" },
      url: `https://onekamer.co${link || "/"}`,
      big_picture: imageUrl || undefined,
      data: { notif_id: id, sender_id, type: notifType, image: imageUrl },
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
router.post("/test-push", async (req, res) => {
  const { user_id, title, message, link, image } = req.body;

  if (!user_id) return res.status(400).json({ error: "user_id requis" });

  try {
    console.log("🧩 Test push image URL:", image);

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
        big_picture: image || undefined,
      }),
    });

    const data = await response.json();
    res.json(data);
  } catch (err) {
    console.error("❌ Erreur /test-push:", err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
