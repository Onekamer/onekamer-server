import express from "express";
import multer from "multer";
import mime from "mime-types";
import { createClient } from "@supabase/supabase-js";

const router = express.Router();
const upload = multer();

// ✅ Initialisation paresseuse de Supabase (pour synchroniser les fichiers "rencontres")
// On évite de faire planter tout le serveur au démarrage si les variables d'env sont absentes.
let supabase = null;

function getSupabaseClient() {
  if (!supabase) {
    const url = process.env.SUPABASE_URL;
    const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

    if (!url || !serviceKey) {
      throw new Error(
        "SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY manquant(e) pour la synchronisation des fichiers 'rencontres'"
      );
    }

    supabase = createClient(url, serviceKey);
  }

  return supabase;
}

// 🟢 Route universelle d’upload vers BunnyCDN
router.post("/upload", upload.single("file"), async (req, res) => {
  try {
    // ✅ Compatibilité étendue avec anciens et nouveaux champs
    const folder = req.body.folder || req.body.type || "misc";
    const userId = req.body.user_id || req.body.userId || req.body.recordId;
    const file = req.file;

    // 🧩 Vérification basique
    if (!file) {
      return res.status(400).json({ error: "Aucun fichier reçu." });
    }

    // ✅ Whitelist des dossiers autorisés
    const allowedFolders = [
      "avatars",
      "posts",
      "partenaires",
      "marketplace_items",
      "annonces",
      "evenements",
      "comments_audio", // ajouté pour les audios
      "comments",
      "misc",
      "groupes",
      "faits_divers",
      "rencontres",
    ];
    if (!allowedFolders.includes(folder)) {
      return res.status(400).json({ error: `Dossier non autorisé: ${folder}` });
    }

    // ✅ Types MIME autorisés
    const ALLOWED_AUDIO_TYPES = [
      "audio/webm",
      "audio/mpeg",
      "audio/mp4",
      "audio/ogg",
      "audio/wav",
      "audio/x-m4a",
      "audio/x-aac",
    ];

    // 🧠 Détection propre du mimetype + extension
    const mimeType = file.mimetype || "application/octet-stream";
    const ext = mime.extension(mimeType) || "bin";

    // 🛑 Vérification du type de fichier
    const isImage = mimeType.startsWith("image/");
    const isVideo = mimeType.startsWith("video/");
    const isAudio = ALLOWED_AUDIO_TYPES.includes(mimeType);

    if (!isImage && !isVideo && !isAudio) {
      return res.status(400).json({
        success: false,
        message: `Type de fichier non pris en charge (${mimeType}).`,
      });
    }

    // 🔧 Nom de fichier sûr et unique
    const originalName =
      file.originalname?.replace(/\s+/g, "_") || `upload.${ext}`;
    const fileName = `${Date.now()}_${originalName}`;
    const safeFolder = allowedFolders.includes(folder) ? folder : "misc";

    // ✅ Organisation: pour "rencontres", on crée un sous-dossier par utilisateur (comme LAB)
    let uploadPath;
    if (safeFolder === "rencontres" && userId) {
      uploadPath = `${safeFolder}/${userId}/${fileName}`;
    } else {
      uploadPath = `${safeFolder}/${fileName}`;
    }

    console.log("📁 Upload vers:", uploadPath, "| Type:", mimeType);

    // 🚀 Upload vers Bunny Storage
    const response = await fetch(
      `https://storage.bunnycdn.com/${process.env.BUNNY_STORAGE_ZONE}/${uploadPath}`,
      {
        method: "PUT",
        headers: {
          AccessKey: process.env.BUNNY_ACCESS_KEY,
          "Content-Type": mimeType,
        },
        body: file.buffer,
      }
    );

    if (!response.ok) {
      const errorText = await response.text();
      console.error("❌ Erreur BunnyCDN:", errorText);
      throw new Error(`Échec de l’upload sur BunnyCDN (${response.status})`);
    }

    // 🌍 URL finale (CDN public)
    let cdnUrl = `${process.env.BUNNY_CDN_URL}/${uploadPath}`;

    // 🪄 Synchronisation dans Supabase pour permettre les URLs signées côté front
    if (safeFolder === "rencontres") {
      try {
        const supabaseClient = getSupabaseClient();
        const { error: supabaseError } = await supabaseClient.storage
          .from("rencontres")
          .upload(uploadPath, file.buffer, {
            contentType: mimeType,
            upsert: true,
          });
        if (supabaseError) {
          console.warn("⚠️ Upload Bunny réussi, mais échec Supabase :", supabaseError.message);
        }
      } catch (syncErr) {
        console.warn("⚠️ Erreur de synchronisation Supabase :", syncErr.message);
      }
    }

    // ✅ Succès
    return res.status(200).json({
      success: true,
      url: cdnUrl,
      path: uploadPath,
      mimeType,
      message: `✅ Upload réussi vers ${cdnUrl}`,
    });
  } catch (err) {
    console.error("❌ Erreur upload:", err.message);
    return res.status(500).json({
      success: false,
      error: err.message,
      hint: "Vérifie ta clé BunnyCDN, ton dossier autorisé, et le Content-Type.",
    });
  }
});

export default router;
