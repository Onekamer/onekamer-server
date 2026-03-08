import express from "express";
import multer from "multer";
import mime from "mime-types";
import { createClient } from "@supabase/supabase-js";
import fs from "fs";
import Busboy from "busboy";

const router = express.Router();
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, "/tmp"),
  filename: (req, file, cb) => {
    const ext = mime.extension(file.mimetype || "application/octet-stream") || "bin";
    const originalName = (file.originalname?.replace(/\s+/g, "_") || `upload.${ext}`);
    cb(null, `${Date.now()}_${originalName}`);
  },
});
const upload = multer({ storage });

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
router.post("/upload", async (req, res) => {
  try {
    if (!req.headers["content-type"] || !/multipart\/form-data/i.test(req.headers["content-type"])) {
      return res.status(400).json({ success: false, error: "Content-Type invalide (multipart/form-data requis)" });
    }

    const allowedFolders = [
      "avatars",
      "posts",
      "partenaires",
      "marketplace_items",
      "annonces",
      "evenements",
      "comments_audio",
      "comments",
      "misc",
      "groupes",
      "faits_divers",
      "rencontres",
    ];
    const ALLOWED_AUDIO_TYPES = [
      "audio/webm",
      "audio/mpeg",
      "audio/mp4",
      "audio/ogg",
      "audio/wav",
      "audio/x-m4a",
      "audio/x-aac",
    ];

    let folder = "misc";
    let userId = null;
    let fileHandled = false;
    let uploadPath = null;
    let mimeType = null;
    let cdnUrl = null;
    let safeFolder = null;

    const busboy = Busboy({ headers: req.headers, limits: { fields: 32 } });

    const done = new Promise((resolve, reject) => {
      busboy.on("field", (name, val) => {
        if (name === "folder" || name === "type") folder = String(val || "").trim() || "misc";
        if (name === "user_id" || name === "userId" || name === "recordId") userId = String(val || "").trim();
      });

      busboy.on("file", (name, file, info) => {
        if (fileHandled) { file.resume(); return; }
        fileHandled = true;
        const { filename, mimeType: mm, mime: legacyMime, encoding } = info || {};
        mimeType = mm || legacyMime || "application/octet-stream";

        const isImage = /^image\//i.test(mimeType);
        const isVideo = /^video\//i.test(mimeType);
        const isAudio = ALLOWED_AUDIO_TYPES.includes(mimeType);
        if (!isImage && !isVideo && !isAudio) {
          file.resume();
          return reject(new Error(`Type de fichier non pris en charge (${mimeType}).`));
        }

        const ext = mime.extension(mimeType) || "bin";
        const originalName = (filename || `upload.${ext}`).replace(/\s+/g, "_");
        const fileName = `${Date.now()}_${originalName}`;
        safeFolder = allowedFolders.includes(folder) ? folder : "misc";
        uploadPath = (safeFolder === "rencontres" && userId) ? `${safeFolder}/${userId}/${fileName}` : `${safeFolder}/${fileName}`;

        console.log("📁 Upload (pass-through) vers:", uploadPath, "| Type:", mimeType, "| Enc:", encoding || "-");

        const bunnyUrl = `https://storage.bunnycdn.com/${process.env.BUNNY_STORAGE_ZONE}/${uploadPath}`;
        const headers = {
          AccessKey: process.env.BUNNY_ACCESS_KEY,
          "Content-Type": mimeType,
        };

        // Pass-through: on streame directement le flux entrant vers Bunny
        fetch(bunnyUrl, { method: "PUT", headers, body: file, duplex: "half" })
          .then(async (resp) => {
            if (!resp.ok) {
              const t = await resp.text().catch(() => "");
              throw new Error(`Échec de l’upload sur BunnyCDN (${resp.status}): ${t}`);
            }
            cdnUrl = `${process.env.BUNNY_CDN_URL}/${uploadPath}`;
            resolve();
          })
          .catch((err) => reject(err));
      });

      busboy.on("error", (e) => reject(e));
      busboy.on("finish", () => { if (!fileHandled) reject(new Error("Aucun fichier reçu.")); });
    });

    req.pipe(busboy);
    await done;

    // Optionnel: synchro Supabase uniquement pour rencontres et images (évite un second transfert lourd)
    if (safeFolder === "rencontres" && mimeType && /^image\//i.test(mimeType)) {
      try {
        const supabaseClient = getSupabaseClient();
        const r = await fetch(cdnUrl);
        const ab = await r.arrayBuffer();
        const { error: supabaseError } = await supabaseClient.storage
          .from("rencontres")
          .upload(uploadPath, new Uint8Array(ab), { contentType: mimeType, upsert: true });
        if (supabaseError) console.warn("⚠️ Upload Bunny réussi, mais échec Supabase :", supabaseError.message);
      } catch (syncErr) {
        console.warn("⚠️ Erreur de synchronisation Supabase :", syncErr.message);
      }
    }

    return res.status(200).json({ success: true, url: cdnUrl, path: uploadPath, mimeType, message: `✅ Upload réussi vers ${cdnUrl}` });
  } catch (err) {
    console.error("❌ Erreur upload:", err.message);
    return res.status(500).json({ success: false, error: err.message, hint: "Vérifie ta clé BunnyCDN, ton dossier autorisé, et le Content-Type." });
  }
});

export default router;
