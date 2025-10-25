import express from "express";
import multer from "multer";
import mime from "mime-types";

const router = express.Router();
const upload = multer();

// 🟢 Route universelle d’upload vers BunnyCDN
router.post("/upload", upload.single("file"), async (req, res) => {
  try {
    // ✅ Compatibilité étendue avec anciens et nouveaux champs
    const folder = req.body.folder || req.body.type || "misc";
    const userId = req.body.userId || req.body.recordId;
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
      "annonces",
      "evenements",
      "comments_audio", // ajouté pour les audios
      "comments",
      "misc",
      "groupes",
      "faits_divers",
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
    const uploadPath = `${safeFolder}/${userId ? `${userId}_` : ""}${fileName}`;

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
    const cdnUrl = `${process.env.BUNNY_CDN_URL}/${uploadPath}`;

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
