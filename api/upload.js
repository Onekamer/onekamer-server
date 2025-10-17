
import express from "express";
import multer from "multer";
import { createClient } from "@supabase/supabase-js";

const router = express.Router();
const upload = multer();

// 🧩 Connexion Supabase
const supabaseClient = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// 🗂️ Mapping des tables ↔ dossiers Bunny
const uploadMap = {
  profiles: { folder: "avatars", column: "avatar_url" },
  annonces: { folder: "annonces", column: "media_url", typeColumn: "media_type" },
  evenements: { folder: "evenements", column: "media_url", typeColumn: "media_type" },
  faits_divers: { folder: "faits_divers", column: "image_url" },
  groupes: { folder: "groupes", column: "image_url" },
  groupes_list: { folder: "groupes", column: "image_url" },
  partenaires: { folder: "partenaires", column: "media_url" },
  posts: { folder: "posts", column: "image_url", videoColumn: "video_url" },
  rencontres: { folder: "rencontres", column: "image_url" },
  comments: { folder: "comments", column: "media_url", typeColumn: "media_type" },
};

// 🟢 Route d’upload vers BunnyCDN
router.post("/upload-media", upload.single("file"), async (req, res) => {
  try {
    const { type, recordId } = req.body;
    const file = req.file;

    if (!type || !recordId || !file) {
      return res.status(400).json({ error: "Champs manquants (type, recordId, file)" });
    }

    const config = uploadMap[type];
    if (!config) return res.status(400).json({ error: "Type de table non reconnu" });

    // ✅ Nom de fichier propre (évite les 'blob')
    const originalName = file.originalname || "upload";
    const extension = originalName.split(".").pop() || "jpg";
    const fileName = `${Date.now()}_${originalName.replace(/\s+/g, "_")}`;
    const uploadPath = `${config.folder}/${fileName}`;

    // ✅ Détection du type MIME (images, gif, vidéos)
    let contentType = "application/octet-stream";
    if (file.mimetype) {
      contentType = file.mimetype;
    } else if (["jpg", "jpeg", "png", "gif", "webp"].includes(extension.toLowerCase())) {
      contentType = `image/${extension === "jpg" ? "jpeg" : extension}`;
    } else if (["mp4", "mov", "webm"].includes(extension.toLowerCase())) {
      contentType = `video/${extension}`;
    }

    // 🔼 Upload vers BunnyCDN
    const uploadRes = await fetch(
      `https://storage.bunnycdn.com/${process.env.BUNNY_STORAGE_ZONE}/${uploadPath}`,
      {
        method: "PUT",
        headers: {
          AccessKey: process.env.BUNNY_ACCESS_KEY,
          "Content-Type": contentType,
        },
        body: file.buffer,
      }
    );

    if (!uploadRes.ok) throw new Error("Erreur upload Bunny");

    // ✅ URL CDN et détection du type
    const cdnUrl = `${process.env.BUNNY_CDN_URL}/${uploadPath}`;
    const mediaType = contentType.startsWith("video") ? "video" : "image";

    // 🧾 Prépare la mise à jour Supabase
    let updateData = { [config.column]: cdnUrl };
    if (config.typeColumn) updateData[config.typeColumn] = mediaType;
    if (config.videoColumn && mediaType === "video") {
      updateData = { [config.videoColumn]: cdnUrl };
    }

    // 📦 Mise à jour Supabase
    const { error: supaError } = await supabaseClient
      .from(type)
      .update(updateData)
      .eq("id", recordId);

    if (supaError) throw supaError;

    return res.json({
      success: true,
      url: cdnUrl,
      mediaType,
      contentType,
      message: `✅ Upload réussi : ${uploadPath}`,
    });
  } catch (err) {
    console.error("❌ Erreur upload:", err);
    return res.status(500).json({ error: err.message });
  }
});

export default router;
