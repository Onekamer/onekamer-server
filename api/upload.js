import express from "express";
import multer from "multer";

const router = express.Router();
const upload = multer();

// 🟢 Nouvelle route simplifiée : upload direct vers BunnyCDN
router.post("/upload", upload.single("file"), async (req, res) => {
  try {
    const { folder = "posts" } = req.body;
    const file = req.file;

    if (!file) {
      return res.status(400).json({ error: "Aucun fichier reçu." });
    }

    // 🔧 Nom de fichier sécurisé
    const originalName = file.originalname || "upload";
    const safeName = originalName.replace(/\s+/g, "_");
    const fileName = `${Date.now()}_${safeName}`;
    const uploadPath = `${folder}/${fileName}`;

    // 🔍 Détection du type MIME
    const contentType =
      file.mimetype || "application/octet-stream";

    // 🚀 Upload vers Bunny Storage
    const response = await fetch(
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

    if (!response.ok) {
      throw new Error("Erreur lors de l’upload vers BunnyCDN");
    }

    // 🌍 URL CDN finale
    const cdnUrl = `${process.env.BUNNY_CDN_URL}/${uploadPath}`;

    // ✅ Retourne simplement l’URL au front
    return res.json({
      success: true,
      url: cdnUrl,
      contentType,
      message: `✅ Upload réussi : ${uploadPath}`,
    });
  } catch (err) {
    console.error("❌ Erreur upload:", err);
    return res.status(500).json({ error: err.message });
  }
});

export default router;

