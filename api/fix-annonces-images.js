import express from "express";
import { createClient } from "@supabase/supabase-js";

const router = express.Router();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// 🧠 Fonction utilitaire : formatage du nom en slug pour trouver l'image correspondante
const slugify = (str) =>
  str
    .normalize("NFD") // supprime les accents
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");

// ✅ Route automatisée pour appliquer les images par défaut aux annonces sans image
router.get("/fix-annonces-images", async (req, res) => {
  try {
    // 1️⃣ Récupération de toutes les catégories d'annonces
    const { data: categories, error: catError } = await supabase
      .from("annonces_categories")
      .select("id, nom");

    if (catError) throw catError;
    if (!categories?.length)
      return res.status(400).json({ error: "Aucune catégorie trouvée." });

    // 2️⃣ Construction du mapping dynamique à partir des noms de catégories
    const CDN_BASE = "https://onekamer-media-cdn.b-cdn.net/annonces/";
    const defaultImages = {};

    for (const cat of categories) {
      const slug = slugify(cat.nom);
      defaultImages[cat.nom] = `${CDN_BASE}default_annonces_${slug}.png`;
    }

    // 3️⃣ Récupération de toutes les annonces sans image
    const { data: annonces, error: annoncesError } = await supabase
      .from("annonces")
      .select(`
        id,
        media_url,
        categorie_id,
        annonces_categories:categorie_id(nom)
      `)
      .or("media_url.is.null,media_url.eq.\"\"");

    if (annoncesError) throw annoncesError;
    if (!annonces?.length)
      return res.status(200).json({ message: "Aucune annonce à corriger." });

    let updated = 0;

    // 4️⃣ Mise à jour des annonces sans image
    for (const annonce of annonces) {
      const categorieNom = annonce.annonces_categories?.nom?.trim();
      if (!categorieNom) continue;

      const defaultImage =
        defaultImages[categorieNom] ||
        `${CDN_BASE}default_annonces_autres.png`;

      const { error: updateError } = await supabase
        .from("annonces")
        .update({ media_url: defaultImage })
        .eq("id", annonce.id);

      if (!updateError) updated++;
    }

    // ✅ Résumé du traitement
    res.status(200).json({
      message: `${updated} annonces mises à jour avec images par défaut.`,
      categories_count: categories.length,
    });
  } catch (err) {
    console.error("Erreur fix-annonces-images:", err.message);
    res.status(500).json({ error: err.message });
  }
});

export default router;
