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

// ✅ Route automatisée de correction des images manquantes
router.get("/fix-partenaire-images", async (req, res) => {
  try {
    // 1️⃣ Récupération de toutes les catégories pour construire dynamiquement le mapping
    const { data: categories, error: catError } = await supabase
      .from("partenaires_categories")
      .select("id, nom");

    if (catError) throw catError;
    if (!categories?.length)
      return res.status(400).json({ error: "Aucune catégorie trouvée." });

    // 2️⃣ Construction du mapping dynamique à partir des noms de catégories
    const CDN_BASE = "https://onekamer-media-cdn.b-cdn.net/partenaires/";
    const defaultImages = {};

    for (const cat of categories) {
      const slug = slugify(cat.nom);
      defaultImages[cat.nom] = `${CDN_BASE}default_partenaires_${slug}.png`;
    }

    // 3️⃣ Récupération de tous les partenaires sans image
    const { data: partenaires, error: partenairesError } = await supabase
      .from("partenaires")
      .select(`
        id,
        media_url,
        category_id,
        partenaires_categories:category_id(nom)
      `)
      .or("media_url.is.null,media_url.eq.\"\"");

    if (partenairesError) throw partenairesError;
    if (!partenaires?.length)
      return res.status(200).json({ message: "Aucun partenaire à corriger." });

    let updated = 0;

    // 4️⃣ Mise à jour des partenaires sans image
    for (const partenaire of partenaires) {
      const categorieNom = partenaire.partenaires_categories?.nom?.trim();
      if (!categorieNom) continue;

      const defaultImage =
        defaultImages[categorieNom] ||
        `${CDN_BASE}default_partenaires_autres.png`;

      const { error: updateError } = await supabase
        .from("partenaires")
        .update({ media_url: defaultImage })
        .eq("id", partenaire.id);

      if (!updateError) updated++;
    }

    // ✅ Résumé
    res.status(200).json({
      message: `${updated} partenaires mis à jour avec images par défaut.`,
      categories_count: categories.length,
    });
  } catch (err) {
    console.error("Erreur fix-partenaire-images:", err.message);
    res.status(500).json({ error: err.message });
  }
});

export default router;


