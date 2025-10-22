import express from "express";
import { createClient } from "@supabase/supabase-js";

const router = express.Router();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// 🧠 Fonction utilitaire : transformer un nom de catégorie en slug compatible avec BunnyCDN
const slugify = (str) =>
  str
    .normalize("NFD") // supprime les accents
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");

// ✅ Route automatisée pour corriger les événements sans image
router.get("/fix-evenements-images", async (req, res) => {
  try {
    // 1️⃣ Récupérer toutes les catégories d'événements
    const { data: categories, error: catError } = await supabase
      .from("evenements_categories")
      .select("id, nom");

    if (catError) throw catError;
    if (!categories?.length)
      return res.status(400).json({ error: "Aucune catégorie trouvée." });

    // 2️⃣ Construire dynamiquement le mapping entre catégorie et image
    const CDN_BASE = "https://onekamer-media-cdn.b-cdn.net/evenements/";
    const defaultImages = {};

    for (const cat of categories) {
      const slug = slugify(cat.nom);
      defaultImages[cat.nom] = `${CDN_BASE}default_evenements_${slug}.png`;
    }

    // 3️⃣ Récupérer tous les événements sans image
    const { data: evenements, error: evError } = await supabase
      .from("evenements")
      .select(`
        id,
        media_url,
        category_id,
        evenements_categories:category_id(nom)
      `)
      .or("media_url.is.null,media_url.eq.\"\"");

    if (evError) throw evError;
    if (!evenements?.length)
      return res.status(200).json({ message: "Aucun événement à corriger." });

    let updated = 0;

    // 4️⃣ Mise à jour de chaque événement sans image
    for (const event of evenements) {
      const catName = event.evenements_categories?.nom?.trim();
      if (!catName) continue;

      const defaultImage =
        defaultImages[catName] || `${CDN_BASE}default_evenements_autres.png`;

      const { error: updateError } = await supabase
        .from("evenements")
        .update({ media_url: defaultImage })
        .eq("id", event.id);

      if (!updateError) updated++;
    }

    // ✅ Retour d’un résumé clair
    res.status(200).json({
      message: `${updated} événements mis à jour avec image par défaut.`,
      categories_count: categories.length,
    });
  } catch (err) {
    console.error("Erreur fix-evenements-images:", err.message);
    res.status(500).json({ error: err.message });
  }
});

export default router;
