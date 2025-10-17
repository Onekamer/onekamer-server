import express from "express";
import { createClient } from "@supabase/supabase-js";

const router = express.Router();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// 🧩 Mapping des noms exacts de catégories Supabase vers images Bunny
const DEFAULT_IMAGES = {
  "Restauration": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaire_restauration.png",
  "Mode et beauté": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaire_mode.png",
  "Technologie et services numériques": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaire_technologies.png",
  "Santé et bien-être": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaire_bien-etre.png",
  "Éducation et formation": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaire_formations.png",
  "Immobilier et logement": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaire_immobilier.png",
  "Finance et assurance": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaire_finances.png",
  "Culture et événementiel": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaire_culture_evenementiel.png",
  "Transports et voyage": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaire_transport.png", // si tu en ajoutes une plus tard
};

// ✅ 1️⃣ Route de correction rétroactive
router.get("/fix-partenaire-images", async (req, res) => {
  try {
    // On récupère tous les partenaires sans image
    const { data: partenaires, error } = await supabase
      .from("partenaires")
      .select(`
        id,
        media_url,
        category_id,
        categories:partenaires_categories(name)
      `)
      .or("media_url.is.null,media_url.eq('')");

    if (error) throw error;
    if (!partenaires?.length) {
      return res.status(200).json({ message: "Aucun partenaire à corriger." });
    }

    let updated = 0;

    for (const partenaire of partenaires) {
      const catName = partenaire.categories?.name?.trim();
      if (!catName) continue;

      const defaultImage =
        DEFAULT_IMAGES[catName] ||
        "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaire_restauration.png";

      const { error: updateError } = await supabase
        .from("partenaires")
        .update({ media_url: defaultImage })
        .eq("id", partenaire.id);

      if (!updateError) updated++;
      else console.warn(`Erreur sur ${partenaire.id}: ${updateError.message}`);
    }

    return res.status(200).json({
      message: `${updated} partenaires mis à jour avec image par défaut.`,
    });
  } catch (err) {
    console.error("Erreur fix-partenaire-images:", err.message);
    return res.status(500).json({ error: err.message });
  }
});

// ✅ 2️⃣ Middleware auto pour les nouveaux ajouts
router.post("/apply-default-partenaire", async (req, res) => {
  try {
    const { id, category_id, media_url } = req.body;

    if (!id || !category_id)
      return res.status(400).json({ error: "id et category_id requis." });

    // on récupère le nom réel de la catégorie depuis Supabase
    const { data: category, error: catError } = await supabase
      .from("partenaires_categories")
      .select("name")
      .eq("id", category_id)
      .single();

    if (catError) throw catError;
    const catName = category?.name;
    const defaultImage =
      DEFAULT_IMAGES[catName] ||
      "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaire_restauration.png";

    // si le partenaire n’a pas déjà une image
    if (!media_url || media_url === "") {
      const { error } = await supabase
        .from("partenaires")
        .update({ media_url: defaultImage })
        .eq("id", id);

      if (error) throw error;
    }

    res.status(200).json({ message: "Image par défaut appliquée avec succès." });
  } catch (err) {
    console.error("Erreur apply-default-partenaire:", err.message);
    res.status(500).json({ error: err.message });
  }
});

export default router;
