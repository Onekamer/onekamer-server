import express from "express";
import { createClient } from "@supabase/supabase-js";

const router = express.Router();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// 🖼️ Mapping entre les industries et les images par défaut sur BunnyCDN
const DEFAULT_IMAGES = {
  "Restauration": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaires_restauration.png",
  "Beauté & Bien-être": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaires_bien-etre.png",
  "Technologie": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaires_technologies.png",
  "Éducation": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaires_formations.png",
  "Commerce": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaires_mode.png",
  "Santé & Bien-être": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaires_bien-etre.png",
  "Immobilier": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaires_immobilier.png",
  "Finance & Services": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaires_finances.png",
  "Événementiel": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaires_culture_evenementiel.png",
  "Transport": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaires_transport.png",
  "Médias & Réseaux": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaires_technologies.png",
  "Public / Administratif": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaires_formations.png",
  "Divers": "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaires_mode.png",
};

// ✅ Route de correction automatique des images manquantes
router.get("/fix-partenaire-images", async (req, res) => {
  try {
    // On récupère tous les partenaires sans image
    const { data: partenaires, error } = await supabase
      .from("partenaires")
      .select(`
        id,
        media_url,
        category_id,
        partenaires_categories:category_id(industrie)
      `)
      .or("media_url.is.null,media_url.eq.\"\"");

    if (error) throw error;
    if (!partenaires?.length)
      return res.status(200).json({ message: "Aucun partenaire à corriger." });

    let updated = 0;

    for (const partenaire of partenaires) {
      const industry = partenaire.partenaires_categories?.industrie?.trim();
      if (!industry) continue;

      const defaultImage =
        DEFAULT_IMAGES[industry] ||
        "https://onekamer-media-cdn.b-cdn.net/partenaires/default_partenaire_mode.png";

      // Mise à jour de la ligne
      const { error: updateError } = await supabase
        .from("partenaires")
        .update({ media_url: defaultImage })
        .eq("id", partenaire.id);

      if (!updateError) updated++;
    }

    res.status(200).json({
      message: `${updated} partenaires mis à jour avec image par défaut.`,
    });
  } catch (err) {
    console.error("Erreur fix-partenaire-images:", err.message);
    res.status(500).json({ error: err.message });
  }
});

export default router;

