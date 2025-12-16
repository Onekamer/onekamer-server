import express from "express";
import { createClient } from "@supabase/supabase-js";

const router = express.Router();

// ✅ Initialisation paresseuse de Supabase pour éviter de faire planter le serveur
// si les variables d'environnement sont absentes ou mal configurées.
let supabase = null;

function getSupabaseClient() {
  if (!supabase) {
    const url = process.env.SUPABASE_URL;
    const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

    if (!url || !serviceKey) {
      throw new Error(
        "SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY manquant(e) pour /fix-evenements-images"
      );
    }

    supabase = createClient(url, serviceKey);
  }

  return supabase;
}

// 🧠 Fonction utilitaire : transformer un nom en slug compatible avec BunnyCDN
const slugify = (str) =>
  str
    .normalize("NFD") // supprime les accents
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");

// ✅ Route pour corriger les événements sans image
router.get("/fix-evenements-images", async (req, res) => {
  try {
    const supabaseClient = getSupabaseClient();

    // 1️⃣ Récupération de tous les types d'événements
    const { data: types, error: typesError } = await supabaseClient
      .from("evenements_types")
      .select("id, nom");

    if (typesError) throw typesError;
    if (!types?.length)
      return res.status(400).json({ error: "Aucun type d'événement trouvé." });

    // 2️⃣ Construction du mapping type → image BunnyCDN
    const CDN_BASE = "https://onekamer-media-cdn.b-cdn.net/evenements/";
    const defaultImages = {};

    for (const type of types) {
      const slug = slugify(type.nom);
      defaultImages[type.nom] = `${CDN_BASE}default_evenements_${slug}.png`;
    }

    // 3️⃣ Récupération des événements sans image
    const { data: evenements, error: evError } = await supabaseClient
      .from("evenements")
      .select(`
        id,
        media_url,
        type_id,
        evenements_types:type_id(nom)
      `)
      .or("media_url.is.null,media_url.eq.\"\"");

    if (evError) throw evError;
    if (!evenements?.length)
      return res.status(200).json({ message: "Aucun événement à corriger." });

    let updated = 0;

    // 4️⃣ Mise à jour des événements sans image
    for (const event of evenements) {
      const typeNom = event.evenements_types?.nom?.trim();
      if (!typeNom) continue;

      let defaultImage =
        defaultImages[typeNom] || `${CDN_BASE}default_evenements_autres.png`;

      // 💡 Exemple correctif si une image a un nom légèrement différent
      // (ex: "table_ronde" → "table-ronde.png")
      if (typeNom.toLowerCase().includes("table ronde")) {
        defaultImage = `${CDN_BASE}default_evenements_table_ronde.png`;
      }

      const { error: updateError } = await supabaseClient
        .from("evenements")
        .update({ media_url: defaultImage })
        .eq("id", event.id);

      if (!updateError) updated++;
    }

    res.status(200).json({
      message: `${updated} événements mis à jour avec images par défaut.`,
      types_count: types.length,
    });
  } catch (err) {
    console.error("Erreur fix-evenements-images:", err.message);
    res.status(500).json({ error: err.message });
  }
});

export default router;
