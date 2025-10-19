import express from "express";
import { supabase } from "../lib/supabaseClient.js"; // adapte le chemin si besoin
import { detectAndSaveMentions } from "../utils/mentions.js";

const router = express.Router();

router.post("/add-comment", async (req, res) => {
  try {
    const { content, user_id, post_id } = req.body;

    // ✅ 1. Ajoute le commentaire dans Supabase
    const { data: comment, error } = await supabase
      .from("comments")
      .insert({
        content,
        user_id,
        content_id: post_id,
        content_type: "post",
      })
      .select()
      .single();

    if (error) {
      console.error("Erreur ajout commentaire:", error);
      return res.status(400).json({ error });
    }

    // ✅ 2. Détecte et enregistre les mentions
    await detectAndSaveMentions({
      text: content,
      senderId: user_id,
      contentId: comment.id,
      contentType: "comment",
      supabase,
    });

    res.json({ success: true, comment });
  } catch (err) {
    console.error("Erreur serveur:", err);
    res.status(500).json({ error: "Erreur interne du serveur" });
  }
});

export default router;
