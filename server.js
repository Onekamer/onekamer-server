// Charger les variables d'environnement depuis .env.local
import * as dotenv from "dotenv";
dotenv.config({ path: ".env.local" });

// Importer les modules nécessaires
import express from "express";
import Stripe from "stripe";
import bodyParser from "body-parser";
import cors from "cors";
import { createClient } from "@supabase/supabase-js";

// Initialiser Express
const app = express();

// Autoriser ton front Horizon / OneKamer à appeler l’API
app.use(
  cors({
    origin: [process.env.FRONTEND_URL || "https://onekamer.co"],
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);

// Initialiser Stripe avec la clé secrète
const stripe = new Stripe(process.env.STRIPE_SECRET_KEY, {
  apiVersion: "2024-06-20",
});

// Initialiser le client Supabase
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// Middleware pour traiter les requêtes Stripe (corps brut)
app.post("/webhook", bodyParser.raw({ type: "application/json" }), async (req, res) => {
  const sig = req.headers["stripe-signature"];
  const endpointSecret = process.env.STRIPE_WEBHOOK_SECRET;
  let event;

  try {
    event = stripe.webhooks.constructEvent(req.body, sig, endpointSecret);
  } catch (err) {
    console.error("❌ Webhook verification failed:", err.message);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  console.log("📦 Événement Stripe reçu :", event.type);

  try {
    if (event.type === "checkout.session.completed") {
      const session = event.data.object;
      const { userId, packId } = session.metadata || {};

      if (!userId || !packId) {
        console.warn("⚠️ Session Stripe sans metadata userId/packId");
        return res.json({ received: true });
      }

      // Vérifie si l’événement n’a pas déjà été traité
      const { error: evtErr } = await supabase
        .from("stripe_events")
        .insert({ event_id: event.id });
      if (evtErr && evtErr.code === "23505") {
        console.log("🔁 Événement déjà traité :", event.id);
        return res.json({ received: true });
      }

      // Appelle la fonction Supabase pour créditer les OK COINS
      const { data, error } = await supabase.rpc("okc_grant_pack_after_payment", {
        p_user: userId,
        p_pack_id: parseInt(packId, 10),
        p_status: "paid",
      });

      if (error) {
        console.error("❌ Erreur RPC Supabase :", error);
      } else {
        console.log("✅ OK COINS crédités avec succès :", data);
      }
    }

    res.json({ received: true });
  } catch (err) {
    console.error("❌ Erreur interne Webhook :", err);
    res.status(500).send("Erreur serveur interne");
  }
});

// Middleware JSON pour les autres routes
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// Créer une session Stripe Checkout
app.post("/create-checkout-session", async (req, res) => {
  try {
    const { packId, userId } = req.body;

    if (!packId || !userId) {
      return res.status(400).json({ error: "packId et userId sont requis" });
    }

    // Récupère les infos du pack dans Supabase
    const { data: pack, error: packErr } = await supabase
      .from("okcoins_packs")
      .select("pack_name, price_eur, is_active")
      .eq("id", packId)
      .single();

    if (packErr || !pack || !pack.is_active) {
      return res.status(404).json({ error: "Pack introuvable ou inactif" });
    }

    const session = await stripe.checkout.sessions.create({
      mode: "payment",
      payment_method_types: ["card"],
      line_items: [
        {
          price_data: {
            currency: "eur",
            product_data: { name: pack.pack_name },
            unit_amount: Math.round(Number(pack.price_eur) * 100),
          },
          quantity: 1,
        },
      ],
      success_url: `${process.env.FRONTEND_URL}/paiement-success?packId=${packId}`,
      cancel_url: `${process.env.FRONTEND_URL}/paiement-annule`,
      metadata: { userId, packId: String(packId) },
    });

    res.json({ url: session.url });
  } catch (err) {
    console.error("❌ Erreur création session Stripe :", err);
    res.status(500).json({ error: "Erreur serveur interne" });
  }
});

// Route racine (Render health check)
app.get("/", (req, res) => {
  res.send("✅ OneKamer backend est opérationnel !");
});

// Port dynamique (Render + local)
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Serveur Stripe Webhook actif sur port ${PORT}`);
});
