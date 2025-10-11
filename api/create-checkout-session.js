import Stripe from "stripe";
import { createClient } from "@supabase/supabase-js";

const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

export default async function handler(req, res) {
  if (req.method !== "POST") return res.status(405).json({ error: "Méthode non autorisée" });

  const { pack_id, user_id } = req.body;

  const { data: pack } = await supabase.from("okcoins_packs").select("*").eq("id", pack_id).single();
  if (!pack) return res.status(404).json({ error: "Pack introuvable" });

  const session = await stripe.checkout.sessions.create({
    payment_method_types: ["card"],
    line_items: [
      {
        price_data: {
          currency: "eur",
          product_data: { name: pack.pack_name },
          unit_amount: pack.price_eur * 100,
        },
        quantity: 1,
      },
    ],
    mode: "payment",
    success_url: `https://onekamer.co/payment-success?session_id={CHECKOUT_SESSION_ID}`,
    cancel_url: `https://onekamer.co/payment-cancel`,
    metadata: { user_id, pack_id },
  });

  res.json({ url: session.url });
}
