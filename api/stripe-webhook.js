import Stripe from "stripe";
import { createClient } from "@supabase/supabase-js";
import { buffer } from "micro";

const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);

let supabaseClient = null;
function getSupabaseClient() {
  if (supabaseClient) return supabaseClient;
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) {
    throw new Error("SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY manquante");
  }
  supabaseClient = createClient(url, key);
  return supabaseClient;
}

export const config = { api: { bodyParser: false } };

export default async function handler(req, res) {
  if (req.method !== "POST") return res.status(405).end();

  const sig = req.headers["stripe-signature"];
  const buf = await buffer(req);

  let event;
  try {
    event = stripe.webhooks.constructEvent(buf, sig, process.env.STRIPE_WEBHOOK_SECRET);
  } catch (err) {
    console.error("Erreur Webhook:", err.message);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  if (event.type === "checkout.session.completed") {
    const supabase = getSupabaseClient();
    const session = event.data.object;
    const { user_id, pack_id } = session.metadata;

    await supabase.rpc("okc_grant_pack_after_payment", {
      p_user: user_id,
      p_pack_id: pack_id,
      p_status: "paid",
    });
  }

  res.json({ received: true });
}
