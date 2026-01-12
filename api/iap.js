// api/iap.js
import express from "express";
import { createClient } from "@supabase/supabase-js";
import { generateAppleJwt } from "../utils/appleJwt.js";

const router = express.Router();

/* =========================
   Supabase (COMMUN)
========================= */
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

/* =========================
   Helpers (COMMUN)
========================= */
function base64UrlDecodeToJson(part) {
  const padded = part.replace(/-/g, "+").replace(/_/g, "/");
  const padLen = (4 - (padded.length % 4)) % 4;
  const b64 = padded + "=".repeat(padLen);
  return JSON.parse(Buffer.from(b64, "base64").toString("utf8"));
}
function decodeJwsPayload(jws) {
  const parts = String(jws || "").split(".");
  if (parts.length < 2) throw new Error("Invalid JWS format");
  return base64UrlDecodeToJson(parts[1]);
}
function toIsoOrNull(msOrIso) {
  if (!msOrIso) return null;
  if (typeof msOrIso === "string" && /^\d+$/.test(msOrIso)) {
    const ms = Number(msOrIso);
    if (!Number.isFinite(ms)) return null;
    return new Date(ms).toISOString();
  }
  const d = new Date(msOrIso);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

/* =========================
   Provider logic (NON-COMMUN)
========================= */
function appleBaseUrl() {
  const env = (process.env.APPLE_ENV || "production").toLowerCase();
  return env === "sandbox"
    ? "https://api.storekit-sandbox.itunes.apple.com"
    : "https://api.storekit.itunes.apple.com";
}

/**
 * APPLE verify (NON-COMMUN)
 * Input: transactionId
 * Output standardisé: { providerTxId, originalTxId, storeProductId, purchasedAt, expiresAt, raw }
 */
async function verifyWithApple(transactionId) {
  const jwtToken = generateAppleJwt();

  try {
    const parts = String(jwtToken).split(".");
    const jwtHeader = base64UrlDecodeToJson(parts[0]);
    const jwtPayload = base64UrlDecodeToJson(parts[1]);
  } catch {}

  const envLog = (process.env.APPLE_ENV || "production").toLowerCase();
  const base = appleBaseUrl();
  const url = `${base}/inApps/v1/transactions/${encodeURIComponent(transactionId)}`;

  const res = await fetch(url, {
    method: "GET",
    headers: { Authorization: `Bearer ${jwtToken}`, "Content-Type": "application/json", Accept: "application/json" },
  });

  const text = await res.text();

  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }

  if (!res.ok) {
    const msg =
      json?.errorMessage || json?.message || json?.error || `Apple API error (${res.status})`;
    const err = new Error(msg);
    err.status = res.status;
    err.details = json;
    throw err;
  }

  const signedTx = json?.signedTransactionInfo;
  if (!signedTx) throw new Error("Apple response missing signedTransactionInfo");

  const payload = decodeJwsPayload(signedTx);

  return {
    providerTxId: payload.transactionId,
    originalTxId: payload.originalTransactionId || null,
    storeProductId: payload.productId,
    purchasedAt: toIsoOrNull(payload.purchaseDate),
    expiresAt: toIsoOrNull(payload.expiresDate),
    raw: { apple: json, signedTransactionInfo: signedTx, decoded: payload },
  };
}

/**
 * GOOGLE verify (NON-COMMUN) — stub demandé
 * On renvoie volontairement non implémenté (500)
 */
async function verifyWithGoogle(/* transactionId, purchaseToken, productId... */) {
  const err = new Error("Google IAP verification not implemented yet.");
  err.status = 500;
  throw err;
}

/* =========================
   DB actions (COMMUN)
========================= */

/** COMMUN: retrouve mapping (iap_product_map) à partir de storeProductId */
async function loadProductMapping({ platform, provider, storeProductId }) {
  const { data, error } = await supabase
    .from("iap_product_map")
    .select("*")
    .eq("platform", platform)
    .eq("provider", provider)
    .eq("store_product_id", storeProductId)
    .eq("is_active", true)
    .maybeSingle();

  if (error) throw Object.assign(new Error("Supabase error: iap_product_map"), { details: error });
  if (!data) {
    const e = new Error("Unknown store_product_id (no active mapping)");
    e.status = 400;
    e.details = { platform, provider, storeProductId };
    throw e;
  }
  return data; // contient kind, plan_key, pack_id...
}

/** COMMUN: anti double-crédit via iap_transactions unique (provider, transaction_id) */
async function findExistingTransaction(provider, providerTxId) {
  const { data, error } = await supabase
    .from("iap_transactions")
    .select("*")
    .eq("provider", provider)
    .eq("transaction_id", providerTxId)
    .maybeSingle();

  if (error) throw Object.assign(new Error("Supabase error: iap_transactions check"), { details: error });
  return data || null;
}

/** COMMUN: insert iap_transactions */
async function insertTransaction({
  userId,
  platform,
  provider,
  providerTxId,
  originalTxId,
  storeProductId,
  kind,
  purchasedAt,
  expiresAt,
  raw,
}) {
  const payload = {
    user_id: userId,
    platform,
    provider,
    transaction_id: providerTxId,
    original_transaction_id: originalTxId,
    product_id: storeProductId,
    product_type: kind, // 'subscription'|'coins'
    status: "paid",
    raw,
    purchased_at: purchasedAt,
    expires_at: expiresAt,
  };

  const { data, error } = await supabase
    .from("iap_transactions")
    .insert(payload)
    .select("*")
    .single();

  if (error) {
    // collision unique => déjà inséré par une autre requête
    if (String(error.code) === "23505") return null;
    throw Object.assign(new Error("Supabase error: iap_transactions insert"), { details: error });
  }
  return data;
}

/** COMMUN: applique l’effet business en fonction de kind */
async function applyBusinessEffect({ userId, mapping, purchasedAt, expiresAt }) {
  if (mapping.kind === "subscription") {
    // NOTE: on stocke plan_key dans abonnements.plan_name (chez vous)
    // IMPORTANT: profile_id doit correspondre à userId chez vous (sinon faudra mapper)
    const startDate = purchasedAt || new Date().toISOString();

    // Remplacement de l'upsert (qui exige une contrainte UNIQUE sur profile_id) par un read-then-update/insert
    const { data: existingRows, error: selErr } = await supabase
      .from("abonnements")
      .select("id, start_date")
      .eq("profile_id", userId)
      .limit(1);

    if (selErr) {
      throw Object.assign(new Error("Supabase error: abonnements read"), { details: selErr });
    }

    const existing = Array.isArray(existingRows) && existingRows.length > 0 ? existingRows[0] : null;
    const effectiveStart = existing?.start_date || startDate;

    if (existing) {
      const { error: updErr } = await supabase
        .from("abonnements")
        .update({
          plan_name: mapping.plan_key,
          status: "active",
          start_date: effectiveStart,
          end_date: expiresAt,
          auto_renew: true,
        })
        .eq("id", existing.id);

      if (updErr) {
        throw Object.assign(new Error("Supabase error: abonnements update"), { details: updErr });
      }
    } else {
      const { error: insErr } = await supabase
        .from("abonnements")
        .insert({
          profile_id: userId,
          plan_name: mapping.plan_key,
          status: "active",
          start_date: startDate,
          end_date: expiresAt,
          auto_renew: true,
        });

      if (insErr) {
        throw Object.assign(new Error("Supabase error: abonnements insert"), { details: insErr });
      }
    }

    return { kind: "subscription", plan_key: mapping.plan_key };
  }

  if (mapping.kind === "coins") {
    // Ici il faut connaître la colonne "nombre de coins" dans okcoins_packs
    // Je garde coins_amount par défaut -> tu me dis le vrai nom et je l’ajuste.
    const { data: pack, error: packErr } = await supabase
      .from("okcoins_packs")
      .select("id, coins")
      .eq("id", mapping.pack_id)
      .single();

    if (packErr) throw Object.assign(new Error("Supabase error: okcoins_packs read"), { details: packErr });

    const coinsToAdd = Number(pack.coins || 0);
    if (!Number.isFinite(coinsToAdd) || coinsToAdd <= 0) {
      const e = new Error("Invalid okcoins_packs.coins_amount (must be > 0)");
      e.status = 500;
      e.details = pack;
      throw e;
    }

    // upsert/increment okcoins_users_balance
    const { data: bal, error: balErr } = await supabase
      .from("okcoins_users_balance")
      .select("user_id, coins_balance")
      .eq("user_id", userId)
      .maybeSingle();

    if (balErr) throw Object.assign(new Error("Supabase error: okcoins_users_balance read"), { details: balErr });

    if (!bal) {
      const { error: insErr } = await supabase
        .from("okcoins_users_balance")
        .insert({ user_id: userId, coins_balance: coinsToAdd });

      if (insErr) throw Object.assign(new Error("Supabase error: okcoins_users_balance insert"), { details: insErr });
    } else {
      const newBal = Number(bal.coins_balance || 0) + coinsToAdd;
      const { error: updErr } = await supabase
        .from("okcoins_users_balance")
        .update({ coins_balance: newBal })
        .eq("user_id", userId);

      if (updErr) throw Object.assign(new Error("Supabase error: okcoins_users_balance update"), { details: updErr });
    }

    return { kind: "coins", pack_id: mapping.pack_id, coins_added: coinsToAdd };
  }

  const e = new Error("Unexpected mapping.kind");
  e.status = 500;
  e.details = { kind: mapping.kind };
  throw e;
}

/* =========================
   Route handler (COMMUN)
========================= */
router.post("/iap/verify", async (req, res) => {
  try {
    const { platform, provider, userId, transactionId } = req.body || {};

    // COMMUN: validation
    if (!platform || !provider || !userId || !transactionId) {
      return res.status(400).json({
        ok: false,
        error: "Missing required fields: platform, provider, userId, transactionId",
      });
    }

    // NON-COMMUN: provider verification (retour standardisé)
    let verified;
    if (provider === "apple") {
      verified = await verifyWithApple(transactionId);
    } else if (provider === "google") {
      verified = await verifyWithGoogle(transactionId);
    } else {
      return res.status(400).json({ ok: false, error: "Unsupported provider" });
    }

    // COMMUN: mapping produit
    const mapping = await loadProductMapping({
      platform,
      provider,
      storeProductId: verified.storeProductId,
    });

    // COMMUN: anti double-crédit
    const existing = await findExistingTransaction(provider, verified.providerTxId);
    if (existing) {
      return res.status(200).json({
        ok: true,
        alreadyProcessed: true,
        transaction: {
          id: existing.id,
          provider: existing.provider,
          transaction_id: existing.transaction_id,
          product_id: existing.product_id,
          product_type: existing.product_type,
          status: existing.status,
        },
      });
    }

    // COMMUN: insert transaction
    const inserted = await insertTransaction({
      userId,
      platform,
      provider,
      providerTxId: verified.providerTxId,
      originalTxId: verified.originalTxId,
      storeProductId: verified.storeProductId,
      kind: mapping.kind,
      purchasedAt: verified.purchasedAt,
      expiresAt: verified.expiresAt,
      raw: verified.raw,
    });

    // Si null => collision UNIQUE, donc quelqu’un l’a déjà inséré entre temps
    if (!inserted) {
      return res.status(200).json({
        ok: true,
        alreadyProcessed: true,
        note: "Transaction already inserted by another request (unique constraint).",
      });
    }

    // COMMUN: appliquer effet business
    const effect = await applyBusinessEffect({
      userId,
      mapping,
      purchasedAt: verified.purchasedAt,
      expiresAt: verified.expiresAt,
    });

    return res.status(200).json({
      ok: true,
      effect,
      transaction: {
        id: inserted.id,
        transaction_id: inserted.transaction_id,
        status: inserted.status,
      },
    });
  } catch (e) {
    return res.status(e?.status || 500).json({
      ok: false,
      error: e?.message || "Unknown error",
      details: e?.details || null,
    });
  }
});

router.post("/iap/restore", async (req, res) => {
  try {
    const { platform, provider, userId, transactionIds, transactionId } = req.body || {};

    if (!platform || !provider || !userId) {
      return res.status(400).json({
        ok: false,
        error: "Missing required fields: platform, provider, userId",
      });
    }

    const list = Array.isArray(transactionIds)
      ? transactionIds
      : (transactionId ? [transactionId] : []);

    if (!list.length) {
      return res.status(400).json({ ok: false, error: "Missing transactionIds or transactionId" });
    }

    const uniq = Array.from(new Set(list.map((x) => String(x))));
    const results = [];

    for (const tx of uniq) {
      try {
        let verified;
        if (provider === "apple") {
          verified = await verifyWithApple(tx);
        } else if (provider === "google") {
          verified = await verifyWithGoogle(tx);
        } else {
          return res.status(400).json({ ok: false, error: "Unsupported provider" });
        }

        const mapping = await loadProductMapping({
          platform,
          provider,
          storeProductId: verified.storeProductId,
        });

        if (mapping.kind !== "subscription") {
          results.push({
            tx: verified.providerTxId,
            productId: verified.storeProductId,
            skipped: true,
            reason: "not_restorable",
          });
          continue;
        }

        const existing = await findExistingTransaction(provider, verified.providerTxId);
        if (!existing) {
          await insertTransaction({
            userId,
            platform,
            provider,
            providerTxId: verified.providerTxId,
            originalTxId: verified.originalTxId,
            storeProductId: verified.storeProductId,
            kind: mapping.kind,
            purchasedAt: verified.purchasedAt,
            expiresAt: verified.expiresAt,
            raw: verified.raw,
          });
        }

        const effect = await applyBusinessEffect({
          userId,
          mapping,
          purchasedAt: verified.purchasedAt,
          expiresAt: verified.expiresAt,
        });

        results.push({
          tx: verified.providerTxId,
          productId: verified.storeProductId,
          effect,
        });
      } catch (e) {
        results.push({ tx: String(tx), error: e?.message || "restore failed", details: e?.details || null });
      }
    }

    return res.status(200).json({ ok: true, results });
  } catch (e) {
    return res.status(e?.status || 500).json({
      ok: false,
      error: e?.message || "Unknown error",
      details: e?.details || null,
    });
  }
});

router.post("/iap/cancel", async (req, res) => {
  try {
    const { userId } = req.body || {};
    if (!userId) {
      return res.status(400).json({ ok: false, error: "Missing required field: userId" });
    }

    const nowIso = new Date().toISOString();

    const { data, error } = await supabase
      .from("abonnements")
      .update({ status: "canceled", end_date: nowIso, auto_renew: false })
      .eq("profile_id", userId)
      .select("profile_id, plan_name, status, start_date, end_date, auto_renew")
      .maybeSingle();

    if (error) {
      throw Object.assign(new Error("Supabase error: abonnements cancel"), { details: error });
    }

    if (!data) {
      return res.status(404).json({ ok: false, error: "No subscription found for user" });
    }

    return res.status(200).json({ ok: true, subscription: data });
  } catch (e) {
    return res.status(e?.status || 500).json({
      ok: false,
      error: e?.message || "Unknown error",
      details: e?.details || null,
    });
  }
});

export default router;
