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
async function applyBusinessEffect({ userId, mapping, purchasedAt, expiresAt, providerTxId, storeProductId, platform, provider, isNewTx = false }) {
  if (mapping.kind === "subscription") {
    // NOTE: on stocke plan_key dans abonnements.plan_name (chez vous)
    // IMPORTANT: profile_id doit correspondre à userId chez vous (sinon faudra mapper)
    const startDate = purchasedAt || new Date().toISOString();

    // Remplacement de l'upsert (qui exige une contrainte UNIQUE sur profile_id) par un read-then-update/insert
    const { data: existingRows, error: selErr } = await supabase
      .from("abonnements")
      .select("id, start_date, end_date")
      .eq("profile_id", userId)
      .limit(1);

    if (selErr) {
      throw Object.assign(new Error("Supabase error: abonnements read"), { details: selErr });
    }

    const existing = Array.isArray(existingRows) && existingRows.length > 0 ? existingRows[0] : null;
    const effectiveStart = existing?.start_date || startDate;
    const nowMs = Date.now();
    const expMs = expiresAt ? new Date(expiresAt).getTime() : null;
    // Ne jamais régresser la date de fin: on garde le max entre l'existant et le nouveau expiresAt
    const effectiveEnd = (() => {
      const prev = existing?.end_date || null;
      if (!prev) return expiresAt || null;
      if (!expiresAt) return prev;
      return new Date(prev).getTime() >= new Date(expiresAt).getTime() ? prev : expiresAt;
    })();
    const isActive = effectiveEnd ? new Date(effectiveEnd).getTime() > nowMs : false;
    const nextStatus = isActive ? "active" : "expired";
    const nextAutoRenew = isActive ? true : false;

    if (existing) {
      const { error: updErr } = await supabase
        .from("abonnements")
        .update({
          plan_name: mapping.plan_key,
          status: nextStatus,
          start_date: effectiveStart,
          end_date: effectiveEnd,
          auto_renew: nextAutoRenew,
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
          status: expMs && expMs > nowMs ? "active" : "expired",
          start_date: startDate,
          end_date: expiresAt,
          auto_renew: expMs && expMs > nowMs ? true : false,
        });

      if (insErr) {
        throw Object.assign(new Error("Supabase error: abonnements insert"), { details: insErr });
      }
    }

    if (nextStatus === "active") {
      await supabase
        .from("profiles")
        .update({ plan: mapping.plan_key, updated_at: new Date().toISOString() })
        .eq("id", userId);
    } else {
      const { data: perm, error: permErr } = await supabase
        .from("abonnements")
        .select("id")
        .eq("profile_id", userId)
        .eq("is_permanent", true)
        .limit(1)
        .maybeSingle();
      const finalPlan = !permErr && perm && perm.id ? "vip" : "free";
      await supabase
        .from("profiles")
        .update({ plan: finalPlan, updated_at: new Date().toISOString() })
        .eq("id", userId);
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

    let finalBalance;
    if (!bal) {
      const { error: insErr } = await supabase
        .from("okcoins_users_balance")
        .insert({ user_id: userId, coins_balance: coinsToAdd });

      if (insErr) throw Object.assign(new Error("Supabase error: okcoins_users_balance insert"), { details: insErr });
      finalBalance = coinsToAdd;
    } else {
      const newBal = Number(bal.coins_balance || 0) + coinsToAdd;
      const { error: updErr } = await supabase
        .from("okcoins_users_balance")
        .update({ coins_balance: newBal })
        .eq("user_id", userId);

      if (updErr) throw Object.assign(new Error("Supabase error: okcoins_users_balance update"), { details: updErr });
      finalBalance = newBal;
    }

    if (isNewTx) {
      const ledgerPayload = {
        user_id: userId,
        delta: coinsToAdd,
        kind: "purchase_in",
        ref_type: "iap",
        ref_id: mapping.pack_id || null,
        balance_after: finalBalance,
        metadata: {
          platform: platform || null,
          provider: provider || null,
          productId: storeProductId || null,
          pack_id: mapping.pack_id || null,
          purchased_at: purchasedAt || null,
          tx_id: providerTxId || null,
        },
      };
      const { error: ledErr } = await supabase
        .from("okcoins_ledger")
        .insert(ledgerPayload);
      if (ledErr) {
        try {
          const fallback = { ...ledgerPayload, kind: "recharge_in", metadata: { ...ledgerPayload.metadata, fallback_kind: true } };
          const { error: ledErr2 } = await supabase.from("okcoins_ledger").insert(fallback);
          if (ledErr2) {
            console.warn("okcoins_ledger insert failed", ledErr, ledErr2);
          }
        } catch (e) {
          console.warn("okcoins_ledger insert exception", e);
        }
        // Ne pas jeter d'erreur: l'achat doit rester réussi même si le ledger échoue
      }
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
      // Même si la transaction existe déjà, on applique l'effet business de façon idempotente
      const effect = await applyBusinessEffect({
        userId,
        mapping,
        purchasedAt: verified.purchasedAt,
        expiresAt: verified.expiresAt,
        providerTxId: verified.providerTxId,
        storeProductId: verified.storeProductId,
        platform,
        provider,
        isNewTx: false,
      });

      return res.status(200).json({
        ok: true,
        alreadyProcessed: true,
        effect,
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
      const effect = await applyBusinessEffect({
        userId,
        mapping,
        purchasedAt: verified.purchasedAt,
        expiresAt: verified.expiresAt,
        providerTxId: verified.providerTxId,
        storeProductId: verified.storeProductId,
        platform,
        provider,
        isNewTx: false,
      });

      return res.status(200).json({
        ok: true,
        alreadyProcessed: true,
        note: "Transaction already inserted by another request (unique constraint).",
        effect,
      });
    }

    // COMMUN: appliquer effet business
    const effect = await applyBusinessEffect({
      userId,
      mapping,
      purchasedAt: verified.purchasedAt,
      expiresAt: verified.expiresAt,
      providerTxId: verified.providerTxId,
      storeProductId: verified.storeProductId,
      platform,
      provider,
      isNewTx: true,
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

    // Sélectionne l'abonnement le plus pertinent (par exemple le plus récent via end_date)
    const { data: sub, error: selErr } = await supabase
      .from("abonnements")
      .select("id, profile_id, plan_name, status, start_date, end_date, auto_renew")
      .eq("profile_id", userId)
      .order("end_date", { ascending: false })
      .limit(1)
      .maybeSingle();

    if (selErr) {
      throw Object.assign(new Error("Supabase error: abonnements read for cancel"), { details: selErr });
    }

    if (!sub) {
      return res.status(404).json({ ok: false, error: "No subscription found for user" });
    }

    const { data: upd, error } = await supabase
      .from("abonnements")
      .update({ status: "expired", end_date: nowIso, auto_renew: false })
      .eq("id", sub.id)
      .select("profile_id, plan_name, status, start_date, end_date, auto_renew")
      .single();

    if (error) {
      throw Object.assign(new Error("Supabase error: abonnements cancel"), { details: error });
    }

    // Si VIP à vie, ne pas downgrader le plan
    const { data: perm, error: permErr } = await supabase
      .from("abonnements")
      .select("id")
      .eq("profile_id", userId)
      .eq("is_permanent", true)
      .limit(1)
      .maybeSingle();
    if (!permErr && perm && perm.id) {
      return res.status(200).json({ ok: true, subscription: upd });
    }

    // Aligner le profil sur free pour l'UI si non-permanent
    try {
      await supabase
        .from("profiles")
        .update({ plan: "free", updated_at: new Date().toISOString() })
        .eq("id", userId);
    } catch {}

    return res.status(200).json({ ok: true, subscription: upd });
  } catch (e) {
    return res.status(e?.status || 500).json({
      ok: false,
      error: e?.message || "Unknown error",
      details: e?.details || null,
    });
  }
});

router.get("/iap/subscription", async (req, res) => {
  try {
    const userId = String(req.query.userId || "").trim();
    if (!userId) {
      return res.status(400).json({ ok: false, error: "Missing required field: userId" });
    }

    const { data, error } = await supabase
      .from("abonnements")
      .select("profile_id, plan_name, status, start_date, end_date, auto_renew")
      .eq("profile_id", userId)
      .order("end_date", { ascending: false })
      .limit(1)
      .maybeSingle();

    if (error) {
      throw Object.assign(new Error("Supabase error: abonnements read"), { details: error });
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

router.post("/iap/sync-subscription", async (req, res) => {
  try {
    const { userId } = req.body || {};
    if (!userId) {
      return res.status(400).json({ ok: false, error: "Missing required field: userId" });
    }

    const { data: tx, error: txErr } = await supabase
      .from("iap_transactions")
      .select("original_transaction_id, transaction_id")
      .eq("user_id", userId)
      .eq("provider", "apple")
      .eq("product_type", "subscription")
      .order("purchased_at", { ascending: false })
      .limit(1)
      .maybeSingle();

    if (txErr) {
      return res.status(500).json({ ok: false, error: txErr.message || "transactions_read_failed" });
    }
    if (!tx) {
      return res.status(404).json({ ok: false, error: "No original transaction found" });
    }

    let originalId = tx?.original_transaction_id || null;
    if (!originalId && tx?.transaction_id) {
      try {
        const verified = await verifyWithApple(tx.transaction_id);
        originalId = verified?.originalTxId || null;
      } catch {}
    }
    if (!originalId) {
      return res.status(404).json({ ok: false, error: "No original transaction id" });
    }

    const base = appleBaseUrl();
    const url = `${base}/inApps/v1/subscriptions/${encodeURIComponent(originalId)}`;
    const jwtToken = generateAppleJwt();
    const r = await fetch(url, {
      method: "GET",
      headers: { Authorization: `Bearer ${jwtToken}`, Accept: "application/json" },
    });
    const t = await r.text();
    let j;
    try {
      j = t ? JSON.parse(t) : {};
    } catch {
      j = { raw: t };
    }
    if (!r.ok) {
      const msg = j?.errorMessage || j?.message || j?.error || `Apple API error (${r.status})`;
      return res.status(r.status).json({ ok: false, error: msg, details: j });
    }

    const items = Array.isArray(j?.data) ? j.data : [];
    const lastTxs = [];
    for (const it of items) {
      const arr = Array.isArray(it?.lastTransactions) ? it.lastTransactions : [];
      for (const x of arr) lastTxs.push(x);
    }
    if (!lastTxs.length) {
      return res.status(404).json({ ok: false, error: "No lastTransactions" });
    }

    let best = null;
    for (const x of lastTxs) {
      try {
        const txInfo = decodeJwsPayload(x.signedTransactionInfo);
        const rnInfo = x?.signedRenewalInfo ? decodeJwsPayload(x.signedRenewalInfo) : null;
        const productId = txInfo?.productId;
        if (!productId) continue;
        let mapping;
        try {
          mapping = await loadProductMapping({ platform: "ios", provider: "apple", storeProductId: productId });
        } catch {
          continue;
        }
        if (mapping?.kind !== "subscription" || String(mapping?.plan_key) !== "vip") continue;
        const expiresIso = toIsoOrNull(txInfo?.expiresDate);
        const ar = rnInfo?.autoRenewStatus;
        const autoRenew = ar === 1 || ar === "1";
        if (!expiresIso) continue;
        const cand = {
          mapping,
          productId,
          expiresIso,
          autoRenew,
        };
        if (!best || new Date(cand.expiresIso).getTime() > new Date(best.expiresIso).getTime()) best = cand;
      } catch {}
    }

    if (!best) {
      return res.status(404).json({ ok: false, error: "No mappable subscription" });
    }

    const now = Date.now();
    const active = new Date(best.expiresIso).getTime() > now;
    const nextStatus = active ? "active" : "expired";
    const nextPlan = active ? best.mapping.plan_key : "free";

    // Protéger les VIP à vie contre le passage à free
    let finalPlan = nextPlan;
    if (finalPlan === "free") {
      const { data: perm, error: permErr } = await supabase
        .from("abonnements")
        .select("id")
        .eq("profile_id", userId)
        .eq("is_permanent", true)
        .limit(1)
        .maybeSingle();
      if (!permErr && perm && perm.id) {
        finalPlan = "vip";
      }
    }

    const { data: sub, error: subSelErr } = await supabase
      .from("abonnements")
      .select("id, start_date")
      .eq("profile_id", userId)
      .order("end_date", { ascending: false })
      .limit(1)
      .maybeSingle();
    if (subSelErr) {
      return res.status(500).json({ ok: false, error: subSelErr.message || "subscription_read_failed" });
    }

    if (sub) {
      const { data: up, error: updErr } = await supabase
        .from("abonnements")
        .update({ plan_name: best.mapping.plan_key, status: nextStatus, end_date: best.expiresIso, auto_renew: best.autoRenew })
        .eq("id", sub.id)
        .select("profile_id, plan_name, status, start_date, end_date, auto_renew")
        .single();
      if (updErr) {
        return res.status(500).json({ ok: false, error: updErr.message || "subscription_update_failed" });
      }
      try {
        await supabase.from("profiles").update({ plan: finalPlan, updated_at: new Date().toISOString() }).eq("id", userId);
      } catch {}
      return res.status(200).json({ ok: true, subscription: up });
    } else {
      const { data: ins, error: insErr } = await supabase
        .from("abonnements")
        .insert({ profile_id: userId, plan_name: best.mapping.plan_key, status: nextStatus, start_date: new Date().toISOString(), end_date: best.expiresIso, auto_renew: best.autoRenew })
        .select("profile_id, plan_name, status, start_date, end_date, auto_renew")
        .single();
      if (insErr) {
        return res.status(500).json({ ok: false, error: insErr.message || "subscription_insert_failed" });
      }
      try {
        await supabase.from("profiles").update({ plan: finalPlan, updated_at: new Date().toISOString() }).eq("id", userId);
      } catch {}
      return res.status(200).json({ ok: true, subscription: ins });
    }
  } catch (e) {
    return res.status(e?.status || 500).json({
      ok: false,
      error: e?.message || "Unknown error",
      details: e?.details || null,
    });
  }
});

export default router;
