// utils/appleJwt.js
import jwt from "jsonwebtoken";

/**
 * Génère un JWT (ES256) pour appeler l'App Store Server API
 * Requiert ces env vars (Render):
 * - APPLE_ISSUER_ID
 * - APPLE_KEY_ID
 * - APPLE_PRIVATE_KEY   (contenu de la .p8, avec les retours à la ligne \n)
 */
export function generateAppleJwt() {
  const issuerId = (process.env.APPLE_ISSUER_ID || "").trim();
  const iapKeyId = (process.env.APPLE_IAP_KEY_ID || "").trim();
  const iapPrivate = process.env.APPLE_IAP_PRIVATE_KEY || "";
  const ascKeyId = (process.env.APPLE_KEY_ID || "").trim();
  const ascPrivate = process.env.APPLE_PRIVATE_KEY || "";
  const useIap = Boolean(iapKeyId && iapPrivate);
  const keyId = useIap ? iapKeyId : ascKeyId;
  let privateKey = useIap ? iapPrivate : ascPrivate;
  const bundleId = (process.env.APPLE_BUNDLE_ID || "").trim();
  const keySrc = useIap ? "iap" : "asc";

  try {
    const orig = String(privateKey || "");
    const preHasEscNL = /\\n/.test(orig);
    const preHasRealNL = orig.includes("\n");
    const hasBegin = /BEGIN PRIVATE KEY/.test(orig);
    const hasEnd = /END PRIVATE KEY/.test(orig);
    console.info(`[IAP][PK] src=${keySrc} kid=${keyId} iss=${issuerId} preLen=${orig.length} begin=${hasBegin} end=${hasEnd} escNL=${preHasEscNL} realNL=${preHasRealNL}`);
  } catch {}

  if (!issuerId || !keyId || !privateKey) {
    throw new Error(
      "Missing Apple env vars. Need APPLE_ISSUER_ID and IAP or ASC key (KEY_ID + PRIVATE_KEY)"
    );
  }

  // Si la clé est stockée en env avec des \n, on reconvertit en vrais retours à la ligne
  privateKey = privateKey.replace(/\r/g, "").replace(/\\n/g, "\n");

  try {
    const post = String(privateKey || "");
    const postHasRealNL = post.includes("\n");
    console.info(`[IAP][PK] postLen=${post.length} realNL=${postHasRealNL}`);
  } catch {}

  const now = Math.floor(Date.now() / 1000);

  // Apple recommande des JWT courts (ex: 5-20 min)
  const payload = {
    iss: issuerId,
    iat: now - 10,
    exp: now + 60 * 5, // 5 minutes
    aud: "appstoreconnect-v1",
  };

  if (bundleId) {
    payload.bid = bundleId;
  }

  const token = jwt.sign(payload, privateKey, {
    algorithm: "ES256",
    header: {
      alg: "ES256",
      kid: keyId,
      typ: "JWT",
    },
  });

  return token;
}
