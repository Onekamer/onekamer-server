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
  const issuerId = process.env.APPLE_ISSUER_ID;
  const keyId = process.env.APPLE_KEY_ID;
  let privateKey = process.env.APPLE_PRIVATE_KEY;

  if (!issuerId || !keyId || !privateKey) {
    throw new Error(
      "Missing Apple env vars. Need APPLE_ISSUER_ID, APPLE_KEY_ID, APPLE_PRIVATE_KEY"
    );
  }

  // Si la clé est stockée en env avec des \n, on reconvertit en vrais retours à la ligne
  privateKey = privateKey.replace(/\\n/g, "\n");

  const now = Math.floor(Date.now() / 1000);

  // Apple recommande des JWT courts (ex: 5-20 min)
  const payload = {
    iss: issuerId,
    iat: now,
    exp: now + 60 * 10, // 10 minutes
    aud: "appstoreconnect-v1",
  };

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
