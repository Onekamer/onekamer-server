// Script de rattrapage : créer les QR codes manquants pour les événements payés
// À exécuter une fois pour corriger les événements existants

import * as dotenv from "dotenv";
dotenv.config({ path: ".env.local" });

import { createClient } from "@supabase/supabase-js";
import crypto from "crypto";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

async function migrateMissingQRCodes() {
  console.log("🔍 Recherche des paiements sans QR code...");
  
  try {
    // Récupérer tous les paiements payés qui n'ont pas de QR correspondant
    const { data: paymentsWithoutQR, error: fetchError } = await supabase
      .from("event_payments")
      .select(`
        user_id,
        event_id,
        status,
        amount_total,
        amount_paid,
        evenements!inner(
          id,
          title,
          date,
          end_date,
          end_time
        )
      `)
      .eq("status", "paid")
      .not("event_qrcodes", "user_id", "event_id"); // Vérifie qu'il n'y a pas déjà un QR
    
    if (fetchError) {
      console.error("❌ Erreur récupération paiements:", fetchError);
      return;
    }

    console.log(`📊 ${paymentsWithoutQR?.length || 0} paiements sans QR trouvés`);

    if (!paymentsWithoutQR || paymentsWithoutQR.length === 0) {
      console.log("✅ Tous les QR codes sont déjà créés");
      return;
    }

    let created = 0;
    let skipped = 0;

    for (const payment of paymentsWithoutQR) {
      const { user_id, event_id, evenements } = payment;
      
      // Vérifier si l'événement n'est pas terminé
      const endDateIso = evenements.end_date;
      const endTime = evenements.end_time || "23:59";
      let eventEnded = false;
      
      if (endDateIso) {
        const eventEnd = new Date(`${endDateIso}T${endTime}:00`);
        const now = new Date();
        eventEnded = eventEnd < now;
      }
      
      if (eventEnded) {
        console.log(`⏭️  Événement terminé - skip: ${evenements.title}`);
        skipped++;
        continue;
      }
      
      // Vérifier si un QR existe déjà (double sécurité)
      const { data: existingQR } = await supabase
        .from("event_qrcodes")
        .select("id")
        .eq("user_id", user_id)
        .eq("event_id", event_id)
        .eq("status", "active")
        .maybeSingle();
      
      if (existingQR) {
        console.log(`⏭️  QR déjà existant - skip: ${evenements.title}`);
        skipped++;
        continue;
      }
      
      // Créer le QR code
      const qrcode_value = crypto.randomUUID();
      const { error: insertError } = await supabase
        .from("event_qrcodes")
        .insert([{
          user_id,
          event_id,
          qrcode_value,
          status: "active",
          created_at: new Date().toISOString()
        }]);
      
      if (insertError) {
        console.error(`❌ Erreur création QR pour ${evenements.title}:`, insertError);
      } else {
        console.log(`✅ QR créé pour: ${evenements.title} (user: ${user_id})`);
        created++;
      }
    }
    
    console.log(`\n📈 Bilan: ${created} QR créés, ${skipped} ignorés`);
    
  } catch (error) {
    console.error("❌ Erreur générale:", error);
  }
}

// Lancer le script
migrateMissingQRCodes();
