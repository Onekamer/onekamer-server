// utils/mentions.js

/**
 * Détecte les mentions (@username) dans un texte
 * et les enregistre dans la table "mentions" sur Supabase.
 *
 * @param {Object} params
 * @param {string} params.text - le contenu du message ou commentaire
 * @param {string} params.senderId - l'UUID de l'utilisateur qui écrit
 * @param {string} params.contentId - l'UUID du post, commentaire ou message
 * @param {string} params.contentType - 'post', 'comment' ou 'message'
 * @param {object} params.supabase - instance Supabase côté serveur
 */
export async function detectAndSaveMentions({ text, senderId, contentId, contentType, supabase }) {
  try {
    // Recherche les mots précédés de @ (lettres, chiffres ou underscores)
    const usernames = [...text.matchAll(/@([a-zA-Z0-9_]+)/g)].map((m) => m[1]);
    if (usernames.length === 0) return;

    // Récupère les utilisateurs correspondants
    const { data: users, error: usersError } = await supabase
      .from('profiles')
      .select('id, username')
      .in('username', usernames);

    if (usersError) {
      console.error('Erreur récupération utilisateurs mentionnés:', usersError);
      return;
    }

    // Insère chaque mention dans la table "mentions"
    for (const user of users) {
      const { error } = await supabase.from('mentions').insert({
        sender_id: senderId,
        mentioned_user_id: user.id,
        content_id: contentId,
        content_type: contentType,
      });

      if (error) console.error('Erreur insertion mention:', error);
    }

    console.log(`✅ Mentions détectées et enregistrées pour: ${usernames.join(', ')}`);
  } catch (err) {
    console.error('Erreur dans detectAndSaveMentions:', err);
  }
}
