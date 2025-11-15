// In server.js (around line 86)

// 🎯 CORRECTED FUNCTION: Fetch Admin Emails from BigQuery 🎯
async function fetchAdminEmails() {
    const query = `
        SELECT Emails
        FROM \`${projectId}.${bigQueryDataset}.${BIGQUERY_ADMIN_TABLE}\`
        WHERE Access = TRUE  -- 👈 FIX: Uses your actual BOOLEAN column name 'Access'
    `;

    try {
        const [rows] = await bigQueryClient.query(query);
        // Map the array of objects [{Emails: 'a@b.com'}, ...] to a simple array of strings ['a@b.com', ...]
        ADMIN_EMAILS_BACKEND_CACHE = rows.map(row => row.Emails);
        console.log(`✅ Admin emails successfully loaded from BigQuery. Total active admins: ${ADMIN_EMAILS_BACKEND_CACHE.length}`);
    } catch (error) {
        console.error('🚨 FATAL ERROR: Failed to fetch admin emails from BigQuery. Using empty list.', error);
        // CRITICAL: Exit the process on failure as the access control list is compromised.
        ADMIN_EMAILS_BACKEND_CACHE = [];
        throw new Error('Failed to load critical admin access list.');
    }
}
