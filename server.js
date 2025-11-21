const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const cors = require('cors');
const dotenv = require('dotenv');
const moment = require('moment');
// --- New Imports for Google Sheets ---
const { google } = require('googleapis');
// --- New Imports for Google Sheets ---

dotenv.config();

// --- Configuration Variables ---
const projectId = process.env.GOOGLE_PROJECT_ID;
const bigQueryDataset = process.env.BIGQUERY_DATASET;
const bigQueryTable = process.env.BIGQUERY_TABLE; // Your main task table

// Static Table Names
const bigQueryTable2 = "Per_Key_Per_Day";
const bigQueryTable3 = "Per_Person_Per_Day";
const bigQueryStatusUpdateTable = "StatusUpdatesBackup";

// 🚀 Admin Table Configuration 🚀
const ADMIN_TABLE_SHORT_NAME = "AdminEmails_Native";
const BIGQUERY_ADMIN_TABLE_NATIVE_FULL = `${projectId}.${bigQueryDataset}.${ADMIN_TABLE_SHORT_NAME}`;
const SYSTEM_EMAIL_FOR_GLOBAL_TASKS = "systems@brightbraintech.com";

// --- Google Sheet Configuration ---
const SPREADSHEET_ID = '1dBgy8CRGQ9lUFvuBvcS0CWu085PzGtQZlBWKox0m6IY';
const SHEET_NAME = 'Form Responses';
// Based on the requirement:
const DELIVERY_CODE_COLUMN = 'R'; // Column containing the DelCode_w_o__ to match
const DUE_DATE_COLUMN = 'X'; // Column containing the Due Date to update

const app = express();

// --- CORS Configuration (unchanged) ---
const allowedOrigins = [
    'http://localhost:3000',
    /^https:\/\/.*\.vercel\.app$/, // Allows any subdomain of vercel.app
    'https://scheduler-ui-roan.vercel.app', // Explicitly allow the Vercel frontend URL
    'https://server-ui-2.onrender.com' // Allow self-requests if needed
];

// --- 1. CORS CONFIGURATION (Consolidated) ---
app.use(cors({
    origin: function (origin, callback) {
        if (!origin) return callback(null, true);

        const isAllowed = allowedOrigins.some(allowedOrigin => {
            if (typeof allowedOrigin === 'string') {
                return allowedOrigin === origin;
            } else if (allowedOrigin instanceof RegExp) {
                return allowedOrigin.test(origin);
            }
            return false;
        });

        if (!isAllowed) {
            const msg = 'The CORS policy for this site does not allow access from the specified Origin.';
            console.error(`CORS BLOCKED for Origin: ${origin}`); // Log blocked origin
            return callback(new Error(msg), false);
        }
        return callback(null, true);
    },
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
    credentials: true
}));
app.use(express.json());

// --- Debugging Logs for Service Account and Config ---
console.log('DEBUG: GOOGLE_PROJECT_ID:', process.env.GOOGLE_PROJECT_ID);
console.log('DEBUG: BIGQUERY_CLIENT_EMAIL:', process.env.BIGQUERY_CLIENT_EMAIL);
console.log('DEBUG: BIGQUERY_PRIVATE_KEY exists:', !!process.env.BIGQUERY_PRIVATE_KEY);
console.log('DEBUG: ADMIN_TABLE_FULL_PATH:', BIGQUERY_ADMIN_TABLE_NATIVE_FULL);

// --- BigQuery Client Initialization ---
const bigQueryClient = new BigQuery({
    projectId: projectId,
    credentials: {
        client_email: process.env.BIGQUERY_CLIENT_EMAIL,
        private_key: process.env.BIGQUERY_PRIVATE_KEY ? process.env.BIGQUERY_PRIVATE_KEY.replace(/\\n/g, '\n') : undefined,
    },
});

// --- Google Sheets Auth and Client Initialization ---
const sheetsAuth = new google.auth.GoogleAuth({
    credentials: {
        client_email: process.env.BIGQUERY_CLIENT_EMAIL,
        private_key: process.env.BIGQUERY_PRIVATE_KEY ? process.env.BIGQUERY_PRIVATE_KEY.replace(/\\n/g, '\n') : undefined,
    },
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
});

const sheets = google.sheets({ version: 'v4', auth: sheetsAuth });
// --- End Google Sheets Client Initialization ---


// --- Admin Email Caching (unchanged) ---
let cachedAdminEmails = [];
let lastAdminFetchTime = 0;
const ADMIN_CACHE_DURATION = 5 * 60 * 1000; // 5 minutes

/**
 * Fetches the list of admin emails from BigQuery, utilizing a 5-minute cache.
 * @returns {Promise<string[]>} A promise that resolves to an array of admin emails.
 */
async function fetchAdminEmailsFromBQ() {
    const now = Date.now();
    // Use cached list ONLY if it has data AND is recent
    if (cachedAdminEmails.length > 0 && (now - lastAdminFetchTime) < ADMIN_CACHE_DURATION) {
        console.log('Backend: Serving admin emails from cache.');
        return cachedAdminEmails;
    }
    // If cache is expired OR empty, proceed to fetch
    if (cachedAdminEmails.length === 0) {
        console.log('Backend: Cache is empty. Fetching admin emails from BigQuery...');
    } else {
        console.log('Backend: Cache expired. Fetching admin emails from BigQuery...');
    }


    const query = `
        SELECT admin_email
        FROM \`${BIGQUERY_ADMIN_TABLE_NATIVE_FULL}\`
    `;

    console.log(`Backend DEBUG: Querying admin table with full path: ${BIGQUERY_ADMIN_TABLE_NATIVE_FULL}`);

    try {
        const [rows] = await bigQueryClient.query(query);
        // Ensure to handle potential nulls or undefined values gracefully
        const adminEmails = rows.map(row => row.admin_email).filter(email => email);

        // 🛑 CRITICAL FIX: Only update cache if we received data 🛑
        if (adminEmails.length > 0) {
            cachedAdminEmails = adminEmails;
            lastAdminFetchTime = now;
            console.log(`Backend: Fetched and cached ${adminEmails.length} admin emails.`);
        } else {
            console.warn("Backend: Fetched 0 admins. NOT updating cache to prevent 'Poisoned Cache'.");
        }
        return adminEmails;
    } catch (error) {
        console.error('ERROR: Failed to fetch admin emails from BigQuery:', error);
        // Fallback: Return the last known good list if fetch failed
        return cachedAdminEmails.length > 0 ? cachedAdminEmails : [];
    }
}


// --- NEW GOOGLE SHEET UPDATE FUNCTION ---

/**
 * Updates the 'Due Date' column (X) in the Google Sheet for a given delivery code (E).
 * @param {string} delCodeWO The Delivery Code (DelCode_w_o__).
 * @param {string} newDeadlineDate The new date string (YYYY-MM-DD).
 * @param {string} userEmail The email of the admin performing the update.
 * @returns {Promise<object>} The Google Sheets API response.
 */
async function updateSheetDeadline(delCodeWO, newDeadlineDate, userEmail) {
    console.log(`Sheet Update: Searching for row with DelCode_w_o__ ${delCodeWO}`);

    // 1. Fetch the Delivery Code column to find the row index.
    const response = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!${DELIVERY_CODE_COLUMN}:${DELIVERY_CODE_COLUMN}`,
        valueInputOption: 'USER_ENTERED', // <-- CHANGE THIS
        resource,
    });

    const rows = response.data.values;
    if (!rows || rows.length === 0) {
        throw new Error('Google Sheet is empty or delivery code column is misconfigured.');
    }

    // Find the row index where the delivery code matches. Start search from Row 2 (index 1) to skip header.
    let targetRowIndex = -1;
    for (let i = 1; i < rows.length; i++) {
        // [i][0] because 'rows' is an array of single-item arrays (the column values)
        if (rows[i] && rows[i][0] === delCodeWO) {
            targetRowIndex = i + 1; // Sheets are 1-indexed
            break;
        }
    }

    if (targetRowIndex === -1) {
        throw new Error(`Delivery Code ${delCodeWO} not found in Google Sheet column ${DELIVERY_CODE_COLUMN}.`);
    }

    // 2. Perform the update on the found row and target column (DUE_DATE_COLUMN)
    const updateRange = `${SHEET_NAME}!${DUE_DATE_COLUMN}${targetRowIndex}`;

    // The date format should match the input received from the frontend (YYYY-MM-DD)
    const valueInputOption = 'USER_ENTERED'; // Treats the string as a date format
    const values = [[newDeadlineDate]];

    const updateResponse = await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: updateRange,
        valueInputOption: valueInputOption,
        requestBody: {
            values: values,
        },
    });

    console.log(`Sheet Update: Successfully updated row ${targetRowIndex} in column ${DUE_DATE_COLUMN}.`);
    return updateResponse.data;
}

// --- API Endpoints ---

// NEW ENDPOINT: Expose Admin Emails to the Frontend (unchanged)
app.get('/api/admins', async (req, res) => {
    try {
        const adminEmails = await fetchAdminEmailsFromBQ();
        res.status(200).json(adminEmails);
    } catch (error) {
        res.status(500).send({ error: 'Failed to fetch admin list.' });
    }
});


// 🚀 UPDATED ENDPOINT: Update Delivery Deadline (Admin Only) 🚀
app.put('/api/delivery/update-deadline', async (req, res) => {
    const { delCodeWO, newDeadlineDate, userEmail } = req.body;

    if (!delCodeWO || !newDeadlineDate || !userEmail) {
        return res.status(400).json({
            message: 'Bad Request: Delivery code, new deadline date, and user email are required.',
        });
    }

    try {
        const adminEmails = await fetchAdminEmailsFromBQ();
        const isAdmin = adminEmails.includes(userEmail);

        if (!isAdmin) {
            console.warn(`SECURITY ALERT: Non-admin user ${userEmail} attempted to change deadline for ${delCodeWO}.`);
            return res.status(403).json({
                message: 'Forbidden: You do not have permission to change the delivery deadline.',
                details: 'Only Admin users can perform this action.',
            });
        }

        // --- STEP 1: UPDATE GOOGLE SHEET (Source of Truth) ---
        console.log(`Backend: Attempting to update Google Sheet for ${delCodeWO} to ${newDeadlineDate}...`);
        await updateSheetDeadline(delCodeWO, newDeadlineDate, userEmail); // Await the sheet update

        // --- STEP 2: UPDATE BIGQUERY (Fast Query Layer) ---
        // Format the date to BigQuery TIMESTAMP format (YYYY-MM-DD HH:mm:ss)
        // We set the time to midnight UTC for consistency.
        const formattedDeadline = moment.utc(newDeadlineDate).format('YYYY-MM-DD 00:00:00');

        // Update the header record (Step_ID = 0)
        const updateQuery = `
            UPDATE \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
            SET Planned_Delivery_Timestamp = @newDeadline, Updated_at = CURRENT_DATETIME('Asia/Kolkata')
            WHERE DelCode_w_o__ = @delCodeWO
            AND Step_ID = 0
        `;
        const updateOptions = {
            query: updateQuery,
            params: { newDeadline: formattedDeadline, delCodeWO: delCodeWO },
            types: { newDeadline: 'TIMESTAMP', delCodeWO: 'STRING' },
            location: 'US',
        };

        console.log(`Backend: Admin ${userEmail} updating deadline for ${delCodeWO} in BigQuery...`);
        const [updateJob] = await bigQueryClient.createQueryJob(updateOptions);
        await updateJob.getQueryResults();

        res.status(200).send({ message: 'Delivery deadline updated in Google Sheet and BigQuery successfully.' });

    } catch (error) {
        console.error('Backend: Error updating delivery deadline:', error);
        // Provide specific error details for debugging the sheet update failure
        res.status(500).json({
            message: 'Failed to update delivery deadline due to a backend error.',
            details: error.message || 'Unknown server error.',
            source: error.message.includes('Google Sheet') ? 'GoogleSheetAPI' : 'BigQueryAPI'
        });
    }
});


// Endpoint to fetch people mapping (unchanged)
app.get('/api/people-mapping', async (req, res) => {
    const NATIVE_PEOPLE_TABLE = 'People_To_Email_Mapping_Native';
    const query = `
        SELECT Current_Employes, Emp_Emails
        FROM \`${projectId}.${bigQueryDataset}.${NATIVE_PEOPLE_TABLE}\`
    `;

    try {
        const [rows] = await bigQueryClient.query(query);
        const formattedRows = rows.map(row => ({
            Current_Employes: row.Current_Employes,
            Emp_Emails: row.Emp_Emails
        }));
        res.status(200).json(formattedRows);
    } catch (error) {
        console.error('Error fetching people mapping from BigQuery:', error);
        res.status(500).send({ error: 'Failed to fetch people mapping data.' });
    }
});

// UPDATED ENDPOINT: Fetch only Active Unique Clients for the Filter Dropdown (unchanged)
app.get('/api/active-clients', async (req, res) => {
    const query = `
        SELECT DISTINCT Client
        FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
        WHERE Inactive = 'Active'
        AND Client IS NOT NULL
        ORDER BY Client
    `;

    try {
        const [rows] = await bigQueryClient.query(query);
        const activeClients = rows.map(row => row.Client);
        console.log(`Backend: Fetched ${activeClients.length} active unique clients.`);
        res.status(200).json(activeClients);
    } catch (error) {
        console.error('Error fetching active clients from BigQuery:', error);
        res.status(500).send({ error: 'Failed to fetch active client list.' });
    }
});


// GET workflow headers only, with filtering for non-admins (unchanged)
app.get('/api/data', async (req, res) => {
    const userEmail = req.query.email;
    const searchQuery = req.query.searchQuery;
    const clientFilter = req.query.clientFilter;

    // --- CHECK 1: Dynamic Admin Status Check ---
    const ADMIN_EMAILS_BACKEND = await fetchAdminEmailsFromBQ();

    let query;
    let params = {};
    let whereClauses = [`Step_ID = 0`];

    if (userEmail && !ADMIN_EMAILS_BACKEND.includes(userEmail)) {
        whereClauses.push(`DelCode_w_o__ IN (
            SELECT DISTINCT DelCode_w_o__
            FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
            WHERE Emails LIKE @userEmail OR Emails LIKE @systemEmail
        )`);
        params.userEmail = `%${userEmail}%`;
        params.systemEmail = `%${SYSTEM_EMAIL_FOR_GLOBAL_TASKS}%`;
        console.log(`Filtering workflow headers for non-admin user: ${userEmail}`);
    } else if (userEmail && ADMIN_EMAILS_BACKEND.includes(userEmail)) {
        console.log(`Fetching all workflow headers for admin user: ${userEmail}`);
    } else {
        console.log(`Fetching all workflow headers (no user email provided or default behavior)`);
    }

    if (searchQuery) {
        whereClauses.push(`(Task_Details LIKE @searchQuery OR Delivery_code LIKE @searchQuery)`);
        params.searchQuery = `%${searchQuery}%`;
        console.log(`Applying search filter: ${searchQuery}`);
    }

    if (clientFilter) {
        whereClauses.push(`Client = @clientFilter`);
        params.clientFilter = clientFilter;
        console.log(`Applying client filter: ${clientFilter}`);
    }

    query = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
             WHERE ${whereClauses.join(' AND ')}`;

    try {
        const [rows] = await bigQueryClient.query({
            query: query,
            params: params,
            location: 'US',
        });
        res.status(200).json(rows);
    } catch (error) {
        console.error('Error fetching data from BigQuery for /api/data:', error);
        res.status(500).send({ error: 'Failed to fetch data from BigQuery.' });
    }
});

// FIXED ENDPOINT: /api/workflow-details/:deliveryCode (GET all tasks for a specific workflow) (unchanged)
app.get('/api/workflow-details/:deliveryCode', async (req, res) => {
    const { deliveryCode } = req.params;
    const query = `
        SELECT
            Key,
            Delivery_code,
            DelCode_w_o__,
            Step_ID,
            Task_Details,
            Frequency___Timeline,
            Client,
            Short_Description,
            Planned_Start_Timestamp,
            Planned_Delivery_Timestamp,
            Responsibility,
            Current_Status,
            Emails,
            Total_Tasks,
            Completed_Tasks,
            Planned_Tasks,
            Percent_Tasks_Completed,
            Created_at,
            Updated_at,
            Time_Left_For_Next_Task_dd_hh_mm_ss,
            Card_Corner_Status
        FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
        WHERE DelCode_w_o__ = @deliveryCode
    `;
    const params = { deliveryCode: deliveryCode };

    try {
        const [rows] = await bigQueryClient.query({
            query: query,
            params: params,
            location: 'US',
        });
        res.status(200).json(rows);
    } catch (error) {
        console.error(`Error fetching workflow details for ${deliveryCode} from BigQuery:`, error);
        res.status(500).send({ error: `Failed to fetch workflow details for ${deliveryCode}.` });
    }
});


// NEW ENDPOINT: /api/per-key-per-day-by-key (unchanged)
app.get('/api/per-key-per-day-by-key', async (req, res) => {
    const { key } = req.query;
    if (!key) {
        return res.status(400).send({ error: 'Key parameter is required.' });
    }

    const query = `
        SELECT Key, Day, Duration, Duration_Unit, Planned_Delivery_Slot, Responsibility
        FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\`
        WHERE Key = @key
    `;
    const params = { key: parseInt(key, 10) };
    const queryTypes = {
        key: 'INT64'
    };

    try {
        const [rows] = await bigQueryClient.query({
            query: query,
            params: params,
            types: queryTypes,
            location: 'US',
        });

        const groupedData = {
            totalDuration: 0,
            entries: []
        };
        rows.forEach(row => {
            groupedData.entries.push(row);
            groupedData.totalDuration += row.Duration || 0;
        });

        if (rows.length === 0) {
            return res.status(404).send({ message: 'No entries found for this key.' });
        }

        res.status(200).json(groupedData);
    } catch (error) {
        console.error(`Error fetching Per_Key_Per_Day data for Key ${key} from BigQuery:`, error);
        res.status(500).send({ error: `Failed to fetch Per_Key_Per_Day data for Key ${key}.` });
    }
});


// Existing /api/per-key-per-day route (kept for other potential uses) (unchanged)
app.get('/api/per-key-per-day', async (req, res) => {
    const query = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\``;
    try {
        const [rows] = await bigQueryClient.query(query);
        const groupedData = {};
        rows.forEach(row => {
            const key = row.Key;
            if (!groupedData[key]) {
                groupedData[key] = {
                    totalDuration: 0,
                    entries: []
                };
            }
            groupedData[key].entries.push(row);
            groupedData[key].totalDuration += row.Duration_In_Minutes || 0;
        });
        res.status(200).json(groupedData);
    } catch (error) {
        console.error('Error fetching per-key-per-day data from BigQuery:', error);
        res.status(500).send({ error: 'Failed to fetch per-key-per-day data from BigQuery.' });
    }
});

// Existing /api/per-person-per-day route (unchanged)
app.get('/api/per-person-per-day', async (req, res) => {
    const query = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable3}\``;
    try {
        const [rows] = await bigQueryClient.query(query);
        res.status(200).json(rows);
    }
    catch (error) {
        console.error('Error fetching per-person-per-day data from BigQuery:', error);
        res.status(500).send({ error: 'Failed to fetch per-person-per-day data from BigQuery.' });
    }
});

// NEW ENDPOINT: Update Task Status and Log to Backup Table (unchanged)
app.post('/api/task/status-update', async (req, res) => {
    console.log('Backend: Received POST request to /api/task/status-update');

    const { key, email, status } = req.body;

    if (!key || !email || !status) {
        return res.status(400).json({
            message: 'Bad Request: Key, email, and status are required in the request body.',
            details: 'Missing task key, user email, or status (Complete/Not Required).'
        });
    }

    if (status !== 'Complete' && status !== 'Not Required') {
        return res.status(400).json({
            message: 'Bad Request: Invalid status value.',
            details: 'Status must be "Complete" or "Not Required".'
        });
    }

    const targetDataset = 'PMS';
    const targetTable = bigQueryStatusUpdateTable; // "StatusUpdatesBackup"

    // 1. Update the main task table (bigQueryTable) to set the task's Current_Status
    // Also update Updated_at to reflect the change
    const updateMainTaskQuery = `
        UPDATE \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
        SET Current_Status = @status, Updated_at = CURRENT_DATETIME('Asia/Kolkata')
        WHERE Key = @key
    `;
    const updateMainTaskOptions = {
        query: updateMainTaskQuery,
        params: { key: parseInt(key, 10), status: status },
        types: { key: 'INT64', status: 'STRING' },
        location: 'US',
    };

    // 2. Log the Status Update (Insert into the Backup table)
    // NOTE: This insert assumes the project ID for the target table is 'stellar-acre-407408'
    const insertBackupQuery = `
        INSERT INTO \`stellar-acre-407408.${targetDataset}.${targetTable}\` (Timestamp, Email, Key, Status)
        VALUES (CURRENT_TIMESTAMP(), @email, @key, @status)
    `;
    const insertBackupOptions = {
        query: insertBackupQuery,
        params: { email: email, key: parseInt(key, 10), status: status },
        types: { email: 'STRING', key: 'INT64', status: 'STRING' },
        location: 'US',
    };

    try {
        // Run Main Task Update first
        console.log(`Backend: Updating main task status for Key ${key} to ${status}...`);
        const [updateMainTaskJob] = await bigQueryClient.createQueryJob(updateMainTaskOptions);
        await updateMainTaskJob.getQueryResults();

        // Run Backup Table Insert (Timestamp logging)
        console.log(`Backend: Logging status update to backup table for Key ${key}...`);
        const [insertBackupJob] = await bigQueryClient.createQueryJob(insertBackupOptions);
        await insertBackupJob.getQueryResults();

        console.log(`Backend: Key ${key} status successfully updated and logged.`);
        res.status(200).send({ message: 'Task status updated and logged successfully.' });

    } catch (error) {
        console.error('Backend: Error processing task status update:', error);
        res.status(500).json({
            message: 'Failed to update task status due to a backend error.',
            details: error.message || 'Unknown server error.',
        });
    }
});


// Modified POST route to handle both main task and Per_Key_Per_Day updates (unchanged)
app.post('/api/post', async (req, res) => {
    console.log('Backend: Received POST request to /api/post');

    const { mainTask, perKeyPerDayRows, requestingUserEmail } = req.body;
    const userEmail = requestingUserEmail;

    // Check if mainTask or its Key is missing
    if (!mainTask || mainTask.Key === undefined || mainTask.Key === null || String(mainTask.Key) === '') {
        console.error("Backend: mainTask or mainTask.Key is missing or empty in the request body.");
        return res.status(400).json({
            message: 'Bad Request: Task data or Task Key is missing in the request body.',
            details: 'The server expected a "mainTask" object with a non-empty "Key" property but it was not found or was incomplete.'
        });
    }

    const taskKeyString = String(mainTask.Key);

    // --- 2. SERVER-SIDE RESPONSIBILITY CHANGE VALIDATION (SECURITY CHECK) ---
    try {
        // --- CHECK 2: Dynamic Admin Status Check for Reassignment ---
        const ADMIN_EMAILS_BACKEND = await fetchAdminEmailsFromBQ();

        const fetchCurrentTaskQuery = `
            SELECT Responsibility
            FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
            WHERE Key = @key
        `;
        const fetchOptions = {
            query: fetchCurrentTaskQuery,
            params: { key: parseInt(taskKeyString, 10) },
            types: { key: 'INT64' },
            location: 'US',
        };

        const [currentRows] = await bigQueryClient.query(fetchOptions);
        const currentTask = currentRows[0];

        // Check if the task exists and the Responsibility field is actually changing
        if (currentTask && currentTask.Responsibility !== mainTask.Responsibility) {

            // If the user is NOT an admin, reject the change
            if (!ADMIN_EMAILS_BACKEND.includes(userEmail)) {
                console.warn(`SECURITY ALERT: Non-admin user ${userEmail} attempted to change Responsibility for Key ${taskKeyString} from "${currentTask.Responsibility}" to "${mainTask.Responsibility}".`);
                return res.status(403).json({
                    message: 'Forbidden: You do not have permission to change the Responsibility for an existing task.',
                    details: 'Only Admin users can reassign tasks.'
                });
            }
        }
    } catch (fetchError) {
        console.error("Backend: Error during security check (fetching current task state):", fetchError);
    }
    // --- END SERVER-SIDE VALIDATION ---


    // Convert timestamps to BigQuery compatible format for mainTask (unchanged)
    const formatTimestamp = (timestamp, type) => {
        if (!timestamp) return null;
        const cleanedTimestamp = typeof timestamp === 'string' ? timestamp.replace(' UTC', '') : timestamp;
        const momentObj = moment.utc(cleanedTimestamp);
        if (type === 'TIMESTAMP') {
            return momentObj.isValid() ? momentObj.format('YYYY-MM-DD HH:mm:ss.SSSSSS') + ' UTC' : null;
        } else if (type === 'DATETIME') {
            return momentObj.isValid() ? momentObj.format('YYYY-MM-DD HH:mm:ss.SSSSSS') : null;
        }
        return null;
    };

    const formattedPlannedStartTimestamp = formatTimestamp(mainTask.Planned_Start_Timestamp, 'TIMESTAMP');
    const formattedPlannedDeliveryTimestamp = formatTimestamp(mainTask.Planned_Delivery_Timestamp, 'TIMESTAMP');
    const formattedCreatedAt = formatTimestamp(mainTask.Created_at, 'TIMESTAMP');
    const formattedUpdatedAt = formatTimestamp(mainTask.Updated_at, 'DATETIME');


    // Prepare data for the main task table update (unchanged)
    const mainTaskRow = {
        Key: mainTask.Key,
        Delivery_code: mainTask.Delivery_code,
        DelCode_w_o__: mainTask.DelCode_w_o__,
        Step_ID: mainTask.Step_ID,
        Task_Details: mainTask.Task_Details,
        Frequency___Timeline: mainTask.Frequency___Timeline,
        Client: mainTask.Client,
        Short_Description: mainTask.Short_Description,
        Planned_Start_Timestamp: formattedPlannedStartTimestamp,
        Planned_Delivery_Timestamp: formattedPlannedDeliveryTimestamp,
        Responsibility: mainTask.Responsibility,
        Current_Status: mainTask.Current_Status,
        Emails: mainTask.Emails,
        Total_Tasks: mainTask.Total_Tasks,
        Completed_Tasks: mainTask.Completed_Tasks,
        Planned_Tasks: mainTask.Planned_Tasks,
        Percent_Tasks_Completed: mainTask.Percent_Tasks_Completed,
        Created_at: formattedCreatedAt,
        Updated_at: formattedUpdatedAt,
        Time_Left_For_Next_Task_dd_hh_mm_ss: mainTask.Time_Left_For_Next_Task_dd_hh_mm_ss,
        Card_Corner_Status: mainTask.Card_Corner_Status,
    };

    // Define types for nullable parameters in mainTaskRow for BigQuery UPDATE (unchanged)
    const mainTaskParameterTypes = {
        Key: 'INTEGER',
        Delivery_code: 'STRING',
        DelCode_w_o__: 'STRING',
        Step_ID: 'INTEGER',
        Task_Details: 'STRING',
        Frequency___Timeline: 'STRING',
        Client: 'STRING',
        Short_Description: 'STRING',
        Planned_Start_Timestamp: 'TIMESTAMP',
        Planned_Delivery_Timestamp: 'TIMESTAMP',
        Responsibility: 'STRING',
        Current_Status: 'STRING',
        Emails: 'STRING',
        Total_Tasks: 'INTEGER',
        Completed_Tasks: 'INTEGER',
        Planned_Tasks: 'INTEGER',
        Percent_Tasks_Completed: 'FLOAT',
        Created_at: 'TIMESTAMP',
        Updated_at: 'DATETIME',
        Time_Left_For_Next_Task_dd_hh_mm_ss: 'STRING',
        Card_Corner_Status: 'STRING',
    };

    try {
        // 1. Update the main task table (componentv2) (unchanged)
        const updateMainTaskQuery = `
            UPDATE \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
            SET
                Delivery_code = @Delivery_code,
                DelCode_w_o__ = @DelCode_w_o__,
                Step_ID = @Step_ID,
                Task_Details = @Task_Details,
                Frequency___Timeline = @Frequency___Timeline,
                Client = @Client,
                Short_Description = @Short_Description,
                Planned_Start_Timestamp = @Planned_Start_Timestamp,
                Planned_Delivery_Timestamp = @Planned_Delivery_Timestamp,
                Responsibility = @Responsibility,
                Current_Status = @Current_Status,
                Emails = @Emails,
                Total_Tasks = @Total_Tasks,
                Completed_Tasks = @Completed_Tasks,
                Planned_Tasks = @Planned_Tasks,
                Percent_Tasks_Completed = @Percent_Tasks_Completed,
                Created_at = @Created_at,
                Updated_at = @Updated_at,
                Time_Left_For_Next_Task_dd_hh_mm_ss = @Time_Left_For_Next_Task_dd_hh_mm_ss,
                Card_Corner_Status = @Card_Corner_Status
            WHERE Key = @Key
        `;
        const updateMainTaskOptions = {
            query: updateMainTaskQuery,
            params: mainTaskRow,
            types: mainTaskParameterTypes,
            location: 'US',
        };
        console.log('Backend: Executing main task update query...');
        const [mainTaskJob] = await bigQueryClient.createQueryJob(updateMainTaskOptions);
        await mainTaskJob.getQueryResults();
        console.log(`Backend: Main task with Key ${mainTask.Key} updated successfully.`);


        // 2. Safely Update/Replace Per_Key_Per_Day using MERGE (unchanged)
        if (perKeyPerDayRows && perKeyPerDayRows.length > 0) {

            const targetKey = parseInt(mainTask.Key, 10);

            // 2a. DELETE all existing rows for this key.
            const deleteMergeQuery = `
                MERGE INTO \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` AS T
                USING (
                    SELECT @targetKey AS Key
                ) AS S
                ON T.Key = S.Key
                WHEN MATCHED THEN DELETE
            `;

            const deleteMergeOptions = {
                query: deleteMergeQuery,
                params: { targetKey: targetKey },
                types: { targetKey: 'INT64' },
                location: 'US',
            };

            console.log('Backend: Deleting existing perKeyPerDayRows using MERGE...');
            const [deleteMergeJob] = await bigQueryClient.createQueryJob(deleteMergeOptions);
            await deleteMergeJob.getQueryResults(); // Wait for delete to complete
            console.log(`Backend: Existing Per_Key_Per_Day entries for Key ${targetKey} deleted using MERGE.`);


            // 2b. Insert new Per_Key_Per_Day entries
            const insertRows = perKeyPerDayRows.map(row => ({
                Key: targetKey, // Use the fixed integer key
                Day: row.Day,
                Duration: parseInt(row.Duration, 10),
                Duration_Unit: row.Duration_Unit,
                Planned_Delivery_Slot: row.Planned_Delivery_Slot || null,
                Responsibility: row.Responsibility,
            }));

            const perKeyPerDaySchema = [
                { name: 'Key', type: 'INTEGER' },
                { name: 'Day', type: 'DATE' },
                { name: 'Duration', type: 'INTEGER' },
                { name: 'Duration_Unit', type: 'STRING' },
                { name: 'Planned_Delivery_Slot', type: 'STRING', mode: 'NULLABLE' },
                { name: 'Responsibility', type: 'STRING' },
            ];

            await bigQueryClient
                .dataset(bigQueryDataset)
                .table(bigQueryTable2)
                .insert(insertRows, { schema: perKeyPerDaySchema });
            console.log(`Backend: New Per_Key_Per_Day entries for Key ${targetKey} inserted successfully.`);

        } else {
            console.log('Backend: No perKeyPerDayRows to insert.');
        }


        res.status(200).send({ message: 'Task and associated schedule data updated successfully.' });

    } catch (error) {
        console.error('Backend: Error updating task and schedule in BigQuery:', error);
        if (error.response && error.response.insertErrors) {
            console.error('Backend: BigQuery specific insert errors details:');
            error.response.insertErrors.forEach((insertError, index) => {
                console.error(`Backend: Row ${index} had errors:`);
                insertError.errors.forEach(e => console.error(`Backend: - Reason: ${e.reason}, Message: ${e.message}`));
                console.error('Backend: Raw row that failed:', JSON.stringify(insertError.row, null, 2));
            });
        } else if (error.code && error.errors) {
            console.error('Backend: Google Cloud API Error:', JSON.stringify(error.errors, null, 2));
        }

        res.status(500).json({
            message: 'Failed to update task due to a backend error.',
            details: error.message || 'Unknown server error.',
            bigQueryErrorDetails: error.response?.insertErrors ? JSON.stringify(error.response.insertErrors) : null,
        });
    }
});

// Delete Task from BigQuery (unchanged)
app.delete('/api/data/:deliveryCode', async (req, res) => {
    const { deliveryCode } = req.params;
    console.log("Backend: Delete request for deliveryCode:", deliveryCode);
    const query = `
        DELETE FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
        WHERE DelCode_w_o__ = @deliveryCode
    `;

    const options = {
        query: query,
        params: { deliveryCode },
    };

    try {
        const [job] = await bigQueryClient.createQueryJob(options);
        await job.getQueryResults();
        res.status(200).send({ message: 'All tasks with the specified delivery code were deleted successfully.' });
    } catch (error) {
        console.error('Backend: Error deleting tasks from BigQuery:', error);
        res.status(500).send({ error: 'Failed to delete tasks from BigQuery.' });
    }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);

});
