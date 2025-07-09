// server.js

const express = require('express');
const cors = require('cors');
// const { BigQuery } = require('@google-cloud/bigquery'); // REMOVE THIS IF NOT USING BIGQUERY AT ALL
const { google } = require('googleapis'); // Import googleapis
const path = require('path');
require('dotenv').config(); // If you're using .env for credentials

const app = express();
const port = process.env.PORT || 3001;

// --- CORS Configuration (remain as is) ---
const allowedOrigins = [
    'https://scheduler-ui-roan.vercel.app', // Your Vercel frontend URL
    'http://localhost:3000', // For local development of your frontend
];

app.use(cors({
    origin: function (origin, callback) {
        if (!origin) return callback(null, true);
        if (allowedOrigins.indexOf(origin) === -1) {
            const msg = 'The CORS policy for this site does not allow access from the specified Origin.';
            return callback(new Error(msg), false);
        }
        return callback(null, true);
    },
    methods: ['GET', 'POST', 'PUT', 'DELETE'],
    allowedHeaders: ['Content-Type', 'Authorization'],
    credentials: true
}));
// --- END CORS Configuration ---

app.use(express.json());


// --- Google Sheets API Authentication ---
let sheets;
async function initializeGoogleSheetsAPI() {
    try {
        const auth = new google.auth.GoogleAuth({
            keyFile: process.env.NODE_ENV === 'production'
                ? undefined // In production, use env vars directly, or pass them
                : path.join(__dirname, 'path/to/your/google-sheets-service-account-key.json'), // ADJUST THIS PATH FOR LOCAL DEV
            scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'], // Read-only scope
            credentials: process.env.NODE_ENV === 'production'
                ? {
                    client_email: process.env.GOOGLE_SHEETS_CLIENT_EMAIL,
                    private_key: process.env.GOOGLE_SHEETS_PRIVATE_KEY.replace(/\\n/g, '\n'),
                }
                : undefined, // Handled by keyFile in local dev
        });

        const authClient = await auth.getClient();
        sheets = google.sheets({ version: 'v4', auth: authClient });
        console.log('Google Sheets API client initialized successfully.');
    } catch (error) {
        console.error('Failed to initialize Google Sheets API client:', error);
        // Do NOT exit process if only Sheets API fails, if BigQuery is also used for other routes.
        // If BigQuery is NOT used, you might want to exit here.
    }
}

// Initialize the Sheets API when the server starts
initializeGoogleSheetsAPI();


// --- BIGQUERY CLIENT INITIALIZATION (KEEP THIS ONLY IF YOU USE BIGQUERY FOR OTHER ROUTES) ---
// If /api/data, /api/per-key-per-day, /api/per-person-per-day are from BigQuery, keep this block.
// Otherwise, remove it and adjust those routes to use Google Sheets too.
let bigquery;
// Ensure this condition is true for BigQuery to initialize
if (process.env.USE_BIGQUERY === 'true' || process.env.NODE_ENV === 'production') { // Added NODE_ENV for robustness
    try {
        const { BigQuery } = require('@google-cloud/bigquery');
        if (process.env.NODE_ENV === 'production') {
            bigquery = new BigQuery({
                projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
                credentials: {
                    client_email: process.env.GOOGLE_CLOUD_CLIENT_EMAIL,
                    private_key: process.env.GOOGLE_CLOUD_PRIVATE_KEY.replace(/\\n/g, '\n'),
                },
            });
        } else {
            // For local development
            const serviceAccountKeyPath = path.join(__dirname, 'path/to/your/bigquery-service-account-key.json'); // ADJUST THIS PATH
            bigquery = new BigQuery({
                keyFilename: serviceAccountKeyPath,
                projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
            });
        }
        console.log('BigQuery client initialized successfully.');
    } catch (error) {
        console.error('Failed to initialize BigQuery client:', error);
        // Do NOT process.exit(1) if you have other independent services like Google Sheets API
        // But log the error clearly.
    }
} else {
    console.log('BigQuery client initialization skipped (USE_BIGQUERY not true and not in production).');
}


// --- API Endpoints ---

// Endpoint for /api/data (ASSUMING THIS STILL COMES FROM BIGQUERY IF USED)
app.get('/api/data', async (req, res) => {
    if (!bigquery) {
        return res.status(500).json({ error: 'BigQuery client not initialized.' });
    }
    // Your existing logic for fetching general data (Task details etc.)
    // ... (Your existing BigQuery query for /api/data)
    const email = req.query.email;
    const offset = parseInt(req.query.offset) || 0;
    const limit = parseInt(req.query.limit) || 500;
    const isAdmin = req.query.isAdmin === 'true';

    try {
        const query = `
            SELECT
                Key, Delivery_code, DelCode_w_o__, Step_ID, Task_Details, Frequency___Timeline,
                Client, Short_Description, Planned_Start_Timestamp, Planned_Delivery_Timestamp,
                Responsibility, Current_Status, Email, Emails, Total_Tasks, Completed_Tasks,
                Planned_Tasks, Percent_Tasks_Completed, Created_at, Updated_at,
                Time_Left_For_Next_Task_dd_hh_mm_ss, Card_Corner_Status
            FROM
                \`your-gcp-project.your_dataset.your_table_delivery_details\`
            ORDER BY
                Created_at DESC
            LIMIT ${limit} OFFSET ${offset}
        `;
        const [rows] = await bigquery.query(query);
        res.json(rows);
    } catch (error) {
        console.error("Error fetching data from BigQuery (Delivery Details):", error);
        res.status(500).json({ error: 'Failed to fetch delivery data.' });
    }
});


// NEW ENDPOINT: /api/people-to-email-mapping (USING GOOGLE SHEETS)
app.get('/api/people-to-email-mapping', async (req, res) => {
    if (!sheets) {
        return res.status(500).json({ error: 'Google Sheets API client not initialized.' });
    }

    const SPREADSHEET_ID = process.env.GOOGLE_SHEETS_MAPPING_ID; // Your Google Sheet ID
    const RANGE = 'Sheet1!A:C'; // <<< IMPORTANT: Adjust this to your sheet name and columns

    // Assuming your sheet has columns: Name, Email, Emails (in that order)
    // Example: A1=Name, B1=Email, C1=Emails
    // If your columns are different, adjust the `map` function below.

    try {
        const response = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: RANGE,
        });

        const rows = response.data.values;
        if (!rows || rows.length === 0) {
            return res.json([]); // Return empty array if no data
        }

        // Assuming the first row is headers, skip it
        const headers = rows[0];
        const dataRows = rows.slice(1);

        // Find the column indices dynamically
        const nameIndex = headers.indexOf('Name');
        const emailIndex = headers.indexOf('Email');
        const allEmailsIndex = headers.indexOf('Emails'); // Or whatever your column for multiple emails is called

        if (nameIndex === -1 || emailIndex === -1) {
            console.error("Required columns 'Name' or 'Email' not found in Google Sheet headers.");
            return res.status(500).json({ error: "Required columns 'Name' or 'Email' not found in Google Sheet." });
        }


        const peopleData = dataRows.map(row => ({
            Name: row[nameIndex] || '',
            Email: row[emailIndex] || '', // Primary email
            Emails: row[allEmailsIndex] || row[emailIndex] || '' // All emails, fallback to primary if 'Emails' column is missing
        }));

        res.json(peopleData);

    } catch (error) {
        console.error("Error fetching people mapping from Google Sheet:", error);
        res.status(500).json({ error: `Failed to fetch people mapping data: ${error.message}` });
    }
});

// --- Other API Endpoints (e.g., for Per-Key-Per-Day, Per-Person-Per-Day) ---
// If these are also from Google Sheets, you'll need to replicate the Sheets API logic.
// If they are from BigQuery, ensure the BigQuery initialization block is kept and used.

// Example placeholder for BigQuery-based route:
app.get('/api/per-key-per-day', async (req, res) => {
    if (!bigquery) {
        return res.status(500).json({ error: 'BigQuery client not initialized for this route.' });
    }
    // ... (your existing BigQuery query for /api/per-key-per-day)
    try {
        const query = `
            SELECT
                Key, Day, Duration
            FROM
                \`your-gcp-project.your_dataset.your_table_per_key_per_day\`
            ORDER BY Key, Day
        `;
        const [rows] = await bigquery.query(query);

        const transformedData = rows.reduce((acc, row) => {
            if (!acc[row.Key]) {
                acc[row.Key] = { entries: [] };
            }
            acc[row.Key].entries.push({
                Day: row.Day,
                Duration: row.Duration
            });
            return acc;
        }, {});

        res.json(transformedData);
    } catch (error) {
        console.error("Error fetching per-key-per-day data from BigQuery:", error);
        res.status(500).json({ error: 'Failed to fetch per-key-per-day data.' });
    }
});

// ... and similarly for /api/per-person-per-day if it's from BigQuery

// Endpoint for POST requests (updating/creating tasks)
app.post('/api/post', async (req, res) => {
    if (!bigquery) {
        return res.status(500).json({ error: 'BigQuery client not initialized for POST operations.' });
    }
    // ... (Your existing POST logic that uses BigQuery)
    const {
        Key, Delivery_code, DelCode_w_o__, Step_ID, Task_Details, Frequency___Timeline,
        Client, Short_Description, Planned_Start_Timestamp, Planned_Delivery_Timestamp,
        Responsibility, Current_Status, Email, Emails, Total_Tasks, Completed_Tasks,
        Planned_Tasks, Percent_Tasks_Completed, Created_at, Updated_at,
        Time_Left_For_Next_Task_dd_hh_mm_ss, Card_Corner_Status, sliders
    } = req.body;

    const deliveryDetailsRow = {
        Key, Delivery_code, DelCode_w_o__, Step_ID, Task_Details, Frequency___Timeline,
        Client, Short_Description, Planned_Start_Timestamp, Planned_Delivery_Timestamp,
        Responsibility, Current_Status, Email, Emails, Total_Tasks, Completed_Tasks,
        Planned_Tasks, Percent_Tasks_Completed, Created_at, Updated_at,
        Time_Left_For_Next_Task_dd_hh_mm_ss, Card_Corner_Status
    };

    const perKeyPerDayRows = sliders.map(slider => ({
        Key: Key,
        Day: slider.day,
        Duration: slider.duration
    }));

    const perPersonPerDayRows = sliders.map(slider => ({
        Responsibility: slider.personResponsible,
        Day: slider.day,
        Duration_In_Minutes: slider.duration
    }));

    const datasetId = 'your_dataset';
    const deliveryTableId = 'your_table_delivery_details';
    const perKeyTableId = 'your_table_per_key_per_day';
    const perPersonTableId = 'your_table_per_person_per_day';

    try {
        const deletePerKeyQuery = `
            DELETE FROM \`${bigquery.projectId}.${datasetId}.${perKeyTableId}\`
            WHERE Key = @key_to_delete
        `;
        const optionsDeletePerKey = {
            query: deletePerKeyQuery,
            params: { key_to_delete: Key },
        };
        await bigquery.query(optionsDeletePerKey);
        console.log(`Deleted existing per-key-per-day entries for Key: ${Key}`);

        if (perKeyPerDayRows.length > 0) {
            await bigquery.dataset(datasetId).table(perKeyTableId).insert(perKeyPerDayRows);
            console.log('Per-key-per-day entries inserted successfully.');
        }

        const upsertDeliveryQuery = `
            MERGE \`${bigquery.projectId}.${datasetId}.${deliveryTableId}\` T
            USING (SELECT
                '${Key}' as Key,
                '${Delivery_code}' as Delivery_code,
                '${DelCode_w_o__}' as DelCode_w_o__,
                '${Step_ID}' as Step_ID,
                '${Task_Details}' as Task_Details,
                '${Frequency___Timeline}' as Frequency___Timeline,
                '${Client}' as Client,
                '${Short_Description}' as Short_Description,
                '${Planned_Start_Timestamp}' as Planned_Start_Timestamp,
                '${Planned_Delivery_Timestamp}' as Planned_Delivery_Timestamp,
                '${Responsibility}' as Responsibility,
                '${Current_Status}' as Current_Status,
                '${Email}' as Email,
                '${Emails}' as Emails,
                ${Total_Tasks || 'NULL'} as Total_Tasks,
                ${Completed_Tasks || 'NULL'} as Completed_Tasks,
                ${Planned_Tasks || 'NULL'} as Planned_Tasks,
                ${Percent_Tasks_Completed || 'NULL'} as Percent_Tasks_Completed,
                '${Created_at}' as Created_at,
                '${Updated_at}' as Updated_at,
                '${Time_Left_For_Next_Task_dd_hh_mm_ss}' as Time_Left_For_Next_Task_dd_hh_mm_ss,
                '${Card_Corner_Status}' as Card_Corner_Status
            ) S
            ON T.Key = S.Key
            WHEN MATCHED THEN
                UPDATE SET
                    Delivery_code = S.Delivery_code,
                    DelCode_w_o__ = S.DelCode_w_o__,
                    Step_ID = S.Step_ID,
                    Task_Details = S.Task_Details,
                    Frequency___Timeline = S.Frequency___Timeline,
                    Client = S.Client,
                    Short_Description = S.Short_Description,
                    Planned_Start_Timestamp = S.Planned_Start_Timestamp,
                    Planned_Delivery_Timestamp = S.Planned_Delivery_Timestamp,
                    Responsibility = S.Responsibility,
                    Current_Status = S.Current_Status,
                    Email = S.Email,
                    Emails = S.Emails,
                    Total_Tasks = S.Total_Tasks,
                    Completed_Tasks = S.Completed_Tasks,
                    Planned_Tasks = S.Planned_Tasks,
                    Percent_Tasks_Completed = S.Percent_Tasks_Completed,
                    Created_at = S.Created_at,
                    Updated_at = S.Updated_at,
                    Time_Left_For_Next_Task_dd_hh_mm_ss = S.Time_Left_For_Next_Task_dd_hh_mm_ss,
                    Card_Corner_Status = S.Card_Corner_Status
            WHEN NOT MATCHED THEN
                INSERT (Key, Delivery_code, DelCode_w_o__, Step_ID, Task_Details, Frequency___Timeline,
                        Client, Short_Description, Planned_Start_Timestamp, Planned_Delivery_Timestamp,
                        Responsibility, Current_Status, Email, Emails, Total_Tasks, Completed_Tasks,
                        Planned_Tasks, Percent_Tasks_Completed, Created_at, Updated_at,
                        Time_Left_For_Next_Task_dd_hh_mm_ss, Card_Corner_Status)
                VALUES (S.Key, S.Delivery_code, S.DelCode_w_o__, S.Step_ID, S.Task_Details, S.Frequency___Timeline,
                        S.Client, S.Short_Description, S.Planned_Start_Timestamp, S.Planned_Delivery_Timestamp,
                        S.Responsibility, S.Current_Status, S.Email, S.Emails, S.Total_Tasks, S.Completed_Tasks,
                        S.Planned_Tasks, S.Percent_Tasks_Completed, S.Created_at, S.Updated_at,
                        S.Time_Left_For_Next_Task_dd_hh_mm_ss, S.Card_Corner_Status);
        `;

        await bigquery.query(upsertDeliveryQuery);
        console.log('Delivery details upserted successfully.');

        for (const row of perPersonPerDayRows) {
            const mergePerPersonQuery = `
                MERGE \`${bigquery.projectId}.${datasetId}.${perPersonTableId}\` T
                USING (SELECT
                    '${row.Responsibility}' as Responsibility,
                    '${row.Day}' as Day,
                    ${row.Duration_In_Minutes} as Duration_In_Minutes
                ) S
                ON T.Responsibility = S.Responsibility AND T.Day = S.Day
                WHEN MATCHED THEN
                    UPDATE SET Duration_In_Minutes = S.Duration_In_Minutes
                WHEN NOT MATCHED THEN
                    INSERT (Responsibility, Day, Duration_In_Minutes)
                    VALUES (S.Responsibility, S.Day, S.Duration_In_Minutes);
            `;
            await bigquery.query(mergePerPersonQuery);
        }
        console.log('Per-person-per-day entries upserted successfully.');

        res.status(200).json({ message: 'Task and schedules updated successfully' });

    } catch (error) {
        console.error("Error during BigQuery operation (POST):", error);
        if (error.errors && error.errors[0] && error.errors[0].reason) {
            console.error("BigQuery specific error reason:", error.errors[0].reason);
        }
        res.status(500).json({ error: `Failed to update task: ${error.message}` });
    }
});


// Start the server
app.listen(port, () => {
    console.log(`Server listening at http://localhost:${port}`);
});
